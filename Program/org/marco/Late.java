package org.leicester;

import java.io.IOException;
import java.util.*;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * In this task, there are some points which are noteworthy as follows
 * 1. For the mapper output:
 *   a. The "airline,year" is used as the key which is exactly the same as the output of reducer, 
 *      As a result, the reducer don't need to waste another cost to deal with the key again.
 *   b. Format of the value is depCount|depSum.
 *   
 * 2. For the combiner, The in-mapper combining design pattern is used and some featuers are as follows
 *   a. The in-mapper combining design pattern is used to make sure that the combiner process will be executed every time.
 *      and the in-mapper combiner can provide better performance for the data with a big size.
 *   b. For the hash object, which will temporary store the mapper items, The lazy loading design pattern is used to make sue 
 *      the object will be created only when it is really needed.
 *   c. The flush mechanism is used to make sure that the hash will not run out the memory (In this case, the hash size sets to 1000).
 *   d. The cleanup method is overridden to make sure that all the map items are emited.
 *   
 * 3. The original "flight late percentage" in a row is come from "delayed flight amount" divided by "flight number", 
 *    That is to say, the original "delayed flight amount" must be an integer. 
 *    As a result, the "delayed flight amount" is declared as integer and the result of ("flight number" * "flight late percentage (sum all percentages over 30 minutes late)") is rounded up in the mapper.
 */

public class Late {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{

    private static final int FLUSH_SIZE = 1000; //The maximum size to execute flush 
    private Map<String, String> map; //Lazy loading. Only declare it in the beginning, and initial it when we really need it or get existing object by getMap method.
    private int counter = 0;
    
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	//Split the data by comma
    	String[] s = splitbycomma(value.toString());
    	
    	
    	//First condition checks that there is no empty row, because the last row in some files are empty. 
    	//Second and third conditions filter out the value of scheduled_charter field is 'C' and the value of number_flights_matched field is 0.
    	//These two conditions can filter out the hearder line also
    	//Fourth condition checks "arrival_departure" is "D"
    	if (s.length != 0 && s[7].trim().equals("S") && !s[8].trim().equals("0") && s[6].trim().equals("D")) {	
    		//Get values from the string array.
    		String recordKey = s[5].trim() + "," + s[1].trim().substring(0, 4); //key format is airline,year (e.g. BRUSSELS AIRLINES,2011)
    		int flightAmount = Integer.parseInt(s[8].trim());
    		int delayFlightAmount = (int) Math.round(flightAmount * (Double.parseDouble(s[12].trim()) + Double.parseDouble(s[13].trim()) + Double.parseDouble(s[14].trim()) + Double.parseDouble(s[15].trim()))/100);
    		
    		//Execute in-mapper combining design pattern.
    		//Get a map hash, use the airportName as the key, and the format of the value is depCount|depSum  
    		//If the key is exist in the hash, then cumulate the data.
    		//If the key is not exist, then insert a new item into the hash.
    		Map<String, String> map = getMap();
    		
    		if (map.containsKey(recordKey)) {
    			//The key is exist, cumulate the data
    			String sValue = (String) map.get(recordKey);
    			String[] sValueArray = sValue.toString().split("\\|");
    			
    			int flightAmountSum = Integer.parseInt(sValueArray[0]);
    			int delayFlightAmountSum = Integer.parseInt(sValueArray[1]);
    			
    			flightAmountSum += flightAmount;
    			delayFlightAmountSum += delayFlightAmount;
    			
    			map.put(recordKey, combineTheMapValue(flightAmountSum, delayFlightAmountSum));
    		}
    		else {
    			//The key is not exist, then insert a new item into the hash.
    			map.put(recordKey, combineTheMapValue(flightAmount, delayFlightAmount));	
    		}
    	}
    	
    	flush(context, false); //Not force to flush the hash, unless the hash size reaches the max flush size (we set it as 1000)
    }
    
    protected void cleanup(Context context)
  		  throws IOException, InterruptedException {
  	  flush(context, true); //force flush no matter what at the end.
    }
    
    //the format of the return value is depCount|depSum 
    private static String combineTheMapValue(int flghtAmountSum, int delayFlightAmountSum) {
  	  return Integer.toString(flghtAmountSum) + "|" + Integer.toString(delayFlightAmountSum);
    }
    
    private void flush(Context context, boolean force)
  		  throws IOException, InterruptedException {
  	  Map<String, String> map = getMap();
  	  
  	//if map size is not greater than the max flush size and execution flag is not "True", then do nothing in this method
  	  if (!force){
  		  if (map.size() < FLUSH_SIZE)
  			  return;
  	  }
  	  
  	  //Run a while loop to emit all the items in the hash.
  	  Iterator<Map.Entry<String, String>> it = map.entrySet().iterator();
  	  while (it.hasNext()) {
  		  Map.Entry<String, String> entry = it.next();
  		  context.write(new Text(entry.getKey()), new Text(entry.getValue()));
  	  }
  	  
  	  map.clear(); //make sure to empty map
    }
    
    public Map<String, String> getMap(){
  	  //lazy loading.  Only declare it in the beginning, and initial it when we really need it or return the existing object
  	  if (null == map){  
  		  map = new HashMap<String, String>();
  	  }
  	  return map;
    }
    
    //Split a string by comma and handles fields enclosed in quotation marks correctly.
    //Return empty array if the input string is empty.
    public static String[] splitbycomma(String S) {
  	  ArrayList<String> L = new ArrayList<String>();
        String[] a = new String[0];
        int i = 0;
        while (i<S.length()) {
      	  int start = i;
            int end=-1;
            if (S.charAt(i)=='"') {
          	  end = S.indexOf('"',i+1);
            }
            else {
                    end = S.indexOf(',',i)-1;
                    if (end<0) end = S.length()-1;
            }
            L.add(S.substring(start,end+1));
            i = end+2;
        }
        return L.toArray(a);
    }
  }
   
  
  public static class LateCalculationReducer
  	extends Reducer<Text,Text,Text,Text> {
	  private Text result = new Text();

	  public void reduce(Text key, Iterable<Text> values, Context context) 
			  throws IOException, InterruptedException {

		  int flightAmountSum = 0;
		  int delayFlightAmountSum = 0;
		  
		  //Key is the "airline,year" and the format of the value is "depCount|depSum"  
		  //Sum all the data
	      for (Text val : values) {
	    	  String[] s = val.toString().split("\\|");
	    	  flightAmountSum += Integer.parseInt(s[0]);
	    	  delayFlightAmountSum += Integer.parseInt(s[1]);
	      }
	      
	      //Make sure there is no zero fligh amount to prevent an "divided by 0" exception occurred.
	      if (flightAmountSum > 0){
	    	  double delayPercentage = delayFlightAmountSum / (double) flightAmountSum;
	    	  //filter out the percentage of the data smaller than 0.5
	    	  if (delayPercentage >= 0.5){
		    	  result.set(Double.toString(delayPercentage * 100));
		    	  context.write(key, result);
		      }
	      }
	      
	  }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Late Calculation");
    job.setJarByClass(Late.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(LateCalculationReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }  
}

/*Terminate Script
rm -rf ~/org/marco/*.class
rm -rf ~/CC/Jar/UKFlightAnalysis.jar
rm -rf ~/CC/outputDelay
javac org/marco/Delay.java
javac org/marco/Late.java
jar cvf ~/CC/Jar/UKFlightAnalysis.jar org
hadoop jar ~/CC/Jar/UKFlightAnalysis.jar org.marco.Late ~/CC/input/ ~/CC/outputDelay/
*/
