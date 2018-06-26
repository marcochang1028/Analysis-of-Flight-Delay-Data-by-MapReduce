package org.marco;

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
 * 1. For the mapper output, the airportName is used as the key, and the format of the value is arrCount|arrSum|depCount|depSum.
 * 
 * 2. For the combiner, the in-mapper combining design pattern is used and some featuers are as follows
 *   a. The in-mapper combining design pattern is used to make sure that the combiner process will be executed every time.
 *      and the in-mapper combiner can provide better performance for the data with a big size.
 *   b. For the hash object, which will temporary store the mapper items, The lazy loading design pattern is used to make sue 
 *      the object will be created only when it is really needed.
 *   c. The flush mechanism is used to make sure that the hash will not run out the memory (In this case, the hash size sets to 1000).
 *   d. The cleanup method is overridden to make sure that all the map items are emited.
 * 
 * 3. The Original "average delay time" in a row is come from "delay time" divided by "flight number". 
 * 	  That is to say, the original "delay time" must be an integer. 
 *    As a result, the "delay time" is declared as integer and the result of ("flight number" * "average delay time") is rounded up in the mapper.
 */

public class Delay {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{

    private static final int FLUSH_SIZE = 1000; //The maximum size to execute flush 
    private Map<String, String> map; //Lazy loading. Only declare it in the beginning, and initial it when we really need it or get existing object by getMap method.
    
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	//Split the data by comma
    	String[] s = splitbycomma(value.toString());
    	
    	//First condition checks that there is no empty row, because the last row in some files are empty. 
    	//Second and third conditions filter out the value of scheduled_charter field is 'C' and the value of number_flights_matched field is 0. 
    	//These two conditions can filter out the hearder line also
    	if (s.length != 0 && s[7].trim().equals("S") && !s[8].trim().equals("0")) {	
    		//Get values from the string array.
    		String airportName = s[2].trim();
    		String arrDepFlag = s[6].trim();
    		int flightAmount = Integer.parseInt(s[8].trim());
    		double averageDelayTime = Double.parseDouble(s[16].trim());
          
    		//Execute in-mapper combining design pattern.
    		//Get a map hash, use the airportName as the key, and the format of the value is arrCount|arrSum|depCount|depSum  
    		//If the key is exist in the hash, then cumulate the data.
    		//If the key is not exist, then insert a new item into the hash.
    		Map<String, String> map = getMap();
    		
    		if (map.containsKey(airportName )) {
    			//If the key is exist in the hash, then cumulate the data
    			String sValue = (String) map.get(airportName);
    			String[] sValueArray = sValue.toString().split("\\|");
    			
    			int arrCount = Integer.parseInt(sValueArray[0]);
    			int arrSum = Integer.parseInt(sValueArray[1]);
    			int depCount = Integer.parseInt(sValueArray[2]);
    			int depSum = Integer.parseInt(sValueArray[3]);
    			
    			
    			if (arrDepFlag.equals("A")) {
    				//if the flag is "Arrival", then cumulate the number of the arrival flight count and sum.
    				arrCount += flightAmount;
    				arrSum += (int) Math.round(flightAmount*averageDelayTime);
    			}
    			else {
    				//if the flag is "Depature", then cumulate the number of the depature flight count and sum.
    				depCount += flightAmount;
    				depSum += (int) Math.round(flightAmount*averageDelayTime);
    			}
    			map.put(airportName, combineTheMapValue(arrCount, arrSum, depCount, depSum));//put into hash
    		}
    		else {
    			//if the key is not exist, then insert a new item into the hash.
    			if (arrDepFlag.equals("A")) {
    				//if the flag is "Arrival", then set inital values for the arrival fligh count and sum, set zeros for the depature flight count and sum
    				map.put(airportName, combineTheMapValue(flightAmount, (int) Math.round(flightAmount*averageDelayTime), 0, 0));
    			}
    			else {
    				////if the flag is "Depature", then set inital values for the depature fligh count and sum, set zeros for the arrival flight count and sum
    				map.put(airportName, combineTheMapValue(0, 0, flightAmount, (int) Math.round(flightAmount*averageDelayTime)));
    			}	
    		}
    	}
    	
    	flush(context, false); //Not force to flush the hash, unless the hash size reaches the max flush size (we set it as 1000)
    }
    
    protected void cleanup(Context context)
  		  throws IOException, InterruptedException {
  	  flush(context, true); //force flush no matter what at the end.
    }
    
    //the format of the return value is arrCount|arrSum|depCount|depSum 
    private static String combineTheMapValue(int arrCount, int arrSum, int depCount, int depSum) {
  	  return Integer.toString(arrCount) + "|" + Integer.toString(arrSum) + "|" + Integer.toString(depCount) + "|" + Integer.toString(depSum);
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
  	  //Lazy loading. Only declare it in the beginning, and initial it when we really need it or return the existing object
  	  if (null == map){  
  		  map = new HashMap<String, String>();
  	  }
  	  return map;
    }
    
    //Splits a string by comma and handles fields enclosed in quotation marks correctly.
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
   
  
  public static class CalculateAverageReducer
  	extends Reducer<Text,Text,Text,Text> {
	  private Text result = new Text();

	  public void reduce(Text key, Iterable<Text> values, Context context) 
			  throws IOException, InterruptedException {

		  int arrCount = 0;
		  int arrSum = 0;
		  int depCount = 0;
		  int depSum = 0;
		  
		  //Key is the airportName and the format of the value is arrCount|arrSum|depCount|depSum  
		  //Sum all the data
	      for (Text val : values) {
	    	  String[] s = val.toString().split("\\|");
	    	  
	    	  arrCount += Integer.parseInt(s[0]);
	    	  arrSum += Integer.parseInt(s[1]);
	    	  depCount += Integer.parseInt(s[2]);
	    	  depSum += Integer.parseInt(s[3]);
	      }
      
	      //set result and emit it
	      result.set(Double.toString(arrSum/ (double) arrCount) + "," + Double.toString(depSum/ (double) depCount));
	      context.write(key, result);
	  }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Flight Delay Calculation");
    job.setJarByClass(Delay.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(CalculateAverageReducer.class);
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
rm -rf ~/CC/output
javac org/marco/Delay.java
javac org/marco/Late.java
jar cvf ~/CC/Jar/UKFlightAnalysis.jar org
hadoop jar ~/CC/Jar/UKFlightAnalysis.jar org.marco.Delay ~/CC/input/ ~/CC/output/
*/
