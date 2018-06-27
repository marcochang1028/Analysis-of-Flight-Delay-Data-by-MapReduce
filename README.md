# Analysis-of-Flight-Delay-Data-by-MapReduce

## Objective
It is an implementation of MapReduce to analysis the flight delay data with "in-mapper combining with flush-when-full" design pattern. There are two tasks in this project: 
    1. Calculate the average delay (in minutes) of the departures of all scheduled flights and the average delay of the arrivals of all scheduled flights. The source file is the "Delay.java" in the Program folder.
    2. Output all combinations of airline name and year such that the percentage P of scheduled flights whose departures were at least 31 minutes late (among all scheduled flights of that airline in that year) is at least 50%. The source file is the "Late.java" in the Program folder.

## Data
The dataset is the monthly "Full Analysis with Arrival-Departure Split" files (in CSV format) from January 2011 to August 2017. It can be downloaded from [here](Data/FlightDelayData.zip) (in the Data folder) or you can download it from the [UK flight punctuality data](http://www.caa.co.uk/Data-and-analysis/UK-aviation-market/Flight-reliability/Datasets/UK-flight-punctuality-data/).

## Program
As mentioned above, the program files are "Delay.java" and "Late.java" in the Program folder. As the code is well documented with comments, I do not explain anything here. If you want to ask me anything, welcome to contact me via [marcochang1028@gmail.com](mailto:marcochang1028@gmail.com)

## Running Test
1. Download the dataset from the Data folder and unzip it.
2. Download the JAR file from the Jar folder and execute the following scripts:
```
hadoop jar [jar folder path]/UKFlightAnalysis.jar org.marco.Delay [data folder] [output folder]
hadoop jar [jar folder path]/UKFlightAnalysis.jar org.marco.Late [data folder] [output folder]
```
An example of the [jar folder path] could be `~/CC/Jar`. An example of the [data folder] could be `~/CC/input/`. An example of the [output folder] could be `~/CC/outputDelay/`.

## Reference
The original implementation of the design pattern of "in-mapper combining with flush-when-full": [https://vangjee.wordpress.com/2012/03/07/the-in-mapper-combining-design-pattern-for-mapreduce-programming/](https://vangjee.wordpress.com/2012/03/07/the-in-mapper-combining-design-pattern-for-mapreduce-programming/)
