CSE 512 - Assignment 4 - HADOOP map-reduce program to perform equijoin
By: Pranav Bajoria
ASU ID: 1215321107

Following is the approach used in solving the quijoin task broken into 3 segments: Driver, Mapper and Reducer

Mapper:
Mapper takes input single line at a time as input: Input class types: (Key: Object, Value: Text).
Then, using a ',' delimiter, the join column is extracted and set as a DoubleWritable Key and the input text is mapped to this key.
Mapper outputs join column entity as key and entire input text as value. Output class types: (Key: DoubleWritable, value: Text).


Reducer:
Reducer gets input after the shuffling and grouping of mapper's outputs. Therefore each join column key might have multiple Text values wherever that key existed in both the tables. 
Input class types: (Key: DoubleWritable, Value: Iterable Text).
The main task of the Reducer then was to first group the values based on the table it belonged to. I used a hashmap for this, where the key was the table name and value was set to a set to handle duplicates if any.
Then if there are more than 1 tables found for the join column key, we proceed to join each entity from 1st table to 2nd table and keep writing it to output.
Since output needs only the Text, Reducer Output class types: (Key: NullWritable, value: Text).

Driver:
This is the main function in the equijoin class where the jobs can be configured as we want. 
1. For setting the functionality of the job, I have set the equijoin class as the main Jar class, Mapper class to my custom mapper class called "JoinColumnMapper" and Reducer class to my custom Reducer class called "EquiJoinMapper". 
2. I also set the intermediatory mapper output classtypes and the final reducer output classtypes in the driver function.
3. The driver function is used to set the input and output paths of the job by taking the command line arguments and parsing it.
4. Finally the main function waits for job completrion and exits.
