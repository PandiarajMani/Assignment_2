 
########################################Assignment 2###############################

#########################################Task 1####################################

Execute wordmedian program using hadoop-mapreduce-examples-2.6.5.jar file 
*************************************************************************

*//To run the wordmedian pre-defined program using the jar file mentioned and output is stored in the file /wordmedian_out is displayed below//*

[acadgild@localhost ~]$ hadoop jar /home/acadgild/install/hadoop/hadoop-2.6.5/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.6.5.jar wordmedian /tele.txt /wordmedian_out

[acadgild@localhost ~]$ hadoop fs -cat /wordmedian_out/*
18/08/14 00:28:22 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
17	1
20	9
21	1
24	3
31	1
34	1
36	5
37	1
40	3
[acadgild@localhost ~]$



Execute wordmean program using hadoop-mapreduce-examples-2.6.5.jar file 
*************************************************************************

*//To run the wordmean pre-defined program using the jar file mentioned and output is stored in the file /wordmean_out is displayed below//*

[acadgild@localhost ~]$ hadoop jar /home/acadgild/install/hadoop/hadoop-2.6.5/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.6.5.jar wordmean /tele.txt /wordmean_out

[acadgild@localhost ~]$ hadoop fs -cat /wordmean_out/*
18/08/14 00:33:56 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
count	25
length	692
You have new mail in /var/spool/mail/acadgild
[acadgild@localhost ~]$


Execute wordstandarddeviation program using hadoop-mapreduce-examples-2.6.5.jar file 
************************************************************************************

*//To run the wordstandarddeviation pre-defined program using the jar file mentioned and output is stored in the file /wordstandard_out is displayed below//*

[acadgild@localhost ~]$ hadoop jar /home/acadgild/install/hadoop/hadoop-2.6.5/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.6.5.jar wordstandarddeviation /tele.txt /wordstandard_out

[acadgild@localhost ~]$ hadoop fs -cat /wordstandard_out/*
18/08/14 00:37:18 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
count	25
length	692
square	20824
[acadgild@localhost ~]$



###########################################TASK 2########################################################

Write a Map Reduce program to filter out the invalid records.Map only job will fit for this context.
****************************************************************************************************

*//Driver code and Mapper code is programmed and displayed with output below for your reference//*

package com;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

*//Mapper code to get the input and split the input file and take the required values from the input and convert into output file in key, value pair//*

public class InvalidRecords {
	public static class Map extends Mapper<LongWritable, Text, Text,NullWritable>{
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			String[] FileWords = value.toString().split("\\|");
			
			String CompanyName = FileWords[0];
			String ProductName = FileWords[1];
			
			if(CompanyName!="NA" && ProductName!="NA") {
				context.write(value, NullWritable.get());
			
			}
		}
	}

*//DriverCode and required configuration features to run the program//*

       public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration con = new Configuration();
		Job job = new Job(con, "InvalidRecords");
		job.setJarByClass(InvalidRecords.class);
		job.setMapperClass(Map.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,  new Path(args[1]));
		System.exit(job.waitForCompletion(true)?0:1);
		
		
		
	}
}

*//The entire class is exported to jar file and ran the command below with the input file and outfile as /tele.txt /tele_out//*

[acadgild@localhost ~]$ hadoop jar Records.jar /tele.txt /tele_out

[acadgild@localhost ~]$ hadoop fs -cat /tele_out/*
18/08/13 21:55:21 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Samsung|Optima|14|Madhya Pradesh|132401|14200
Onida|Lucid|18|Uttar Pradesh|232401|16200
Akai|Decent|16|Kerala|922401|12200
Lava|Attention|20|Assam|454601|24200
Zen|Super|14|Maharashtra|619082|9200
Samsung|Optima|14|Madhya Pradesh|132401|14200
Onida|Lucid|18|Uttar Pradesh|232401|16200
Onida|Decent|14|Uttar Pradesh|232401|16200
Onida|NA|16|Kerala|922401|12200
Lava|Attention|20|Assam|454601|24200
Zen|Super|14|Maharashtra|619082|9200
Samsung|Optima|14|Madhya Pradesh|132401|14200
NA|Lucid|18|Uttar Pradesh|232401|16200
Samsung|Decent|16|Kerala|922401|12200
Lava|Attention|20|Assam|454601|24200
Samsung|Super|14|Maharashtra|619082|9200
Samsung|Super|14|Maharashtra|619082|9200
Samsung|Super|14|Maharashtra|619082|9200
[acadgild@localhost ~]$ 


!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
Write a MapReduce program to calculate the total units sold for each company.
*****************************************************************************

package com;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class OnidaStateWise {
	public static class Map extends Mapper<LongWritable, Text,Text,IntWritable>{
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] fileWords = value.toString().split("\\|");
			String CompanyName = fileWords[0];
			String StateName = fileWords[3];
			
			if(CompanyName=="Onida") {
				context.write(new Text(StateName), new IntWritable(1));
			}
		}
	}
   public static class Reduce extends Reducer<Text,IntWritable,Text, IntWritable>{
	   @Override
	protected void reduce(Text key, Iterable<IntWritable> value,
			Context con) throws IOException, InterruptedException {
		   int i = 0;
		   
		   for(IntWritable val : value) {
			   i=i+val.get();
			  }
		con.write(key,new IntWritable(i));
	}
   }
  public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
	Configuration con =  new Configuration();
	Job job = new Job(con, "OnidaStateWise");
	job.setJarByClass(OnidaStateWise.class);
	job.setMapperClass(Map.class);
	job.setReducerClass(Reduce.class);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(IntWritable.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job,new Path(args[1]));
	System.exit(job.waitForCompletion(true)?0:1);

[acadgild@localhost ~]$ hadoop jar sold.jar /tele.txt /tele_sold

[acadgild@localhost ~]$ hadoop fs -cat /tele_sold/*
18/08/13 22:05:24 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Onida	4
[acadgild@localhost ~]$ 

!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!1!!!!!!!!!!! !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
**Write a MapReduce program to calculate the total unit sold in each state for onida**
***************************************************************************************


package com;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UnitSold {
	public static class Map extends Mapper<LongWritable,Text,Text,IntWritable>{
	  @Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		String CompanyName = value.toString().split("\\|")[0];
		context.write(new Text(CompanyName), new IntWritable(1));
		
	}	
	}
public static class Reduce extends Reducer<Text,IntWritable,Text,IntWritable>{
	@Override
	protected void reduce(Text key, Iterable<IntWritable> value,
			Context context) throws IOException, InterruptedException {
		int i = 0;
		for(IntWritable val: value) {
			i = i+val.get();
		}
		context.write(key, new IntWritable(i));
		
		
	}
}
public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
	Configuration con = new Configuration();
	Job job = new Job(con,"UnitSold");
	job.setJarByClass(UnitSold.class);
	job.setMapperClass(Map.class);
	job.setReducerClass(Reduce.class);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(IntWritable.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);
	FileInputFormat.addInputPath(job,new Path(args[0]));
	FileOutputFormat.setOutputPath(job,new Path(args[1]));
    System.exit(job.waitForCompletion(true)?0:1);	
}
}


[acadgild@localhost ~]$ hadoop jar unitwisejar.jar /tele.txt /tele_unitOut1


[acadgild@localhost ~]$ hadoop fs -cat /tele_unitOut1/*
18/08/13 23:21:58 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Akai	1
Lava	3
NA	1
Onida	4
Samsung	7
Zen	2
[acadgild@localhost ~]$


#########################################END##################################################

