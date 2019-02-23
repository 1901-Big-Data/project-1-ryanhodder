package com.revature.Project1;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//import my classes

public class Driver {
	
	public static void main(String [] args) throws Exception{
		
		//Check the input is right
		if (args.length != 2) {
			System.out.print("Usage: Project1 <input dir> <output dir>\n");
			System.exit(-1);
		}
		
		Job job = new Job();
		
		job.setJarByClass(Driver.class);
		job.setJobName("Project1");
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//job.setMapperClass(myMapperClass);
		//job.setReducerClass(myreducerClass);
		//job.setCombinerClass(myCombinerClass);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		//If the inputFormat is something other than the default line with line termination
		//I need to specify here SetInputFormatClass
		
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}
}
