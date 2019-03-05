package com.revature.Project1;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.map.MapQ1;
import com.revature.map.MapQ2;
import com.revature.map.MapQ3;
import com.revature.map.MapQ4;
import com.revature.map.MapQ5;
import com.revature.reduce.ReduceQ1;
import com.revature.reduce.ReduceQ2;
import com.revature.reduce.ReduceQ3;
import com.revature.reduce.ReduceQ4;
import com.revature.reduce.ReduceQ5;

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
		
		job.setMapperClass(MapQ2.class);
		job.setReducerClass(ReduceQ2.class);
		//job.setNumReduceTasks(0);
		//job.setCombinerClass(myCombinerClass);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		//If the inputFormat is something other than the default line with line termination
		//I need to specify here SetInputFormatClass
		
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}
}
