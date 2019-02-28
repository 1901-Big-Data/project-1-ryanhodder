package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;


public class ReduceQ1 extends Reducer<Text, DoubleWritable, Text, IntWritable>{

	@Override
	protected void reduce(Text k, Iterable<DoubleWritable> values, 
			Reducer<Text, DoubleWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException{
		
		//gets a total of each word
//		int total = 0;
//		
//		for(IntWritable i: values) {
//			total += i.get();
//		}
		
		for(DoubleWritable i: values) {
			context.write(k, new IntWritable(1));
		}
		
		//context.write(k, new IntWritable(1));
	}
}
