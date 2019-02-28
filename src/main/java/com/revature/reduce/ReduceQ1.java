package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;


public class ReduceQ1 extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{

	@Override
	protected void reduce(Text k, Iterable<DoubleWritable> values, 
			Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException{
		
		//gets a total of each word
//		int total = 0;
//		
//		for(IntWritable i: values) {
//			total += i.get();
//		}
		Double d;
		
		for(DoubleWritable i: values) {
			
			d = i.get();
			//check percentage is less than 30%
			if(d < 30.0) {
				context.write(k, i);
			}
		}
	}
}
