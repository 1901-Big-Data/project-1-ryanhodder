package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceQ5 extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{

	@Override
	protected void reduce(Text k, Iterable<DoubleWritable> values, 
			Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException{
		
	}
}
