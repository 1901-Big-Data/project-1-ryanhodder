package com.revature.reduce;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceQ3 extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{

	@Override
	protected void reduce(Text k, Iterable<DoubleWritable> values, 
			Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException{
		
		ArrayList<Double> emp = new ArrayList<Double>();
		//took in unemployment rate
		//need to flip it to employment rates
		for(DoubleWritable d: values) {
			emp.add(100.0 - (d.get()));
		}
		
		double first = emp.get(0);
		double last = emp.get(emp.size()-1);
		
		double change = last - first;
		context.write(k, new DoubleWritable(change));
	}
}
