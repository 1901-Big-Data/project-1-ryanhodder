package com.revature.reduce;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceQ2 extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{

	@Override
	protected void reduce(Text k, Iterable<DoubleWritable> values, 
			Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException{
		
		double total = 0.0;
		double d = 0.0;
		ArrayList<Double> yearValues = new ArrayList<Double>();
		ArrayList<Double> diff = new ArrayList<Double>();
		//create list
		//get difference for each year, store in list
		//can iterate over list
		//can use this to get the avg difference 
		//do this for each type
		
		for(DoubleWritable x: values) {
			yearValues.add(x.get());
		}
		
		for(int i = 1; i < yearValues.size(); i++) {
			d = yearValues.get(i) - yearValues.get(i - 1);
			diff.add(d);
		}
		for(double y: diff) {
			total += y;
		}
		//should be actually one less than in the list
		total = (total / (diff.size()-1));
		DecimalFormat dp = new DecimalFormat("#.000");
		//want to think about shortening to just a few dp
		//5dp because that is what the data is in
		//dont want to be more accurate
		context.write(k, new DoubleWritable(new Double(dp.format(total))));
	}
}
