package com.revature.Project1;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.revature.map.MapQ1;

public class P1Test {
	
	//harnesses
	private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;
	
	@Before
	public void setUp() {

		/*
		 * Set up the mapper test harness.
		 */
		MapQ1 mapper = new MapQ1();
		mapDriver = new MapDriver<LongWritable, Text, Text, IntWritable>();
		mapDriver.setMapper(mapper);

//		/*
//		 * Set up the reducer test harness.
//		 */
//		SumReducer reducer = new SumReducer();
//		reduceDriver = new ReduceDriver<Text, IntWritable, Text, IntWritable>();
//		reduceDriver.setReducer(reducer);
//
//		/*
//		 * Set up the mapper/reducer test harness.
//		 */
//		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable>();
//		mapReduceDriver.setMapper(mapper);
//		mapReduceDriver.setReducer(reducer);
	}
	

	@Test
	public void testGenericMapper() {
		String input = "I hope this works";
		mapDriver.withInput(new LongWritable(1), new Text(input));
		
		mapDriver.withOutput(new Text("I"), new IntWritable(1));
		mapDriver.withOutput(new Text("hope"), new IntWritable(1));
		mapDriver.withOutput(new Text("this"), new IntWritable(1));
		mapDriver.withOutput(new Text("works"), new IntWritable(1));
		
		mapDriver.runTest();
	}
}
