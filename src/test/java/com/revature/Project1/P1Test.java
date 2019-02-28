package com.revature.Project1;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.revature.map.MapQ1;
import com.revature.reduce.ReduceQ1;

public class P1Test {
	
	//harnesses
	private MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;
	private ReduceDriver<Text, DoubleWritable, Text, IntWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, IntWritable> mapReduceDriver;
	
	@Before
	public void setUp() {

		/*
		 * Set up the mapper test harness.
		 */
		MapQ1 mapper = new MapQ1();
		mapDriver = new MapDriver<LongWritable, Text, Text, DoubleWritable>();
		mapDriver.setMapper(mapper);

		/*
		 * Set up the reducer test harness.
		 */
		ReduceQ1 reducer = new ReduceQ1();
		reduceDriver = new ReduceDriver<Text, DoubleWritable, Text, IntWritable>();
		reduceDriver.setReducer(reducer);

		/*
		 * Set up the mapper/reducer test harness.
		 */
		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, IntWritable>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}
	

//	@Test
//	public void testGenericMapper() {
//		String input = "I hope this works";
//		mapDriver.withInput(new LongWritable(1), new Text(input));
//		
//		mapDriver.withOutput(new Text("I"), new IntWritable(1));
//		mapDriver.withOutput(new Text("hope"), new IntWritable(1));
//		mapDriver.withOutput(new Text("this"), new IntWritable(1));
//		mapDriver.withOutput(new Text("works"), new IntWritable(1));
//		
//		mapDriver.runTest();
//	}
	
//	@Test
//	public void testEducationAttainmentMap1() {
//		String input1 = "Education attainment, this is m\"ore education attainment";
//		mapDriver.withInput(new LongWritable(1), new Text(input1));
//		mapDriver.withOutput(new Text("Education attainment"), new IntWritable(1));
//		mapDriver.withOutput(new Text(" this is m"), new IntWritable(1));
//		mapDriver.withOutput(new Text("ore education attainment"), new IntWritable(1));
//		
//		mapDriver.runTest();
//	}
//	
//	@Test
//	public void testEducationAttainmentMap2() {
//		String input2 = "This has Education attainment in the string ";
//		mapDriver.withInput(new LongWritable(1), new Text(input2));
//		mapDriver.withOutput(new Text(input2), new IntWritable(1));
//		
//		mapDriver.runTest();
//	}
	
//	@Test
//	public void testEducationAttainmentMap3() {
//		String input3 = "\"Hello\",\"hola\",\"Education attainment yes\",\"goodbye\"";
//		mapDriver.withInput(new LongWritable(1), new Text(input3));
//		mapDriver.withOutput(new Text("\"Hello\""), new IntWritable(1));
//		mapDriver.withOutput(new Text("\"hola\""), new IntWritable(1));
//		mapDriver.withOutput(new Text("\"Education attainment yes\""), new IntWritable(1));
//		mapDriver.withOutput(new Text("\"goodbye\""), new IntWritable(1));
//		
//		mapDriver.runTest();
//	}
	
//	@Test
//	public void testGenericReducer() {
//		List<IntWritable> testList = new ArrayList<IntWritable>();
//		testList.add(new IntWritable(1));
//		testList.add(new IntWritable(1));
//		
//		reduceDriver.withInput(new Text("working"), testList);
//		reduceDriver.withInput(new Text("working"), testList);
//		reduceDriver.withOutput(new Text("working"), new IntWritable(1));
//		reduceDriver.withOutput(new Text("working"), new IntWritable(1));
//		
////		reduceDriver.withInput(new Text("or is it"), testList);
////		reduceDriver.withOutput(new Text("or is it"), new IntWritable(1));
//		
//		reduceDriver.runTest();
//	}
	
//	@Test
//	public void testGenericMapReduce() {
//		String input = "Testing how well this works this this this!";
//		mapReduceDriver.withInput(new LongWritable(1), new Text(input));
//		mapReduceDriver.withOutput(new Text("Testing"), new IntWritable(1));
//		mapReduceDriver.withOutput(new Text("how"), new IntWritable(1));
//		mapReduceDriver.withOutput(new Text("this"), new IntWritable(4));
//		mapReduceDriver.withOutput(new Text("well"), new IntWritable(1));
//		mapReduceDriver.withOutput(new Text("works"), new IntWritable(1));
//		
//		//order of the output matters
//		//seems like it needs to be alphabetical order
//		//with capitals first
//		mapReduceDriver.runTest();
//	}
}
