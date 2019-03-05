package com.revature.Project1;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.revature.map.MapQ1;
import com.revature.reduce.ReduceQ1;


public class Q1Test {
	
	//harnesses
	private MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;
	private ReduceDriver<Text, DoubleWritable, Text, DoubleWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable> mapReduceDriver;
	
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
		reduceDriver = new ReduceDriver<Text, DoubleWritable, Text, DoubleWritable>();
		reduceDriver.setReducer(reducer);

		/*
		 * Set up the mapper/reducer test harness.
		 */
		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}
	
	/**
	 * Sample piece of data taken from data set
	 * Should output the country name and the data from the 2015 slot
	 * Where the indicator code is "SE.TER.CUAT.BA.FE.ZS"
	 */
	@Test
	public void testMapperGoodData() {
		String input = "\"New Zealand\",\"NZL\",\"Educational attainment, at least Bachelor's or equivalent, population 25+, female (%) (cumulative)\",\"SE.TER.CUAT.BA.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"27.29524\",\"27.85181\",\"\",";
		mapDriver.withInput(new LongWritable(1), new Text(input));
		mapDriver.withOutput(new Text("New Zealand"), new DoubleWritable(27.85181));
		mapDriver.runTest();
	}
	
	/**
	 * When bad data is found, gives no output
	 */
	@Test
	public void testMapperBadData() {
		String input = "\"I\",\"hope\",\"this\",\"works!\"";
		mapDriver.withInput(new LongWritable(1), new Text(input));
		mapDriver.runTest();
	}
	
	/**
	 * When no data is found there is no output
	 */
	@Test
	public void testMapperNoData() {
		String input = "\"New Zealand\",\"NZL\",\"Educational attainment, at least Bachelor's or equivalent, population 25+, female (%) (cumulative)\",\"SE.TER.CUAT.BA.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",";
		mapDriver.withInput(new LongWritable(1), new Text(input));
		mapDriver.runTest();
	}
	
	/**
	 * Test first line of file
	 */
	@Test
	public void testFirstLineMap() {
		String input = "\"Country Name\",\"Country Code\",\"Indicator Name\",\"Indicator Code\",\"1960\",\"1961\",\"1962\",\"1963\",\"1964\",\"1965\",\"1966\",\"1967\",\"1968\",\"1969\",\"1970\",\"1971\",\"1972\",\"1973\",\"1974\",\"1975\",\"1976\",\"1977\",\"1978\",\"1979\",\"1980\",\"1981\",\"1982\",\"1983\",\"1984\",\"1985\",\"1986\",\"1987\",\"1988\",\"1989\",\"1990\",\"1991\",\"1992\",\"1993\",\"1994\",\"1995\",\"1996\",\"1997\",\"1998\",\"1999\",\"2000\",\"2001\",\"2002\",\"2003\",\"2004\",\"2005\",\"2006\",\"2007\",\"2008\",\"2009\",\"2010\",\"2011\",\"2012\",\"2013\",\"2014\",\"2015\",\"2016\"";
		mapDriver.withInput(new LongWritable(1), new Text(input));
		mapDriver.runTest();
	}
	
	/**
	 * Tests data that shouldn't be filtered out (under 30%)
	 */
	@Test
	public void testReducerGoodData() {
		List<DoubleWritable> testList = new ArrayList<DoubleWritable>();
		testList.add(new DoubleWritable(25.0));
		
		reduceDriver.withInput(new Text("New Zealand"), testList);
		reduceDriver.withOutput(new Text("New Zealand"), new DoubleWritable(25.0));
		reduceDriver.runTest();
	}
	
	/**
	 * Test data that should be filtered out (over 30%)
	 * No output
	 */
	@Test
	public void testReducerTooHigh() {
		List<DoubleWritable> testList = new ArrayList<DoubleWritable>();
		testList.add(new DoubleWritable(45.0));
		
		reduceDriver.withInput(new Text("New Zealand"), testList);
		reduceDriver.runTest();
	}
	
	/**
	 * MapReduce with good data
	 * Should be same results as the mapper itself
	 */
	@Test
	public void testMapReduceGoodData() {
		String input = "\"New Zealand\",\"NZL\",\"Educational attainment, at least Bachelor's or equivalent, population 25+, female (%) (cumulative)\",\"SE.TER.CUAT.BA.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"27.29524\",\"27.85181\",\"\",";
		mapReduceDriver.withInput(new LongWritable(1), new Text(input));
		mapReduceDriver.withOutput(new Text("New Zealand"), new DoubleWritable(27.85181));
		mapReduceDriver.runTest();
	}
	
	/**
	 * Data that should be filtered out
	 */
	@Test
	public void testMapReduceBadData() {
		String input = "\"New Zealand\",\"NZL\",\"Educational attainment, at least Bachelor's or equivalent, population 25+, female (%) (cumulative)\",\"SE.TER.CUAT.BA.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"27.29524\",\"47.85181\",\"\",";
		mapReduceDriver.withInput(new LongWritable(1), new Text(input));
		mapReduceDriver.runTest();
	}
	
	/**
	 * No data at 2015 position
	 */
	@Test
	public void testMapReduceNoData() {
		String input = "\"New Zealand\",\"NZL\",\"Educational attainment, at least Bachelor's or equivalent, population 25+, female (%) (cumulative)\",\"SE.TER.CUAT.BA.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"27.29524\",\"\",\"\",";
		mapReduceDriver.withInput(new LongWritable(1), new Text(input));
		mapReduceDriver.runTest();
	}
	 /**
	  * First line of file
	  */
	@Test
	public void testMapReduceFirstLine() {
		String input = "\"Country Name\",\"Country Code\",\"Indicator Name\",\"Indicator Code\",\"1960\",\"1961\",\"1962\",\"1963\",\"1964\",\"1965\",\"1966\",\"1967\",\"1968\",\"1969\",\"1970\",\"1971\",\"1972\",\"1973\",\"1974\",\"1975\",\"1976\",\"1977\",\"1978\",\"1979\",\"1980\",\"1981\",\"1982\",\"1983\",\"1984\",\"1985\",\"1986\",\"1987\",\"1988\",\"1989\",\"1990\",\"1991\",\"1992\",\"1993\",\"1994\",\"1995\",\"1996\",\"1997\",\"1998\",\"1999\",\"2000\",\"2001\",\"2002\",\"2003\",\"2004\",\"2005\",\"2006\",\"2007\",\"2008\",\"2009\",\"2010\",\"2011\",\"2012\",\"2013\",\"2014\",\"2015\",\"2016\"";
		mapReduceDriver.withInput(new LongWritable(1), new Text(input));
		mapReduceDriver.runTest();
	}
}
