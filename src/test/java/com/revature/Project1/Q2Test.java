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

import com.revature.map.MapQ2;
import com.revature.reduce.ReduceQ2;


public class Q2Test {
	
	//harnesses
	private MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;
	private ReduceDriver<Text, DoubleWritable, Text, DoubleWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable> mapReduceDriver;
	
	@Before
	public void setUp() {

		/*
		 * Set up the mapper test harness.
		 */
		MapQ2 mapper = new MapQ2();
		mapDriver = new MapDriver<LongWritable, Text, Text, DoubleWritable>();
		mapDriver.setMapper(mapper);

		/*
		 * Set up the reducer test harness.
		 */
		ReduceQ2 reducer = new ReduceQ2();
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
	 * 
	 * 
	 */
	@Test
	public void testMapperGoodData() {
		String input = "\"United States\",\"USA\",\"School enrollment, primary, female (% net)\",\"SE.PRM.NENR.FE\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"94.35473\",\"96.07038\",\"\",\"\",\"98.6454\",\"98.23264\",\"\",\"96.03438\",\"96.03576\",\"97.18752\",\"96.78507\",\"\",\"\",\"96.74318\",\"96.84398\",\"97.07956\",\"95.69427\",\"95.43967\",\"94.11045\",\"94.42654\",\"95.79755\",\"96.439\",\"96.82568\",\"95.78414\",\"93.85194\",\"93.59352\",\"92.98951\",\"92.62706\",\"93.32211\",\"94.0818\",\"\",";
		mapDriver.withInput(new LongWritable(1), new Text(input));
		mapDriver.withOutput(new Text("School enrollment, primary, female (% net)"), new DoubleWritable(96.84398));
		mapDriver.withOutput(new Text("School enrollment, primary, female (% net)"), new DoubleWritable(97.07956));
		mapDriver.withOutput(new Text("School enrollment, primary, female (% net)"), new DoubleWritable(95.69427));
		mapDriver.withOutput(new Text("School enrollment, primary, female (% net)"), new DoubleWritable(95.43967));
		mapDriver.withOutput(new Text("School enrollment, primary, female (% net)"), new DoubleWritable(94.11045));
		mapDriver.withOutput(new Text("School enrollment, primary, female (% net)"), new DoubleWritable(94.42654));
		mapDriver.withOutput(new Text("School enrollment, primary, female (% net)"), new DoubleWritable(95.79755));
		mapDriver.withOutput(new Text("School enrollment, primary, female (% net)"), new DoubleWritable(96.439));
		mapDriver.withOutput(new Text("School enrollment, primary, female (% net)"), new DoubleWritable(96.82568));
		mapDriver.withOutput(new Text("School enrollment, primary, female (% net)"), new DoubleWritable(95.78414));
		mapDriver.withOutput(new Text("School enrollment, primary, female (% net)"), new DoubleWritable(93.85194));
		mapDriver.withOutput(new Text("School enrollment, primary, female (% net)"), new DoubleWritable(93.59352));
		mapDriver.withOutput(new Text("School enrollment, primary, female (% net)"), new DoubleWritable(92.98951));
		mapDriver.withOutput(new Text("School enrollment, primary, female (% net)"), new DoubleWritable(92.62706));
		mapDriver.withOutput(new Text("School enrollment, primary, female (% net)"), new DoubleWritable(93.32211));
		mapDriver.withOutput(new Text("School enrollment, primary, female (% net)"), new DoubleWritable(94.0818));
		
		mapDriver.runTest();
	}
	
	/**
	 * Another test 
	 * just wanted to use less data
	 * Only data at 2000 position
	 */
	@Test
	public void testMapperMostlyEmptyData() {
		String input = "\"United States\",\"USA\",\"School enrollment, primary, female (% net)\",\"SE.PRM.NENR.FE\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"94.35473\",\"96.07038\",\"\",\"\",\"98.6454\",\"98.23264\",\"\",\"96.03438\",\"96.03576\",\"97.18752\",\"96.78507\",\"\",\"\",\"96.74318\",\"96.84398\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",";
		mapDriver.withInput(new LongWritable(1), new Text(input));
		mapDriver.withOutput(new Text("School enrollment, primary, female (% net)"), new DoubleWritable(96.84398));
		
		mapDriver.runTest();
	}
	
	/**
	 * Tests mostly empty data set
	 * But checks that it works at secondary level
	 */
	@Test
	public void testMapperMostlyEmptySecondary() {
		String input = "\"United States\",\"USA\",\"School enrollment, secondary, female (% net)\",\"SE.PRM.NENR.FE\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"94.35473\",\"96.07038\",\"\",\"\",\"98.6454\",\"98.23264\",\"\",\"96.03438\",\"96.03576\",\"97.18752\",\"96.78507\",\"\",\"\",\"96.74318\",\"96.84398\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",";
		mapDriver.withInput(new LongWritable(1), new Text(input));
		mapDriver.withOutput(new Text("School enrollment, secondary, female (% net)"), new DoubleWritable(96.84398));
		
		mapDriver.runTest();
	}
	
	/**
	 * Tests mostly empty data set
	 * But checks that it works at tertiary level
	 * Also tertiary requires gross % due to data set
	 */
	@Test
	public void testMapperMostlyEmptyTertiary() {
		String input = "\"United States\",\"USA\",\"School enrollment, tertiary, female (% gross)\",\"SE.PRM.NENR.FE\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"94.35473\",\"96.07038\",\"\",\"\",\"98.6454\",\"98.23264\",\"\",\"96.03438\",\"96.03576\",\"97.18752\",\"96.78507\",\"\",\"\",\"96.74318\",\"96.84398\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",";
		mapDriver.withInput(new LongWritable(1), new Text(input));
		mapDriver.withOutput(new Text("School enrollment, tertiary, female (% gross)"), new DoubleWritable(96.84398));
		
		mapDriver.runTest();
	}
	
	/**
	 * Makes sure that it can't be tertiary and not be gross %
	 */
	@Test
	public void testMapperMostlyEmptyTertiaryNotGross() {
		String input = "\"United States\",\"USA\",\"School enrollment, tertiary, female (% net)\",\"SE.PRM.NENR.FE\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"94.35473\",\"96.07038\",\"\",\"\",\"98.6454\",\"98.23264\",\"\",\"96.03438\",\"96.03576\",\"97.18752\",\"96.78507\",\"\",\"\",\"96.74318\",\"96.84398\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",";
		mapDriver.withInput(new LongWritable(1), new Text(input));
		
		mapDriver.runTest();
	}
	
	/**
	 * No data at the 2000 position
	 * No output
	 */
	@Test
	public void testMapperNoData2000() {
		String input = "\"United States\",\"USA\",\"School enrollment, primary, female (% net)\",\"SE.PRM.NENR.FE\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"94.35473\",\"96.07038\",\"\",\"\",\"98.6454\",\"98.23264\",\"\",\"96.03438\",\"96.03576\",\"97.18752\",\"96.78507\",\"\",\"\",\"96.74318\",\"\",\"97.07956\",\"95.69427\",\"95.43967\",\"94.11045\",\"94.42654\",\"95.79755\",\"96.439\",\"96.82568\",\"95.78414\",\"93.85194\",\"93.59352\",\"92.98951\",\"92.62706\",\"93.32211\",\"94.0818\",\"\",";
		mapDriver.withInput(new LongWritable(1), new Text(input));
		
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
	 * Specifically checks for female
	 */
	@Test
	public void testMapperBadDataMale() {
		String input = "\"United States\",\"USA\",\"School enrollment, primary, male (% net)\",\"SE.PRM.NENR.FE\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"94.35473\",\"96.07038\",\"\",\"\",\"98.6454\",\"98.23264\",\"\",\"96.03438\",\"96.03576\",\"97.18752\",\"96.78507\",\"\",\"\",\"96.74318\",\"96.84398\",\"97.07956\",\"95.69427\",\"95.43967\",\"94.11045\",\"94.42654\",\"95.79755\",\"96.439\",\"96.82568\",\"95.78414\",\"93.85194\",\"93.59352\",\"92.98951\",\"92.62706\",\"93.32211\",\"94.0818\",\"\",";
		mapDriver.withInput(new LongWritable(1), new Text(input));
		mapDriver.runTest();
	}
	
	/**
	 * Specifically checks for level of schooling
	 */
	@Test
	public void testMapperBadDataLevel() {
		String input = "\"United States\",\"USA\",\"School enrollment, kindergarten, female (% net)\",\"SE.PRM.NENR.FE\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"94.35473\",\"96.07038\",\"\",\"\",\"98.6454\",\"98.23264\",\"\",\"96.03438\",\"96.03576\",\"97.18752\",\"96.78507\",\"\",\"\",\"96.74318\",\"96.84398\",\"97.07956\",\"95.69427\",\"95.43967\",\"94.11045\",\"94.42654\",\"95.79755\",\"96.439\",\"96.82568\",\"95.78414\",\"93.85194\",\"93.59352\",\"92.98951\",\"92.62706\",\"93.32211\",\"94.0818\",\"\",";
		mapDriver.withInput(new LongWritable(1), new Text(input));
		mapDriver.runTest();
	}
	
	/**
	 * Specifically checks for USA country code
	 */
	@Test
	public void testMapperBadDataUSA() {
		String input = "\"United States\",\"ASU\",\"School enrollment, primary, female (% net)\",\"SE.PRM.NENR.FE\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"94.35473\",\"96.07038\",\"\",\"\",\"98.6454\",\"98.23264\",\"\",\"96.03438\",\"96.03576\",\"97.18752\",\"96.78507\",\"\",\"\",\"96.74318\",\"96.84398\",\"97.07956\",\"95.69427\",\"95.43967\",\"94.11045\",\"94.42654\",\"95.79755\",\"96.439\",\"96.82568\",\"95.78414\",\"93.85194\",\"93.59352\",\"92.98951\",\"92.62706\",\"93.32211\",\"94.0818\",\"\",";
		mapDriver.withInput(new LongWritable(1), new Text(input));
		mapDriver.runTest();
	}
	
	/**
	 * No data found at 2000 position or onwards
	 */
	@Test
	public void testMapperNoData() {
		String input = "\"United States\",\"USA\",\"School enrollment, primary, female (% net)\",\"SE.PRM.NENR.FE\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"94.35473\",\"96.07038\",\"\",\"\",\"98.6454\",\"98.23264\",\"\",\"96.03438\",\"96.03576\",\"97.18752\",\"96.78507\",\"\",\"\",\"96.74318\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",";
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
	  * First line of file
	  */
	@Test
	public void testMapReduceFirstLine() {
		String input = "\"Country Name\",\"Country Code\",\"Indicator Name\",\"Indicator Code\",\"1960\",\"1961\",\"1962\",\"1963\",\"1964\",\"1965\",\"1966\",\"1967\",\"1968\",\"1969\",\"1970\",\"1971\",\"1972\",\"1973\",\"1974\",\"1975\",\"1976\",\"1977\",\"1978\",\"1979\",\"1980\",\"1981\",\"1982\",\"1983\",\"1984\",\"1985\",\"1986\",\"1987\",\"1988\",\"1989\",\"1990\",\"1991\",\"1992\",\"1993\",\"1994\",\"1995\",\"1996\",\"1997\",\"1998\",\"1999\",\"2000\",\"2001\",\"2002\",\"2003\",\"2004\",\"2005\",\"2006\",\"2007\",\"2008\",\"2009\",\"2010\",\"2011\",\"2012\",\"2013\",\"2014\",\"2015\",\"2016\"";
		mapReduceDriver.withInput(new LongWritable(1), new Text(input));
		mapReduceDriver.runTest();
	}
}
