package com.revature.map;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapQ5 extends Mapper<LongWritable, Text, Text, DoubleWritable>{

	static private String[] headers = {"Country Name","Country Code","Indicator Name","Indicator Code","1960","1961","1962","1963","1964","1965","1966","1967","1968","1969","1970","1971","1972","1973","1974","1975","1976","1977","1978","1979","1980","1981","1982","1983","1984","1985","1986","1987","1988","1989","1990","1991","1992","1993","1994","1995","1996","1997","1998","1999","2000","2001","2002","2003","2004","2005","2006","2007","2008","2009","2010","2011","2012","2013","2014","2015","2016"};
	static private ArrayList<String> headerList = new ArrayList<String>(); 
	
	{
		for(String h: headers) {
			headerList.add(h);	
		}
	}
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException{
		String l = value.toString();
		Double percent;
		
		String[] words = l.split("(\",\")");
		
		if(l.contains("NZL") && l.contains("School enrollment")) {
			try {
				int i = headerList.indexOf("2000");
				int indicator = headerList.indexOf("Indicator Name");
				
				if(l.contains("primary") && l.contains("female (% net)")) {
					for(int j = i; j < headerList.size(); j++) {
						//now do some checking that the percentage is actually allgood I guess
						//or do I just want to pass it like it is and then have the reducer deal with it
						percent = Double.parseDouble(words[j]);
						context.write(new Text(words[indicator]), new DoubleWritable(percent));
					}
				}
				if(l.contains("secondary") && l.contains("female (% net)")) {
					for(int j = i; j < headerList.size(); j++) {
						percent = Double.parseDouble(words[j]);
						context.write(new Text(words[indicator]), new DoubleWritable(percent));
					}
				}
				if(l.contains("tertiary") && l.contains("female (% gross)")) {
					for(int j = i; j < headerList.size(); j++) {
						percent = Double.parseDouble(words[j]);
						context.write(new Text(words[indicator]), new DoubleWritable(percent));
					}
				}
			}
			catch(NumberFormatException e) {
				
			}
			
		}
	}
}
