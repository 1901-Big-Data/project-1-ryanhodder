package com.revature.map;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapQ3 extends Mapper<LongWritable, Text, Text, DoubleWritable>{

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
		double percentage = 0.0;
		
		String[] words = l.split("(\",\")");
		
		//checks to see if it is the first line of the file
		if(words[0].matches("Country Name")) {
			return;
		}
		//code for unemployment in male % of labor force based on the national estimate
		if(l.contains("SL.UEM.TOTL.MA.ZS")) {
			try {
				int i = headerList.indexOf("2000");
				int country = headerList.indexOf("Country Name");
				String cString = words[country];
				cString = cString.substring(1, cString.length());
				
				for(; i < headerList.size(); i++) {
					percentage = Double.parseDouble(words[i]);
					context.write(new Text(cString), new DoubleWritable(percentage));
				}
			}
			catch(NumberFormatException e) {
				
			}
		}
	}
}
