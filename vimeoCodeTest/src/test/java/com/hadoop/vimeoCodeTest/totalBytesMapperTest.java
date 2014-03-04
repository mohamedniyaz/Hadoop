package com.hadoop.vimeoCodeTest;


import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;
//import com.hadoop.vimeoCodeTest.totalBytesMapper.Records;
//import static org.junit.Assert.*;

public class totalBytesMapperTest {
	
	MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	
	@Before
	public void setUp() {
		
		totalBytesMapper mapper = new totalBytesMapper();
	    mapDriver = MapDriver.newMapDriver(mapper);;
	    
	}
		
	@Test
	public void processesValidRecord() throws IOException, InterruptedException {
		
		Text value = new Text("2012-01-22	01:22:26	10.1.1.1	GET	/xxxxx.download.akamai.com/1234/123/231.mp4" + 
							"	200	400	0	http://abc.com/1234/123/ " + 
							"	Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.2.18) Gecko/20110614 Firefox/3.6.18" + "	-"); 
		
		new MapDriver<LongWritable, Text, Text, IntWritable>()
		.withMapper(new totalBytesMapper())
		.withInputValue(value)
		.withOutput(new Text("231"), new IntWritable(400))
		.runTest();
	}	
	
		
/*	@Test
	public void processesCounter() throws IOException, InterruptedException {
		
		Text value = new Text("#Fields: date time cs-ip cs-method cs-uri sc-status sc-bytes time-taken cs(Referer) cs(User-Agent) cs(Cookie)"); 
		
		mapDriver.withInputValue(value);		
		mapDriver.runTest();		
		assertEquals(1, mapDriver.getCounters().findCounter(Records.BAD).getValue());
	}
	
	
	@Test
	public void processesNullRecord() throws IOException, InterruptedException {
		
		Text value = new Text("	"); 
		
		new MapDriver<LongWritable, Text, Text, IntWritable>()
		.withMapper(new totalBytesMapper())
		.withInputValue(value)
		.withOutput(new Text("BAD"), new IntWritable(1))		
		.runTest();
	}*/
}