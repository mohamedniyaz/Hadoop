package com.hadoop.vimeoCodeTest;

import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.*;

public class totalBytesReducerTest {
	
	@Test
	public void processValidRecords() throws IOException,	InterruptedException {
		
		Text value = new Text("231");
		new ReduceDriver<Text, IntWritable, Text, IntWritable>()
		.withReducer(new totalBytesReducer())
		.withInputKey(value)
		.withInputValues(Arrays.asList(new IntWritable(400), new IntWritable(600)))
		.withOutput(new Text("231"), new IntWritable(1000))
		.runTest();
	}
	
	@Test
	public void processBadRecords() throws IOException,	InterruptedException {
		
		Text value = new Text("BAD");
		new ReduceDriver<Text, IntWritable, Text, IntWritable>()
		.withReducer(new totalBytesReducer())
		.withInputKey(value)
		.withInputValues(Arrays.asList(new IntWritable(1), new IntWritable(1)))
		.withOutput(new Text("BAD"), new IntWritable(2))
		.runTest();
	}

}
