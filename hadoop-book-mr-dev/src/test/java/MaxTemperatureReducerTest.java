import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.*;

public class MaxTemperatureReducerTest {
	
	@Test
	public void returnsMaximumIntegerInValues() throws IOException,	InterruptedException {
		
		Text value = new Text("1950");
		new ReduceDriver<Text, IntWritable, Text, IntWritable>()
		.withReducer(new MaxTemperatureReducer())
		.withInputKey(value)
		.withInputValues(Arrays.asList(new IntWritable(10), new IntWritable(5)))
		.withOutput(new Text("1950"), new IntWritable(10))
		.runTest();
	}

}
