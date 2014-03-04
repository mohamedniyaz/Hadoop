//import static org.junit.Assert.*;
//import static org.hamcrest.Matchers.is;
//import static org.hamcrest.Matchers.nullValue;
//import java.io.BufferedReader;
import java.io.IOException;
//import java.io.InputStream;
//import java.io.InputStreamReader;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.FileUtil;
//import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
//import org.apache.hadoop.mapred.OutputLogFilter;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.*;



//@SuppressWarnings("deprecation")
public class MaxTemperatureMapperTestV2 {
	
	@Test
	public void processesValidRecord() throws IOException, InterruptedException {
		
		Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" +  // Year ^^^^ 
							"99999V0203201N00261220001CN9999999N9-00111+99999999999"); // Temperature ^^^^^
		
		new MapDriver<LongWritable, Text, Text, IntWritable>()
		.withMapper(new MaxTemperatureMapperV1())
		.withInputValue(value)
		.withOutput(new Text("1950"), new IntWritable(-11))
		.runTest();
	}
	
	@Test
	public void processesNullRecord() throws IOException, InterruptedException {
		
		Text value = new Text(" ");
		
		new MapDriver<LongWritable, Text, Text, IntWritable>()
		.withMapper(new MaxTemperatureMapperV1())
		.withInputValue(value)
		.withOutput(new Text("2014"), new IntWritable(-100))
		.runTest();
	}
	
		
	@Test
	public void ignoresMissingTemperatureRecord() throws IOException,InterruptedException {
		
		Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" +  // Year ^^^^
							"99999V0203201N00261220001CN9999999N9+99991+99999999999"); // Temperature ^^^^^
		
		new MapDriver<LongWritable, Text, Text, IntWritable>()
		.withMapper(new MaxTemperatureMapperV1())
		.withInputValue(value)
		.runTest();
	}
	
	/* @Test
	  public void test() throws Exception {
	    Configuration conf = new Configuration();
	    conf.set("fs.default.name", "file:///");
	    conf.set("mapred.job.tracker", "local");
	    
	    Path input = new Path("input/ncdc/micro");
	    Path output = new Path("output");
	    
	    FileSystem fs = FileSystem.getLocal(conf);
	    fs.delete(output, true); // delete old output
	    
	    MaxTemperatureDriver driver = new MaxTemperatureDriver();
	    driver.setConf(conf);
	    
	    int exitCode = driver.run(new String[] {
	        input.toString(), output.toString() });
	    assertThat(exitCode, is(0));
	    
	    checkOutput(conf, output);
	  }
	//^^ MaxTemperatureDriverTestV3

	  private void checkOutput(Configuration conf, Path output) throws IOException {
	    FileSystem fs = FileSystem.getLocal(conf);
	    Path[] outputFiles = FileUtil.stat2Paths(
	        fs.listStatus(output, new OutputLogFilter()));
	    assertThat(outputFiles.length, is(1));
	    
	    BufferedReader actual = asBufferedReader(fs.open(outputFiles[0]));
	    BufferedReader expected = asBufferedReader(
	        getClass().getResourceAsStream("/expected.txt"));
	    String expectedLine;
	    while ((expectedLine = expected.readLine()) != null) {
	      assertThat(actual.readLine(), is(expectedLine));
	    }
	    assertThat(actual.readLine(), nullValue());
	    actual.close();
	    expected.close();
	  }
	  
	  private BufferedReader asBufferedReader(InputStream in) throws IOException {
	    return new BufferedReader(new InputStreamReader(in));
	  }*/
}