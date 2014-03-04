package com.hadoop.vimeoCodeTest;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;


public class totalBytesMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	static enum Records {
		  BAD
	  }
	
	private totalBytesParser parser = new totalBytesParser();
	
	@SuppressWarnings("rawtypes")
	private MultipleOutputs multipleOutputs;
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	protected void setup(Context context) throws InterruptedException {
			
		multipleOutputs = new MultipleOutputs(context);
    }

	@SuppressWarnings("unchecked")
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		if(parser.validRecord(value)== true){
			
			parser.process(value);			
			String video_id = parser.getVideo_id();
			int bytes = parser.getBytes();
			
			context.write(new Text(video_id), new IntWritable(bytes));
		}
		else{
			
			System.err.println("Ignoring possibly corrupt input: " + value);
	  	    context.getCounter(Records.BAD).increment(1);
			//context.write(new Text("BAD"), new IntWritable(1));
	  	    multipleOutputs.write ("BAD", value, new Text("Bad Records"));
	  	    
		}
		
	}
	
	 @Override()
     protected void cleanup(Context context) throws IOException, InterruptedException {
         multipleOutputs.close();
     }
   
}