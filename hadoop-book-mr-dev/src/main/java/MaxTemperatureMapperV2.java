import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class MaxTemperatureMapperV2 extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	private NcdcRecordParser parser = new NcdcRecordParser();
	
	@Override
	public void map(LongWritable key, Text value, Context context) 	throws IOException, InterruptedException {
		
		if(parser.validRecord(value) == true) {
			
			parser.parse(value);
			
			if (parser.isValidTemperature()) {
				
				context.write(new Text(parser.getYear()), 
							  new IntWritable(parser.getAirTemperature()));
			}			
		}
		else
			context.write(new Text("2014"), new IntWritable(-100));	
		
				
	}
}