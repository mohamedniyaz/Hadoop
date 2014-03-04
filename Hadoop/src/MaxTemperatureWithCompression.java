import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;


public class MaxTemperatureWithCompression {
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void main(String[] args) throws Exception {

		if (args.length != 2) {
			System.err.println("Usage: MaxTemperatureWithCompression <input path> " +
			"<output path>");
			System.exit(-1);
		}
		
		JobConf job = new JobConf();
		job.setJarByClass(MaxTemperature.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		job.setMapperClass((Class<? extends Mapper>) MaxTemperatureMapper.class);
		job.setCombinerClass((Class<? extends Reducer>) MaxTemperatureReducer.class);
		job.setReducerClass((Class<? extends Reducer>) MaxTemperatureReducer.class);
		//System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
}