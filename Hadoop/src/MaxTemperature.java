import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.io.Text;



public class MaxTemperature {
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void main(String[] args) throws Exception {
		
		if (args.length != 2) {
			System.err.println("Usage: MaxTemperature <input path> <output path>");
			System.exit(-1);
			
		}
			
			JobConf job = new JobConf();
			job.setJarByClass(MaxTemperature.class);
			job.setJobName("Max temperature");
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			
			job.setMapperClass((Class<? extends org.apache.hadoop.mapred.Mapper>) MaxTemperatureMapper.class);
			job.setReducerClass((Class<? extends org.apache.hadoop.mapred.Reducer>) MaxTemperatureReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			//System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}