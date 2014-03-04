import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class Word_Count {

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, IntWritable> {

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			
			// value = dear bear river dear
			
			//0 deer beer cat mouse -1
			//201 hadoop Gavin Pankaj Avni deer - 2 
			//404 shashank Lija Allwyn Sudheer beer Avni -3 

			// deer (1,1)
			// beer (1,1)
			// cat (1)
			// mouse (1)
			// avni (1,1)
			
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			// dear
			// bear
			// river
			// dear

			while (tokenizer.hasMoreTokens()) {
				value.set(tokenizer.nextToken());
				output.collect(value, new IntWritable(1));

				// I am fine I am fine - Input
				
				// I
				// am
				// fine
				// I
				// am
				// fine 
				// fine
				 // After Tokenizer
				
				// I 1
				// am 1
				// fine 1
				// I 1
				// am 1
				// fine 1
				// fine 1
				
				// After Map

				// I (1,1)
				// am (1,1)
				// fine (1,1,1)
				
				// After Shuffle and Sort

			}

		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, IntWritable, Text, IntWritable> {

		//K, list (values)
		@Override
		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			// Avni (1,1)
			
			// key =  Avni
			// values = (1,1)
			
			
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
				// sum = sum + 1;
			}

			// beer,3

			output.collect(key, new IntWritable(sum));
			// Avni 2
			
		}
	}

	public static void main(String[] args) throws Exception {

		JobConf conf = new JobConf(WordCount.class);
		conf.setJobName("wordcount");
		
		//conf.set("mapred.textoutputformat.separator", ";");
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
	
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setNumReduceTasks(2);
		
		JobClient.runJob(conf);

	}
}
