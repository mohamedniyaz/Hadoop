import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;


public class DeIdentifyData   {

	public static Integer[] encryptCol={2,3,4,5,6,8};
	private static byte[] key1 = new String("samplekey1234567").getBytes();
	
	public static class Map extends MapReduceBase implements
	Mapper<Object, Text, NullWritable, Text> {

		public void map(Object key, Text value,
				OutputCollector<NullWritable, Text> output, Reporter reporter)
		throws IOException {

			//value = PatientID, Name,DOB,Phone Number,Email_Address,SSN,Gender,Disease,weight
			
			StringTokenizer itr = new StringTokenizer(value.toString(),",");
			List<Integer> list=new ArrayList<Integer>();
			Collections.addAll(list, encryptCol);
			//list=2,3,4,5,6,8
			System.out.println("Mapper :: one :"+value);
			String  newStr="";

			int counter=1;

			while (itr.hasMoreTokens()) {
				String token=itr.nextToken(); 

				if(list.contains(counter))
				{
					if(newStr.length()>0)
						newStr+=",";

					newStr+=encrypt(token, key1);

				}
				else
				{
					if(newStr.length()>0)
						newStr+=",";
					newStr+=token;
				}	
				counter=counter+1;
			}
			output.collect( NullWritable.get(), new Text(newStr.toString()));
		}
	}


	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(DeIdentifyData.class);
		conf.setJobName("DeIdentifyData");
		
		
//		"NullWritable is a special type of Writable, " +
//		"as it has a zero-length serialization. No bytes are written to, " +
//		"or read from, the stream. It is used as a placeholder; for example, " +
//		"in MapReduce, a key or a value can be declared as a NullWritable " +
//		"when you don’t need to use that position—it effectively stores a constant empty value. " +
//		"NullWritable can also be useful as a key in SequenceFile when you want to store a list of values, " +
//		"as opposed to key-value pairs. It is an immutable singleton: the instance can be retrieved by calling NullWritable.get()"


		conf.setOutputKeyClass(NullWritable.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(Map.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
	}
	

	public static String encrypt(String strToEncrypt, byte[] key)
	{
		try
		{
			Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
			SecretKeySpec secretKey = new SecretKeySpec(key, "AES");
			cipher.init(Cipher.ENCRYPT_MODE, secretKey);
			String encryptedString = Base64.encodeBase64String(cipher.doFinal(strToEncrypt.getBytes()));
			return encryptedString.trim();
		}
		catch (Exception e)
		{
		}
		return null;

	}	
}