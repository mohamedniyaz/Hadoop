import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.*;
//import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;

public class hadoopPrac {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		
		String uri = "hdfs://localhost"+args[0];
		Configuration conf = new Configuration();
		
		
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		fs.setVerifyChecksum(false);
		//FSDataInputStream in = null;

	}

}
