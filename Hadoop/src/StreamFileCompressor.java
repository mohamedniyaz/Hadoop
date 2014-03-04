import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;


public class StreamFileCompressor {
	public static void main(String[] args) throws Exception {
		
		String codecClassname = "org.apache.hadoop.io.compress.GzipCodec";
		
		String fsrc = args[0];
		String fdst = args[1];
		
		
		InputStream in = new BufferedInputStream(new FileInputStream(fsrc));		
		Class<?> codecClass = Class.forName(codecClassname);
		
		Configuration conf = new Configuration();		
		CompressionCodec codec = (CompressionCodec)				
		ReflectionUtils.newInstance(codecClass, conf);
		
		LocalFileSystem fs = LocalFileSystem.getLocal(conf);	
		
		OutputStream fout = fs.create(new Path(fdst));
		
		CompressionOutputStream out = codec.createOutputStream(fout);		
		
		IOUtils.copyBytes(in, out, 4096, true);
		
		out.finish();
		
	}
}