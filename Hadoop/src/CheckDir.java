import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class CheckDir {
	
	public static void main(String[] args) throws Exception {
		
		String dir = "hdfs://localhost"+args[0];
		Path p = new Path (dir);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(dir), conf);
		if (fs.exists(p))
			System.out.println("Exsits");
		else
			System.out.println("Nope");
		
		if(fs.mkdirs(p))
			System.out.println("Dir Exsits");
		else
			System.out.println("Dir Nope");
			
	}
}