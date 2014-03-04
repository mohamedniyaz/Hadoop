import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class TestConfiguration {

	@Test
	public void config() throws IOException{
		
		Configuration conf = new Configuration();
		conf.addResource("configuration-1.xml");
		conf.addResource("configuration-2.xml");
		
		assertEquals(conf.get("color"),"yellow");
		assertEquals(conf.get("size"), "12");
		assertEquals(conf.getInt("size",0), 12);
		assertEquals(conf.get("weight"), "heavy");
		assertEquals(conf.get("size-weight"),("12,heavy"));
		
		System.setProperty("length", "2");
		assertEquals(System.getProperty("length"), "2");
		
	}

}
