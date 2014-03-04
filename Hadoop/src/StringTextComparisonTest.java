import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.StringUtils;
import org.junit.Test;


public class StringTextComparisonTest extends WritableTestBase  {
	
	@Test
	public void string() throws UnsupportedEncodingException {
		String s = "\u0041\u00DF\u6771\uD801\uDC00";
		assertEquals(s.length(),(5));
		assertEquals(s.getBytes("UTF-8").length,(10));
		assertEquals(s.indexOf("\u0041"), (0));
		assertEquals(s.indexOf("\u00DF"), (1));
		assertEquals(s.indexOf("\u6771"), (2));
		assertEquals(s.indexOf("\uD801\uDC00"),(3));
		assertEquals(s.charAt(0), ('\u0041'));
		assertEquals(s.charAt(1), ('\u00DF'));
		assertEquals(s.charAt(2), ('\u6771'));
		assertEquals(s.charAt(3), ('\uD801'));
		assertEquals(s.charAt(4), ('\uDC00'));
		assertEquals(s.codePointAt(0), (0x0041));
		assertEquals(s.codePointAt(1), (0x00DF));
		assertEquals(s.codePointAt(2), (0x6771));
		assertEquals(s.codePointAt(3), (0x10400));
	}
	
	@Test
	public void text1() {
		Text t = new Text("\u0041\u00DF\u6771\uD801\uDC00");
		
		assertEquals(t.getLength(), 10);
		assertEquals(t.find("\u0041"), (0));
		assertEquals(t.find("\u00DF"), (1));
		assertEquals(t.find("\u6771"), (3));
		assertEquals(t.find("\uD801\uDC00"), (6));
		assertEquals(t.charAt(0), (0x0041));
		assertEquals(t.charAt(1), (0x00DF));
		assertEquals(t.charAt(3), (0x6771));
		assertEquals(t.charAt(6), (0x10400));
	}
	
	@Test
	public void text2() {
		
		Text t = new Text("hadoop");
		t.set("pig");
		assertEquals(t.getLength(), (3));
		assertEquals(t.getBytes().length, (3));		
	}
	
	@Test
	public void text3() {
		
		Text t = new Text("hadoop");
		t.set(new Text("pig"));
		assertEquals(t.getLength(), (3));
		assertEquals("Byte length not shortened", t.getBytes().length,(6));	
		
	}
	
	@Test
	public void Bytes() throws IOException {
		
		BytesWritable b = new BytesWritable(new byte[] { 3, 5 });
		byte[] bytes = serialize(b);
		assertEquals(StringUtils.byteToHexString(bytes),("000000020305"));
		b.setCapacity(11);
		assertEquals(b.getLength(),(2));
		assertEquals(b.getBytes().length, (11));
	}
	
	@SuppressWarnings("deprecation")
	@Test
	public void MapWritable() throws IOException{
		
		MapWritable src = new MapWritable();
		src.put(new IntWritable(1), new Text("cat"));
		src.put(new VIntWritable(2), new LongWritable(163));
		MapWritable dest = new MapWritable();
		WritableUtils.cloneInto(dest, src);
		assertEquals((Text) dest.get(new IntWritable(1)), (new Text("cat")));
		assertEquals((LongWritable) dest.get(new VIntWritable(2)),(new LongWritable(163)));
	}
}