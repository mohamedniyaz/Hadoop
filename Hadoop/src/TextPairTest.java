import static org.junit.Assert.assertEquals;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.junit.Test;


public class TextPairTest extends TextPair {
	
	@Test
	public void customWritable() throws IOException{
		
		TextPair src = new TextPair();
		src.set(new Text("Map1"), new Text("Reduce1"));
		src.set(new Text("Map2"), new Text("Reduce2"));
		src.set(new Text("Map3"), new Text("Reduce3"));
		
		assertEquals((Text) src.getFirst(), (new Text("Map3")));
				
	}	

}
