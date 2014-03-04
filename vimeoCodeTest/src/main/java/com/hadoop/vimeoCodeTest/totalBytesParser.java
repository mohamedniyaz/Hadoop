package com.hadoop.vimeoCodeTest;
import org.apache.hadoop.io.Text;

public class totalBytesParser {
	
	private String line;
	private String lineArray [];
	private String urlArray [];
	private String check;
	private String video_id;
	private int bytes;
	
	public String parse(Text record) {
		
		return (record.toString().trim());
	}
	
	
	public boolean validRecord(Text value){
		
		line = parse(value);
		lineArray  = line.split("\t");
		try {
			
			check = lineArray[6];
			if (check != " ")
				return true;
			else
				return false;
			
		}catch (Exception e){
			return false;
		}
	}
	
	public void process(Text value) {
		line = parse(value);
		line.trim();
		lineArray  = line.split("\t");
		
		urlArray = lineArray[4].split("/");
		video_id = urlArray [4].replace(".mp4", "").trim();
		bytes = Integer.parseInt(lineArray [6]);
	}
	
	
	public String getVideo_id() {
		return video_id;
	}
	
	public int getBytes() {
		return bytes;
	}
	
	

}
