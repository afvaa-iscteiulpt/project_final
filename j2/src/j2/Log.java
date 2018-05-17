package j2;


import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class Log {

	String currentFolder = System.getProperty("user.dir");
	String file = currentFolder + "\\j2.log";
	
	@SuppressWarnings("resource")
	public void log(String logText, TypeLog typeLog) throws IOException {
		
		String initial = "";
		String error = "";
		if(typeLog == TypeLog.INITIAL) {
			initial = "\n\n";
		}
		else if(typeLog == TypeLog.ERROR) {
			error = "ERROR ";
		}
			
		FileWriter aWriter = new FileWriter(file, true);
		aWriter.write(initial + getTimeStamp() + " " + error + logText + "\n");
		aWriter.flush();
	}
	
	public String getTimeStamp() {
		TimeZone tz = TimeZone.getTimeZone("Europe/Lisbon");
		Date now = new Date();
		DateFormat df = new SimpleDateFormat ("yyyy.MM.dd hh:mm:ss ");
		df.setTimeZone(tz);
		String currentTime = df.format(now);
		
		return currentTime;
	}

}