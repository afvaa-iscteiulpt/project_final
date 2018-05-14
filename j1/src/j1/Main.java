package j1;

import java.io.IOException;

import org.json.simple.parser.ParseException;

public class Main {

	public static void main(String[] args) throws IOException {
		
		//PREPARE LOG FILE
		Log logFile = new Log();
		logFile.log("LogFile Ready.", TypeLog.INITIAL);
		
		//LOAD CONFIG FILE
		Conf confFile = new Conf();		
		try {
			confFile.loadParameters();
			logFile.log("ConfigFile Ready.", TypeLog.NORMAL);
		} catch (IOException | ParseException e) {
			logFile.log("ConfigFile with problems.", TypeLog.ERROR);
		}
				
		//START PROGRAM
		logFile.log("Starting program.", TypeLog.NORMAL);
		J1.startProgram(confFile, logFile);
	}

}
