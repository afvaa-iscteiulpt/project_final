package j2;


import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class Conf {

	String currentFolder = System.getProperty("user.dir");
	
	String sensorTopic;
	String sensorQoS;
	
	String mongoIp;
	String mongoPort;
	String mongoUser;
	String mongoPass;
	
	public void loadParameters() throws FileNotFoundException, IOException, ParseException {
		JSONParser parser = new JSONParser(); 
		JSONObject jsonObject = (JSONObject) parser.parse(new FileReader(currentFolder + "\\j2.conf"));

		JSONObject sensor = (JSONObject) jsonObject.get("sensor");
		sensorTopic = (String) sensor.get("topic");
		sensorQoS = (String) sensor.get("qos");
		
		JSONObject mongo = (JSONObject) jsonObject.get("mongodb");
		mongoIp = (String) mongo.get("ip_server");
		mongoPort = (String) mongo.get("port");
		mongoUser = (String) mongo.get("user");
		mongoPass = (String) mongo.get("password");
		
	}

	public String getCurrentFolder() {
		return currentFolder;
	}

	public String getMongoIp() {
		return mongoIp;
	}

	public String getMongoPort() {
		return mongoPort;
	}

	public String getMongoUser() {
		return mongoUser;
	}

	public String getMongoPass() {
		return mongoPass;
	}

	public String getSensorTopic() {
		return sensorTopic;
	}

	public String getSensorQoS() {
		return sensorQoS;
	}

}
