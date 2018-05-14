package j1;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class Conf {

	String currentFolder = System.getProperty("user.dir");
	
	String sybaseIp;
	String sybasePort;
	String sybaseUser;
	String sybasePass;
	
	String mongoIp;
	String mongoPort;
	String mongoUser;
	String mongoPass;
	
	String periodicity;
	
	public void loadParameters() throws FileNotFoundException, IOException, ParseException {
		JSONParser parser = new JSONParser(); 
		JSONObject jsonObject = (JSONObject) parser.parse(new FileReader(currentFolder + "\\j1.conf"));

		JSONObject sybase = (JSONObject) jsonObject.get("sybase");
		sybaseIp = (String) sybase.get("ip_server");
		sybasePort = (String) sybase.get("port");
		sybaseUser = (String) sybase.get("user");
		sybasePass = (String) sybase.get("password");
		
		JSONObject mongo = (JSONObject) jsonObject.get("mongodb");
		mongoIp = (String) mongo.get("ip_server");
		mongoPort = (String) mongo.get("port");
		mongoUser = (String) mongo.get("user");
		mongoPass = (String) mongo.get("password");
		
		periodicity = (String) jsonObject.get("periodicity");
	}

	public String getCurrentFolder() {
		return currentFolder;
	}

	public String getSybaseIp() {
		return sybaseIp;
	}

	public String getSybasePort() {
		return sybasePort;
	}

	public String getSybaseUser() {
		return sybaseUser;
	}

	public String getSybasePass() {
		return sybasePass;
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

	public String getPeriodicity() {
		return periodicity;
	}
}
