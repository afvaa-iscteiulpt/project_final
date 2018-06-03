package j2;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import org.bson.Document;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class MongoDB {

	private MongoCollection<Document> collection;
	private Log logFile;

	public MongoDB(Conf confFile, Log logFile) throws IOException {

		this.logFile = logFile;

		// set mongo
		MongoClientURI connectionString = new MongoClientURI("mongodb://" + confFile.getMongoIp() + ":" + confFile.getMongoPort());
		MongoClient mongoClient = new MongoClient(connectionString);
		logFile.log("Connected to mongoDb", TypeLog.NORMAL);

		MongoDatabase database = mongoClient.getDatabase("sid_2018");
		logFile.log("Connected to database 'sid_2018'", TypeLog.NORMAL);

		collection = database.getCollection("sensor_messages");
		logFile.log("Connected to collection 'sensor_messages'", TypeLog.NORMAL);

	}

	//send new message to mongo
public void sendNewMessage(MqttMessage message) throws FileNotFoundException, IOException, ParseException {
		
		String date = "";
		String time = "";
		String temperature = "";
		String humidity = "";
		String timestamp = "";
		String checkDate = "";
		String checkTime = "";
		String checkTemperature = "";
		String checkHumidity = "";
		
		logFile.log("Parsing new message.", TypeLog.NORMAL);
		try {
			JSONParser parser = new JSONParser();
			JSONObject json = (JSONObject) parser.parse(message.toString());

			humidity = (String) json.get("humidity");
			temperature = (String) json.get("temperature");
			time = (String) json.get("time");
			date = (String) json.get("date");
			
			checkDate = dateCheck(date);
			checkTime = timeCheck(time);
			checkHumidity = humidityCheck(humidity);
			checkTemperature = temperatureCheck(temperature);
			
			timestamp = checkDate + " " + checkTime;
			
		} catch (Exception e) {
			e.printStackTrace();
		}

		Document doc = new Document("date", timestamp).append("temperature", checkTemperature)
				.append("humidity", checkHumidity);

		logFile.log("Sending new message to mongoDb.", TypeLog.NORMAL);
		collection.insertOne(doc);
		logFile.log("Message sent to mongoDb.", TypeLog.NORMAL);

	}

	private String temperatureCheck(String temperature) {
	
		if(temperature == null || temperature.isEmpty() || temperature.equals("null")) {
			return "null";
		}
		
		return temperature;
	}

	private String humidityCheck(String humidity) {
		
		if(humidity == null || humidity.isEmpty() || humidity.equals("null")) {
			return "null";
		}
		
		return humidity;
		
	}

	private String dateCheck(String date) {
		if(date == null || date.isEmpty() || date.equals("null"))	{
			TimeZone tz = TimeZone.getTimeZone("Europe/Lisbon");
			Date now = new Date();
			DateFormat df = new SimpleDateFormat ("dd/MM/yyyy");
			df.setTimeZone(tz);
			date = df.format(now);
			System.out.println("Date is invalid, adding today's date: " + date);
		}
		return date;
	}

	private String timeCheck(String time)	{
		if(time == null || time.isEmpty() || time.equals("null"))	{
			TimeZone tz = TimeZone.getTimeZone("Europe/Lisbon");
			Date now = new Date();
			DateFormat df = new SimpleDateFormat ("HH:mm:ss");
			df.setTimeZone(tz);
			time = df.format(now);
			System.out.println("Time is invalid, adding current time: " + time);
		}
		return time;
	}
}
