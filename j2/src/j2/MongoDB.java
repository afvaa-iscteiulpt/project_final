package j2;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

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

		logFile.log("Parsing new message.", TypeLog.NORMAL);
		try {
			JSONParser parser = new JSONParser();
			JSONObject json = (JSONObject) parser.parse(message.toString());

			humidity = (String) json.get("humidity");
			temperature = (String) json.get("temperature");
			time = (String) json.get("time");
			date = (String) json.get("date");
			
			String dateAndTime = date + " " + time;
			DateFormat dffrom = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
			DateFormat dfto = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");  
			Date dateTimeStampFormat = dffrom.parse(dateAndTime);
			timestamp = dfto.format(dateTimeStampFormat);

			checkEmptys(date, time, temperature, humidity);
			
		} catch (Exception e) {
			e.printStackTrace();
		}

		Document doc = new Document("date", timestamp).append("temperature", temperature)
				.append("humidity", humidity);

		logFile.log("Sending new message to mongoDb.", TypeLog.NORMAL);
		collection.insertOne(doc);
		logFile.log("Message sent to mongoDb.", TypeLog.NORMAL);

	}

	private void checkEmptys(String date, String time, String temperature, String humidity) {
		//TODO - check if date is empty, if yes insert date
		//if all empty dont insert
	}
}
