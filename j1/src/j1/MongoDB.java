package j1;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;

import org.bson.Document;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
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

		logFile.log("Parsing new message.", TypeLog.NORMAL);
		try {
			JSONParser parser = new JSONParser();
			JSONObject json = (JSONObject) parser.parse(message.toString());

			humidity = (String) json.get("humidity");
			temperature = (String) json.get("temperature");
			time = (String) json.get("time");
			date = (String) json.get("date");

		} catch (Exception e) {
			e.printStackTrace();
		}

		Document doc = new Document("date", date).append("time", time).append("temperature", temperature)
				.append("humidity", humidity);

		logFile.log("Sending new message to mongoDb.", TypeLog.NORMAL);
		collection.insertOne(doc);
		logFile.log("Message sent to mongoDb.", TypeLog.NORMAL);

	}
	
	private void getDataFromMongoDb() {
		FindIterable<Document> cursor = collection.find();
		while (((Iterator<DBObject>) cursor).hasNext()) {
		   DBObject obj = ((Iterator<DBObject>) cursor).next();
		   //do your thing
		   System.out.println(obj.toString());
		}
	}
}
