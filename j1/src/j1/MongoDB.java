package j1;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;

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
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

public class MongoDB {

	private MongoCollection<Document> collection;
	private Log logFile;
	MongoCursor<Document> cursor;
	private String dataReadyforSybase = "";
	
	
	public MongoDB(Conf confFile, Log logFile) throws IOException {

		this.logFile = logFile;

		// set mongo
		MongoClientURI connectionString = new MongoClientURI(
				"mongodb://" + confFile.getMongoIp() + ":" + confFile.getMongoPort());
		MongoClient mongoClient = new MongoClient(connectionString);
		logFile.log("Connected to mongoDb", TypeLog.NORMAL);

		MongoDatabase database = mongoClient.getDatabase("sid_2018");
		logFile.log("Connected to database 'sid_2018'", TypeLog.NORMAL);

		collection = database.getCollection("sensor_messages");
		logFile.log("Connected to collection 'sensor_messages'", TypeLog.NORMAL);

	}

	// send new message to mongo
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

	public void getDataFromMongoDb() {

		// TODO - adicionar query para retirar apenas os de data inferior no find()
		
		cursor = collection.find().iterator();
	}

	@SuppressWarnings("finally")
	public String prepareDataToSybase() throws ParseException {
		try {
			
			dataReadyforSybase = "";
			
			while (cursor.hasNext()) {

				String next = cursor.next().toJson();

				JSONParser parser = new JSONParser();
				JSONObject json = (JSONObject) parser.parse(next);

				String date = "";
				String time = "";
				String temperature = "";
				String humidity = "";

				humidity = (String) json.get("humidity");
				temperature = (String) json.get("temperature");
				time = (String) json.get("time");
				date = (String) json.get("date");

				//timestamp simulado para insercção no sybase, fazer o mesmo com a data e hora do mongodb
				String input = "2007-11-11 12:13:14" ;
				java.sql.Timestamp ts = java.sql.Timestamp.valueOf( input ) ;
				
				dataReadyforSybase += "('" + ts + "', '" + temperature + "', '" + humidity + "'),";
								
			}
		} finally {
			cursor.close();
			
			dataReadyforSybase = dataReadyforSybase.substring(0, dataReadyforSybase.length() - 1);
			return dataReadyforSybase;
		}

	}
}
