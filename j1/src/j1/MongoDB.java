package j1;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.List;

import org.bson.Document;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.mongodb.BasicDBObject;
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
		cursor = collection.find().iterator();
		System.out.println( collection.count() + " documents to migrate/delete.");
	}

	@SuppressWarnings("finally")
	public String prepareDataToSybase() throws ParseException {
		try {
			
			dataReadyforSybase = "";
			String date = "";
			String temperature = "";
			String humidity = "";
			
			while (cursor.hasNext()) {

				String next = cursor.next().toJson();

				JSONParser parser = new JSONParser();
				JSONObject json = (JSONObject) parser.parse(next);
				
				humidity = (String) json.get("humidity");
				temperature = (String) json.get("temperature");
				date = (String) json.get("date");

				String timstamp = dateToTimeStamp(date);
				
				dataReadyforSybase += "('" + timstamp + "', '" + temperature + "', '" + humidity + "'),";
			}
						
		} finally {
			
			if(!dataReadyforSybase.equals("")) 
				dataReadyforSybase = dataReadyforSybase.substring(0, dataReadyforSybase.length() - 1);
			
			return dataReadyforSybase;
			
		}

	}
	
	private String dateToTimeStamp(String date) throws java.text.ParseException {
		try {
			
			String OLD_FORMAT = "dd/MM/yyyy HH:mm:ss";
			String NEW_FORMAT = "yyyy-MM-dd HH:mm:ss";

			String oldDateString = date;
			String newDateString;

			SimpleDateFormat sdf = new SimpleDateFormat(OLD_FORMAT);
			
			java.util.Date utilStartDate = sdf.parse(oldDateString);
			java.sql.Date sqlStartDate = new java.sql.Date(utilStartDate.getTime());
			
			sdf.applyPattern(NEW_FORMAT);
			newDateString = sdf.format(sqlStartDate);
			
			Timestamp ts = new Timestamp(((java.util.Date)sdf.parse(newDateString)).getTime());
			
			return ts.toString();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return "";
		
	}

	public void deleteAllInCollection() {
		
		getDataFromMongoDb();
		
		while (cursor.hasNext()) {
			collection.deleteOne(cursor.next());
		}
		
		cursor.close();
		
	}
}
