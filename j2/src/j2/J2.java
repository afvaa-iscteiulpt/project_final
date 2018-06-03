package j2;

import java.io.IOException;

//Importações para Servidor
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

//PROGRAMA QUE RECEBE VALORES DO SENSOR E METE NO MONGO
public class J2 {

	public static void startProgram(Conf confFile, Log logFile) throws IOException {

		logFile.log("Program started.", TypeLog.NORMAL);

		String broker = "wss://iot.eclipse.org:443";
		String clientId = MqttClient.generateClientId() + "sid18";
		String topic = confFile.getSensorTopic();
		MemoryPersistence persistence = new MemoryPersistence();

		MongoDB mongo = new MongoDB(confFile, logFile);

		try {
			MqttClient client = new MqttClient(broker, clientId, persistence);
			MqttConnectOptions connOpts = new MqttConnectOptions();
			connOpts.setCleanSession(true);

			// connect to broker
			logFile.log("Connecting to broker: " + broker + " with clientID: " + clientId, TypeLog.NORMAL);
			client.connect(connOpts);
			logFile.log("Connected to broker.", TypeLog.NORMAL);

			// subscribe topic
			logFile.log("Subscribing topic: " + topic, TypeLog.NORMAL);
			client.subscribe(topic);
			logFile.log("Topic subscribed.", TypeLog.NORMAL);

			// receive messages
			MqttCallback callback = new MqttCallback() {

				@Override
				public void messageArrived(String arg0, MqttMessage message) throws Exception {

					logFile.log("New message received at: " + logFile.getTimeStamp() + ".Sending to MongoDB.",
							TypeLog.NORMAL);
					System.out.println("New message received at: " + logFile.getTimeStamp() + ".Sending to MongoDB.");
					mongo.sendNewMessage(message);

				}

				@Override
				public void connectionLost(Throwable arg0) {
					try {
						logFile.log("Connection lost at: " + logFile.getTimeStamp(), TypeLog.NORMAL);
					} catch (IOException e) {
						e.printStackTrace();
					}

				}

				@Override
				public void deliveryComplete(IMqttDeliveryToken arg0) {
					// TODO Auto-generated method stub

				}
			};

			client.setCallback(callback);
			logFile.log("Listening for new messages", TypeLog.NORMAL);
			System.out.println("Listening for new messages");
			
		} catch (MqttException me) {
			logFile.log("Connecting. Reason: " + me.getReasonCode(), TypeLog.ERROR);
			me.printStackTrace();
		}

		logFile.log("Program finished.", TypeLog.END);
	}

}
