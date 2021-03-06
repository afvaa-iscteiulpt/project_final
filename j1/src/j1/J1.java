package j1;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.json.simple.parser.ParseException;

//PROGRAMA QUE SACA VALORES DO MONGO E ENVIA PARA O SYBASE
public class J1 {

	final static ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

	public static void startProgram(Conf confFile, Log logFile) throws IOException, ParseException, SQLException {

		logFile.log("Program started.", TypeLog.NORMAL);

		long periodicity = Long.parseLong(confFile.getPeriodicity());

		// create connections
		Sybase sybase = new Sybase(confFile, logFile);
		MongoDB mongo = new MongoDB(confFile, logFile);

		// cycle every "periodicity"
		executorService.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				try {
					startCicle(logFile, mongo, sybase);
				} catch (IOException | ParseException | SQLException e) {
					e.printStackTrace();
				}
			}
		}, 0, periodicity, TimeUnit.MINUTES);

	}

	public static void startCicle(Log logFile, MongoDB mongo, Sybase sybase)
			throws IOException, ParseException, SQLException {

		try {
			logFile.log("Cycle started. Get data from MongoDB", TypeLog.NORMAL);

			mongo.getDataFromMongoDb();
		
			String sqlString = mongo.prepareDataToSybase();

			if(!sqlString.equals("")) {
				// insert query to sybase
				sybase.insertToSybase(sqlString);
				logFile.log("Data inserted to Sybase.", TypeLog.NORMAL);

				mongo.deleteAllInCollection();
				logFile.log("Cycle ended. Documents in collection deleted.", TypeLog.NORMAL);
			} else {
				System.out.println("No data to migrate.");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		
	}

}
