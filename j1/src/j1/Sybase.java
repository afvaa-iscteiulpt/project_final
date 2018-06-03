package j1;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
//Importa��es para Sybase
//import java.sql.*;
import sybase.jdbc4.sqlanywhere.*;


public class Sybase {

	private Connection con;
	private Log logFile;
	
	public Sybase(Conf confFile, Log logFile) throws IOException {

		this.logFile = logFile;
		
		String ip = confFile.getSybaseIp();
		String user = confFile.getSybaseUser();
		String pass = confFile.getSybasePass();
		String port = confFile.getSybasePort();
		String servername = confFile.getSybaseServerName();
		String databasename = confFile.getSybaseDatabaseName();

		try {
			
			con = DriverManager.getConnection("jdbc:sqlanywhere:UserID=" + user + ";Password=" + pass + ";Host=" + ip + ":" + port + ";ServerName=" + servername + ";DatabaseName=" + databasename);
			
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	public void insertToSybase(String sqlString) throws SQLException, IOException {
		
		try
	    {
		String sqlQuery = "INSERT INTO HumidadeTemperatura"
				+ " (dataHoraMedicao, valorMedicaoTemperatura, valorMedicaoHumidade)" + " VALUES " + sqlString;

		System.out.println(sqlQuery);
		
		// Create a statement object, the container for the SQL
		// statement. May throw a SQLException.
		Statement stmt = con.createStatement();
		stmt.executeQuery(sqlQuery);
		stmt.close();
		}
        catch (SQLException sqe)
        {
        	logFile.log("Exece��o de SQL inesperada: " +
                    sqe.toString() + ", sqlstate = " +
                    sqe.getSQLState(), TypeLog.ERROR);
        	System.out.println("Exece��o de SQL inesperada: " +
                                sqe.toString() + ", sqlstate = " +
                                sqe.getSQLState());
            System.exit(1);
        }
	}
	
}
