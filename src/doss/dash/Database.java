package doss.dash;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Connection;
import java.sql.DriverManager;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.compress.utils.Charsets;

abstract class Database implements Closeable {

	public static String DOSS_HOME = System.getProperty("doss.home");
    //Class.forName("org.h2.Driver");

	public static Connection open() throws SQLException {

        Path urlFile = Paths.get(DOSS_HOME + "/db/").resolve("jdbc-url");
        if (Files.exists(urlFile)) {
            try {
                return DriverManager.getConnection(Files.readAllLines(urlFile, Charsets.UTF_8).get(0));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
		} else {
			throw new RuntimeException("No jdbc-url at " + urlFile);
		}
    }



}




