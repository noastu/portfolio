package com.sensor;

import javax.sql.DataSource;
import org.postgresql.ds.PGSimpleDataSource;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Random;
import java.util.Properties;
import java.time.ZoneId;

public class SensorQuery {
    private static PGSimpleDataSource createDataSource() throws SQLException {
        PGSimpleDataSource dataSource = null;
        try {
            String configFilePath = "src\\main\\resources\\application.properties";
            FileInputStream in = new FileInputStream(configFilePath);
            Properties prop = new Properties();
            prop.load(in);
            in.close();
            String url = prop.getProperty("url");
            String username = prop.getProperty("username");
            String password = prop.getProperty("password");
            dataSource = new PGSimpleDataSource();
            dataSource.setUrl(url);
            dataSource.setUser(username);
            dataSource.setPassword(password);
            return dataSource;
        }
        catch (IOException e){
            e.getMessage();
        }
        return dataSource;
    }

    public ArrayList<Sensor> queryData(Integer count) {
        ArrayList<Sensor> sensors = null;
        try {
            // Use the method we defined earlier to create a datasource
            DataSource dataSource = createDataSource();
            // get a connection from the datasource
            Connection conn = dataSource.getConnection();
            // Create a new statement on the connection
            String query = """
                    SELECT address_key as sensor_id,
                    ST_X(ST_Transform (geom, 4326)) as longitude,
                    ST_Y(ST_Transform (geom, 4326)) as latitude
                    FROM public."LBRS_Address_Points"
                   TABLESAMPLE SYSTEM(1.5)
                   limit ?
                   """;
            PreparedStatement stmt = conn.prepareStatement(query);
            stmt.setInt(1, count);

            // Execute the query, and store the results in the ResultSet instance
            ResultSet rs = stmt.executeQuery();

            //list to store results
            sensors = new ArrayList<Sensor>();

            // We run a loop to process the results.
            while (rs.next()) {
                // Now that `rs` points to a valid row (rs.next() is true), we can use the `getString`
                // and `getLong` methods to return each column value of the row as a string and long
                // respectively, and print it to the console
                LocalDateTime timeStamp = LocalDateTime.now(ZoneId.of("America/New_York"));
                Random rand = new Random();
                Sensor sensor = new Sensor(rs.getString("sensor_id"),
                        rs.getDouble("longitude"),
                        rs.getDouble("latitude"),
                        timeStamp,
                        rand.nextDouble(30 - 16) + 16,
                        rand.nextDouble(0.6 - 0.05) + 0.05,
                        rand.nextDouble(55 - 0.01) + 0.01,
                        rand.nextDouble(110 - 0.01) + 0.01
                );
                sensors.add(sensor);
            }

        } catch (SQLException e) {
            e.getMessage();
        }
        return sensors;
    }
}
