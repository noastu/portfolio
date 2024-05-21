package sensor;

import javax.sql.DataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Random;
import java.time.ZoneId;

/**
 * Class for querying sensor data from the database and generating Sensor objects.
 */
public class SensorQuery {

    @Autowired
    DatabaseConfig databaseConfig;

    /**
     * Default constructor for SensorQuery class.
     */
    SensorQuery() {
    }

    /**
     * Queries data from the database and returns a list of Sensor objects.
     *
     * @param count Number of sensor records to query.
     * @return ArrayList of Sensor objects containing queried data.
     */
    public ArrayList<Sensor> queryData(Integer count) {
        ArrayList<Sensor> sensors = null;
        try {
            // Create a new Spring application context and retrieve the DataSource bean
            AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
            context.scan("sensor");
            context.refresh();
            DatabaseConfig databaseConfig = context.getBean(DatabaseConfig.class);
            DataSource dataSource = databaseConfig.getDataSource();
            context.close();

            // Get a connection from the datasource
            Connection conn = dataSource.getConnection();

            // Create a SQL query to fetch sensor data
            String query = """
                    SELECT address_key as sensor_id,
                    ST_X(ST_Transform (geom, 4326)) as longitude,
                    ST_Y(ST_Transform (geom, 4326)) as latitude
                    FROM public."LBRS_Address_Points"
                    TABLESAMPLE SYSTEM(1.5)
                    limit ?
                    """;

            // Prepare the SQL statement with the provided count parameter
            PreparedStatement stmt = conn.prepareStatement(query);
            stmt.setInt(1, count);

            // Execute the query, and store the results in the ResultSet instance
            ResultSet rs = stmt.executeQuery();

            // List to store queried sensor objects
            sensors = new ArrayList<>();

            // Loop through the result set and create Sensor objects with random environmental data
            while (rs.next()) {
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
            // Handle SQL exception
            e.getMessage();
        }
        return sensors;
    }
}
