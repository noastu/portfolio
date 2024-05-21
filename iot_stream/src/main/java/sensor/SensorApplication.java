// Main Java class for the Sensor application.
package sensor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * The main entry point of the Sensor application.
 */
@SpringBootApplication
public class SensorApplication {

    /**
     * Main method to start the Sensor application.
     *
     * @param args Command line arguments provided to the application.
     */
    public static void main(String[] args) {
        // Create a new SpringApplication instance for the SensorApplication class
        SpringApplication app = new SpringApplication(SensorApplication.class);
        // Run the application with the provided command line arguments
        app.run(args);
    }
}
