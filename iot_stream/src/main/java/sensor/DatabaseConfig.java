package sensor;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

/**
 * Configuration class for setting up database connection using Spring Framework.
 */
@Configuration
@PropertySource("classpath:application.properties")
public class DatabaseConfig {
    // Database driver class name configured in application.properties
    @Value("${spring.datasource.driverClassName}")
    private String driverName;

    // Database URL configured in application.properties
    @Value("${spring.datasource.url}")
    private String url;

    // Database username configured in application.properties
    @Value("${spring.datasource.username}")
    private String username;

    // Database password configured in application.properties
    @Value("${spring.datasource.password}")
    private String password;

    /**
     * Default constructor for DatabaseConfig class.
     */
    public DatabaseConfig() {
    }

    /**
     * Configures and returns a DataSource bean using the values from application.properties.
     *
     * @return DataSource bean configured with the database connection details.
     */
    @Bean
    public DataSource getDataSource() {
        // Creating a new DriverManagerDataSource object
        final DriverManagerDataSource dataSource = new DriverManagerDataSource();

        // Setting the driver class name, URL, username, and password for the data source
        dataSource.setDriverClassName(driverName);
        dataSource.setUrl(url);
        dataSource.setUsername(username);
        dataSource.setPassword(password);

        // Returning the configured data source bean
        return dataSource;
    }
}
