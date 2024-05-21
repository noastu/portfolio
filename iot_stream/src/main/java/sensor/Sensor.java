package sensor;

import java.time.LocalDateTime;

/**
 * Represents a sensor reading with various environmental parameters.
 */
public class Sensor {
    private String sensor_id;
    private double longitude;
    private double latitude;
    private LocalDateTime timestamp;
    private double temperature;
    private double humidity;
    private double air_quality;
    private double noise_level;

    /**
     * Constructor to create a Sensor object with the specified parameters.
     *
     * @param sensor_id   Unique identifier for the sensor.
     * @param longitude   Longitude coordinate of the sensor location.
     * @param latitude    Latitude coordinate of the sensor location.
     * @param timestamp   Date and time when the sensor reading was recorded.
     * @param temperature Temperature recorded by the sensor.
     * @param humidity    Humidity level recorded by the sensor.
     * @param air_quality Air quality level recorded by the sensor.
     * @param noise_level Noise level recorded by the sensor.
     */
    public Sensor(String sensor_id, double longitude, double latitude, LocalDateTime timestamp,
                  double temperature, double humidity, double air_quality, double noise_level) {
        this.sensor_id = sensor_id;
        this.longitude = longitude;
        this.latitude = latitude;
        this.timestamp = timestamp;
        this.temperature = temperature;
        this.humidity = humidity;
        this.air_quality = air_quality;
        this.noise_level = noise_level;
    }

    // Getter and setter methods for Sensor class attributes

    /**
     * Gets the unique identifier of the sensor.
     *
     * @return The sensor's unique identifier.
     */
    public String getSensor_id() {
        return sensor_id;
    }

    /**
     * Sets the unique identifier of the sensor.
     *
     * @param sensor_id The new unique identifier for the sensor.
     */
    public void setSensor_id(String sensor_id) {
        this.sensor_id = sensor_id;
    }

    /**
     * Gets the longitude coordinate of the sensor location.
     *
     * @return The longitude coordinate of the sensor.
     */
    public double getLongitude() {
        return longitude;
    }

    /**
     * Sets the longitude coordinate of the sensor location.
     *
     * @param longitude The new longitude coordinate of the sensor.
     */
    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    /**
     * Gets the latitude coordinate of the sensor location.
     *
     * @return The latitude coordinate of the sensor.
     */
    public double getLatitude() {
        return latitude;
    }

    /**
     * Sets the latitude coordinate of the sensor location.
     *
     * @param latitude The new latitude coordinate of the sensor.
     */
    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    /**
     * Gets the timestamp when the sensor reading was recorded.
     *
     * @return The timestamp of the sensor reading.
     */
    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    /**
     * Sets the timestamp when the sensor reading was recorded.
     *
     * @param timestamp The new timestamp for the sensor reading.
     */
    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }

    /**
     * Gets the temperature recorded by the sensor.
     *
     * @return The temperature recorded by the sensor.
     */
    public double getTemperature() {
        return temperature;
    }

    /**
     * Sets the temperature recorded by the sensor.
     *
     * @param temperature The new temperature recorded by the sensor.
     */
    public void setTemperature(double temperature) {
        this.temperature = temperature;
    }

    /**
     * Gets the humidity level recorded by the sensor.
     *
     * @return The humidity level recorded by the sensor.
     */
    public double getHumidity() {
        return humidity;
    }

    /**
     * Sets the humidity level recorded by the sensor.
     *
     * @param humidity The new humidity level recorded by the sensor.
     */
    public void setHumidity(double humidity) {
        this.humidity = humidity;
    }

    /**
     * Gets the air quality level recorded by the sensor.
     *
     * @return The air quality level recorded by the sensor (cast to integer).
     */
    public int getAir_quality() {
        return (int) air_quality;
    }

    /**
     * Sets the air quality level recorded by the sensor.
     *
     * @param air_quality The new air quality level recorded by the sensor.
     */
    public void setAir_quality(double air_quality) {
        this.air_quality = air_quality;
    }

    /**
     * Gets the noise level recorded by the sensor.
     *
     * @return The noise level recorded by the sensor (cast to integer).
     */
    public int getNoise_level() {
        return (int) noise_level;
    }

    /**
     * Sets the noise level recorded by the sensor.
     *
     * @param noise_level The new noise level recorded by the sensor.
     */
    public void setNoise_level(double noise_level) {
        this.noise_level = noise_level;
    }
}
