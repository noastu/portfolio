package com.sensor;

import java.time.LocalDateTime;

public class Sensor {
    String sensor_id;
    double longitude;
    double latitude;
    LocalDateTime timestamp;
    double temperature;
    double humidity;
    double air_quality;
    double noise_level;
    public Sensor(String sens, double lon, double lat, LocalDateTime ts, double temp,
                     double hum, double aq, double nl) {
        this.sensor_id = sens;
        this.longitude = lon;
        this.latitude = lat;
        this.timestamp = ts;
        this.temperature = temp;
        this.humidity = hum;
        this.air_quality = aq;
        this.noise_level = nl;
    }
    public String getSensor_id() {
        return sensor_id;
    }
    public void setSensor_id(String sensor_id) {
        this.sensor_id = sensor_id;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }

    public double getTemperature() {
        return temperature;
    }

    public void setTemperature(double temperature) {
        this.temperature = temperature;
    }

    public double getHumidity() {
        return humidity;
    }

    public void setHumidity(double humidity) {
        this.humidity = humidity;
    }

    public double getAir_quality() {
        return (int)air_quality;
    }

    public void setAir_quality(double air_quality) {
        this.air_quality = air_quality;
    }

    public double getNoise_level() {
        return (int)noise_level;
    }

    public void setNoise_level(double noise_level) {
        this.noise_level = noise_level;
    }
}
