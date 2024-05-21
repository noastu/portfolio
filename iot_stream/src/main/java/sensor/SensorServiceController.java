package sensor;

import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;

/**
 * Controller class for handling sensor data API requests.
 */
@RestController
@Validated // Indicates that this controller supports validation on its handler methods.
public class SensorServiceController {

    /**
     * Handles GET requests to retrieve sensor data.
     *
     * @param count Number of sensor records to retrieve (default value: 10,000, validated between 1 and 10,000).
     * @return ResponseEntity containing the response message, HTTP status, and sensor data list.
     */
    @RequestMapping(path = "api/sensors", produces = "application/json")
    public ResponseEntity<Object> getSensor(
            @Min(value = 1, message = "Count must be at least 1") 
            @Max(value = 10000, message = "Count must not exceed 10,000") 
            @RequestParam(required = false, defaultValue = "10000") Integer count) {

        // Create a SensorQuery object to query data
        SensorQuery sensorQuery = new SensorQuery();
        
        // Query sensor data based on the specified count
        List<Sensor> sensorList = sensorQuery.queryData(count);

        // Generate a response message and HTTP status, and wrap it with sensor data in ResponseEntity
        return ResponseHandler.generateResponse(
                String.format("Successfully retrieved data! Total records: %d", sensorList.size()),
                HttpStatus.OK, 
                sensorList);
    }
}
