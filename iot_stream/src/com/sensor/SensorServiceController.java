package com.sensor;

import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
public class SensorServiceController {

    @RequestMapping(path="api/sensors", produces="application/json")
    public ResponseEntity<Object> getSensor(@RequestParam(defaultValue = "10000") Integer count) {

        SensorQuery sensors = new SensorQuery();
        List<Sensor> sensorList = sensors.queryData(count);
        return ResponseHandler.generateResponse(String.format("Successfully retrieved data! Total records: %d", sensorList.size()),
                HttpStatus.OK, sensorList);
    }
}