package sensor;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for generating standardized HTTP responses.
 */
public class ResponseHandler {

    /**
     * Generates a ResponseEntity object with the specified message, HTTP status, and response object.
     *
     * @param message     A string message describing the response.
     * @param status      HTTP status code to be included in the response.
     * @param responseObj The object to be included in the response payload.
     * @return ResponseEntity object containing the response message, status, and response object.
     */
    public static ResponseEntity<Object> generateResponse(String message, HttpStatus status, Object responseObj) {
        // Create a map to hold the response data
        Map<String, Object> map = new HashMap<>();
        // Put the message, status code, and response object into the map
        map.put("message", message);
        map.put("status", status.value());
        map.put("records", responseObj);

        // Create and return a new ResponseEntity object with the map and specified status
        return new ResponseEntity<>(map, status);
    }
}
