package sensor;

import java.util.*;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.ConstraintViolationException;

// A global exception handler class to handle specific exceptions and provide custom error responses.
@ControllerAdvice
public class CustomExceptionHandler {

    // Exception handling method for ConstraintViolationException.
    @ExceptionHandler(ConstraintViolationException.class)
    @ResponseBody // Indicates that the return value should be written directly to the HTTP response body.
    @ResponseStatus(HttpStatus.BAD_REQUEST) // Sets the HTTP status code of the response to 400 (Bad Request).
    public Object handleRequestValidationException(Exception ex, HttpServletRequest request) {
        // Create a LinkedHashMap to structure the custom error response.
        Map<String, Object> responseBody = new LinkedHashMap<>();

        // Populate the error response with timestamp, status code, error message, and request path.
        responseBody.put("timestamp", new Date()); // Current timestamp of the error occurrence.
        responseBody.put("status", HttpStatus.BAD_REQUEST.value()); // HTTP status code (400 for Bad Request).
        responseBody.put("error", ex.getMessage()); // Error message from the caught exception.
        responseBody.put("path", request.getServletPath()); // Request path where the error occurred.

        // Return the custom error response.
        return responseBody;
    }
}
