package th.mfu.borrow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Records a borrow - by ANNOUNCING it.
 * <p>
 * Compare this with the microservice sample. There, recording a borrow CALLED
 * book-service and waited for the answer:
 *
 * <pre>
 *   BookDTO book = bookClient.getBook(dto.getBookId());   // call, and wait
 * </pre>
 *
 * There is no call here and no waiting. This service publishes an EVENT - "a
 * borrow happened" - to a topic on the broker, and moves on. It does not know
 * who reads the event. It does not care. Whoever is interested subscribes.
 */
@RestController
@RequestMapping("/borrows")
public class BorrowController {

    private static final Logger LOGGER = LoggerFactory.getLogger(BorrowController.class);

    /**
     * The one Kafka class a producer needs. send(topic, message) hands the
     * message to the broker.
     */
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    /** The name of the channel. Every service in this system agrees on it. */
    @Value("${app.kafka.topic:borrows}")
    private String topicName;

    /**
     * POST /borrows with a body like:
     *
     * <pre>
     *   { "bookTitle": "1984", "memberName": "Alice Johnson" }
     * </pre>
     */
    @PostMapping
    public ResponseEntity<String> placeBorrow(@RequestBody String borrowJson) {

        // TODO: (step 1) Publish the event:
        //
        kafkaTemplate.send(topicName, borrowJson);
        LOGGER.info("published to {}: {}", topicName, borrowJson);
        //
        // One line does the work. Note what the line does NOT do: it does not
        // wait for anybody to read the event, and it cannot know who will.
        //
        // Then answer 201, because the borrow has been accepted.

        return new ResponseEntity<>("Borrow placed", HttpStatus.CREATED);
    }
}
