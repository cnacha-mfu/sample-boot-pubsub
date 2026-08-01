package th.mfu.notification;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Reacts to borrow events: writes a notification for the member.
 * <p>
 * Nobody calls this class. The broker delivers every event on the topic to the
 * method below, because of the annotation you are about to write.
 */
@Component
public class BorrowPlacedListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(BorrowPlacedListener.class);

    /** Jackson - the same JSON library the whole course has used. */
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    private NotificationRepository notificationRepository;

    // TODO: (step 2) Subscribe this method to the topic. Put this annotation on
    //       onBorrowPlaced:
    //
    @KafkaListener(topics = "${app.kafka.topic:borrows}", groupId = "notification-group")
    public void onBorrowPlaced(ConsumerRecord<String, String> record) throws Exception {
        LOGGER.info("received from topic {}: {}", record.topic(), record.value());

        // TODO: (step 2) Turn the event back into data, and store a notification:
        //
        JsonNode event = objectMapper.readTree(record.value());
        String memberName = event.get("memberName").asText();
        String bookTitle = event.get("bookTitle").asText();
    
        Notification notification = new Notification(memberName, bookTitle,
                "Dear " + memberName + ", you borrowed " + bookTitle);
        notificationRepository.save(notification);
        //
        // Then watch http://localhost:8201/ while you post borrows.
    }
}
