package th.mfu.popularity;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Reacts to the SAME borrow events as the notification service - but does
 * something completely different with them.
 * <p>
 * The producer sent each event ONCE. Both services receive it, because they
 * subscribe with two different group ids. That is publish/subscribe: one
 * announcement, any number of listeners, and the producer never knew.
 */
@Component
public class BorrowPlacedListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(BorrowPlacedListener.class);

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    private BookPopularityRepository popularityRepository;

    // TODO: (step 3) Do what you did in step 2, on your own this time.
    //
    //       1. Subscribe this method to the topic with @KafkaListener.
    //          Same topic - but the group id must be "popularity-group".
    //          A DIFFERENT group id is what makes Kafka give this service its
    //          own copy of every event. Give it the notification service's
    //          group id instead, and the two services would SHARE the stream:
    //          each event would go to only one of them.
    //
    //       2. Read bookTitle out of the event with objectMapper.
    //
    //       3. Count it:
    //
    //            BookPopularity entry = popularityRepository.findByBookTitle(bookTitle);
    //            if (entry == null) {
    //                entry = new BookPopularity(bookTitle, 0);
    //            }
    //            entry.setBorrowCount(entry.getBorrowCount() + 1);
    //            popularityRepository.save(entry);
    //
    //       Then watch http://localhost:8202/ while you post borrows.
    @KafkaListener(topics = "${app.kafka.topic:borrows}", groupId = "popularity-group")
    public void onBorrowPlaced(ConsumerRecord<String, String> record) throws Exception {
        LOGGER.info("received from topic {}: {}", record.topic(), record.value());

        JsonNode event = objectMapper.readTree(record.value());
        String memberName = event.get("memberName").asText();
        String bookTitle = event.get("bookTitle").asText();

        BookPopularity entry = popularityRepository.findByBookTitle(bookTitle);
        if (entry == null) {
            entry = new BookPopularity(bookTitle, 0);
        }
        entry.setBorrowCount(entry.getBorrowCount() + 1);
        popularityRepository.save(entry);
    }
}
