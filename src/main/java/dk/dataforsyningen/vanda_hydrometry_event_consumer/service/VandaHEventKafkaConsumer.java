package dk.dataforsyningen.vanda_hydrometry_event_consumer.service;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import dk.dataforsyningen.vanda_hydrometry_event_consumer.VandaHUtility;
import dk.dataforsyningen.vanda_hydrometry_event_consumer.model.EventModel;

@Service
public class VandaHEventKafkaConsumer {

	@Autowired
	DatabaseService dbService;
	
	private static final Logger log = LoggerFactory.getLogger(VandaHEventKafkaConsumer.class);
	
	private static int eventCounter = 0;
	private static int eventCounterTotal = 0;
	private static long counterReportTimer = 0;
	
	private int reportPeriodSec = 30; 
	
	@Autowired
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

	@KafkaListener(id="DMPEventHub", topics = "${spring.kafka.topic}", groupId = "${spring.kafka.consumer.group-id}", autoStartup = "false")
    public void consume(ConsumerRecord<String, String> record
    		/*, Acknowledgment acknowledgment*/ 
    		) {
		try {
			// Process each message consumed from Azure Event Hub
			EventModel event = EventModel.fromJson(record.value());
			event.setPartition(record.partition());
			event.setOffset(record.offset());
			event.setRecordDateTime(VandaHUtility.timestampToOffsetDateTimeUtc(record.timestamp()));
			
			System.out.printf("Consumed message -> Key: %s, Value: %s [ts=%s]%n", record.key(), event,record.timestamp());
	        
	        /*System.out.printf("Consumed message -> Key: %s, Value: %s [offset=%s; partition=%s; ts=%s]%n", 
	        		record.key(), 
	        		record.value(),,
	        		record.offset(),
	        		record.partition(),
	        		record.timestamp());*/
			
	        
	        // Acknowledge the message to commit the offset
	        /*acknowledgment.acknowledge();*/
		} catch (Exception e) {
            // Handle any errors during processing
            log.error("Error processing message: {}", e.getMessage(), e);
        }
    }
	
	// Start the listener programmatically
    public void startListener() {
        MessageListenerContainer listenerContainer = kafkaListenerEndpointRegistry.getListenerContainer("DMPEventHub");
        if (listenerContainer != null && !listenerContainer.isRunning()) {
            listenerContainer.start();  // Start the listener
            System.out.println("Kafka Listener started...");
        }
    }

    // Stop the listener programmatically
    public void stopListener() {
        MessageListenerContainer listenerContainer = kafkaListenerEndpointRegistry.getListenerContainer("DMPEventHub");
        if (listenerContainer != null && listenerContainer.isRunning()) {
            listenerContainer.stop();  // Stop the listener
            System.out.println("Kafka Listener stopped.");
        }
    }
	
}
