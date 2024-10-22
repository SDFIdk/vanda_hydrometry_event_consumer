package dk.dataforsyningen.vanda_hydrometry_event_consumer.service;

import java.time.OffsetDateTime;
import java.util.Date;
import java.util.HashMap;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import dk.dataforsyningen.vanda_hydrometry_event_consumer.VandaHUtility;
import dk.dataforsyningen.vanda_hydrometry_event_consumer.config.VandaHEventConsumerConfig;
import dk.dataforsyningen.vanda_hydrometry_event_consumer.model.EventModel;

@Service
public class VandaHEventKafkaConsumer {

	@Autowired
	private DatabaseService dbService;
	
	private final Logger log = LoggerFactory.getLogger(VandaHEventKafkaConsumer.class);
	
	private HashMap<Integer, Long> minOffset = new HashMap<>();
	private HashMap<Integer, Long> maxOffset = new HashMap<>();
	
	private long eventCounter = 0l;
	private long eventCounterTotal = 0l;
	private long counterReportTimer = 0l;
	private OffsetDateTime minRecordTime = null;
	private OffsetDateTime maxRecordTime = null;
	
	@Autowired
	private VandaHEventConsumerConfig config;
	
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

			//skip undesired events
			if (config.getExaminationTypeSc().size() == 0 ||  
				config.getExaminationTypeSc().contains(event.getExaminationTypeSc()) 
				) {
				if (config.isDisplayRawData()) {
			        System.out.printf("Raw Message -> Key: %s, Value: %s [offset=%s; partition=%s; ts=%s]%n", 
			        		record.key(), 
			        		record.value(),
			        		record.offset(),
			        		record.partition(),
			        		record.timestamp());
				}
				
				if (config.isDisplayData()) {
					System.out.printf("Message -> Key: %s, Value: %s%n", record.key(), event);
				} 
				
				if (config.isSaveDb()) {
					//TODO: processing logic
				}
			}

			//calculate offset min/max
			if (!minOffset.containsKey(event.getPartition()) || event.getOffset() < minOffset.get(event.getPartition())) {
				minOffset.put(event.getPartition(), event.getOffset());
			}
			if (!maxOffset.containsKey(event.getPartition()) || event.getOffset() > maxOffset.get(event.getPartition())) {
				maxOffset.put(event.getPartition(), event.getOffset());
			}
			
			//calc min/max record time
			if (minRecordTime == null || event.getRecordDateTime().isBefore(minRecordTime)) {
				minRecordTime = event.getRecordDateTime();
			}
			if (maxRecordTime == null || event.getRecordDateTime().isAfter(maxRecordTime)) {
				maxRecordTime = event.getRecordDateTime();
			}
			
			//show report
			eventCounter++;
			eventCounterTotal++;
			long now = System.currentTimeMillis();
			if (config.getReportPeriodSec() > 0 && now >  counterReportTimer + config.getReportPeriodSec() * 1000) {
				counterReportTimer = now; 
				VandaHUtility.logAndPrint(null, null, config.isVerbose(), (new Date()) + ": Processed " + eventCounter + "/" + eventCounterTotal + " events within " + config.getReportPeriodSec() + " sec"); 
				eventCounter = 0;
				
				//display offset min/max
				String s = "";
				for(int p : minOffset.keySet()) {
					long mO = minOffset.get(p);
					long MO = maxOffset.containsKey(p) ? maxOffset.get(p) : 0;
					s += "min/max for part " + p + ": " + mO + "/" + MO + "; ";
				}
				s += "data between " + minRecordTime + " and " + maxRecordTime;
				VandaHUtility.logAndPrint(null, null, config.isVerbose(), s);
			}
	        
	        /*acknowledgment.acknowledge();*/
		} catch (Exception e) {
			VandaHUtility.logAndPrint(log, Level.ERROR, false, "Error processing message: " + e.getMessage(), e);
        }
    }
	
	// Start the listener programmatically
    public void startListener() {
        MessageListenerContainer listenerContainer = kafkaListenerEndpointRegistry.getListenerContainer("DMPEventHub");
        if (listenerContainer != null && !listenerContainer.isRunning()) {
            listenerContainer.start();  // Start the listener
            VandaHUtility.logAndPrint(log, Level.INFO, config.isVerbose(), "Kafka Listener started...");
        }
    }

    // Stop the listener programmatically
    public void stopListener() {
        MessageListenerContainer listenerContainer = kafkaListenerEndpointRegistry.getListenerContainer("DMPEventHub");
        if (listenerContainer != null && listenerContainer.isRunning()) {
            listenerContainer.stop();  // Stop the listener
            VandaHUtility.logAndPrint(log, Level.INFO, config.isVerbose(), "Kafka Listener stopped.");
        }
    }
	
}
