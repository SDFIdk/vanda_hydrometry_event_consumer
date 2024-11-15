package dk.dataforsyningen.vanda_hydrometry_event_consumer.service;

import java.time.OffsetDateTime;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Service;

import dk.dataforsyningen.vanda_hydrometry_event_consumer.VandaHUtility;
import dk.dataforsyningen.vanda_hydrometry_event_consumer.config.VandaHEventConsumerConfig;
import dk.dataforsyningen.vanda_hydrometry_event_consumer.model.EventModel;

@Service
public class VandaHEventProcessor {
	
	public static final String EVENT_MEASUREMENT_ADDED = "MeasurementAdded";
	public static final String EVENT_MEASUREMENT_UPDATED = "MeasurementUpdated";
	public static final String EVENT_MEASUREMENT_DELETED = "MeasurementDeleted";
	
	@Autowired
	private DatabaseService dbService;
	
	private final Logger logger = LoggerFactory.getLogger(VandaHEventProcessor.class);
	
	private HashMap<Integer, Long> minOffset = new HashMap<>();
	private HashMap<Integer, Long> maxOffset = new HashMap<>();

	private long lastReportTimestamp = 0L;
	private long eventCounter = 0l;
	private long eventCounterAdd = 0l;
	private long eventCounterUpd = 0l;
	private long eventCounterDel = 0l;
	private long eventCounterTotal = 0l;
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
		
		String rawMessage = "Raw Message -> Key: " + record.key() + 
				", Value: " + record.value() + 
				" [offset=" + record.offset() + 
				"; partition=" + record.partition() + 
				"; ts=" + record.timestamp() + "]";
		
		if (config.isDisplayAll()) {
	        System.out.println(rawMessage);
		}
		
		if (config.isLoggingAllEvents()) {
			logger.debug(rawMessage);
		}
		
		try {
			// Process each message consumed from Azure Event Hub
			EventModel event = EventModel.fromJson(record.value());
			event.setPartition(record.partition());
			event.setOffset(record.offset());
			event.setRecordDateTime(VandaHUtility.timestampToOffsetDateTimeUtc(record.timestamp()));

			//skip undesired events
			if (acceptEvent(event)) {
				
				if (config.isLoggingProcessedEvents()) {
					logger.debug(rawMessage);
				}
				
				if (config.isDisplayRawData() && !config.isDisplayAll()) {
			        System.out.println(rawMessage);
				}
				
				if (config.isDisplayData()) {
					System.out.printf("Message -> Key: %s, Value: %s%n", record.key(), event);
				} 
				
				if (config.isSaveDb()) {
					
					if (EVENT_MEASUREMENT_ADDED.equals(event.getEventType())) {
						
						dbService.addMeasurementFromEvent(event);
						eventCounterAdd++;
						
					} else if (EVENT_MEASUREMENT_UPDATED.equals(event.getEventType())) {
						
						dbService.updateMeasurementFromEvent(event);
						eventCounterUpd++;
						
					} else if (EVENT_MEASUREMENT_DELETED.equals(event.getEventType())) {
												
						dbService.deleteMeasurementFromEvent(event);
						eventCounterDel++;
					}
				}
			}

			// Can be used to debugging and matching events from the logfile, offset is the page from event hub
			//calculate offset min/max
			if (!minOffset.containsKey(event.getPartition()) || event.getOffset() < minOffset.get(event.getPartition())) {
				minOffset.put(event.getPartition(), event.getOffset());
			}
			if (!maxOffset.containsKey(event.getPartition()) || event.getOffset() > maxOffset.get(event.getPartition())) {
				maxOffset.put(event.getPartition(), event.getOffset());
			}

			//calc min/max event record time. Is when the measurement event was created
			if (minRecordTime == null || event.getRecordDateTime().isBefore(minRecordTime)) {
				minRecordTime = event.getRecordDateTime();
			}
			if (maxRecordTime == null || event.getRecordDateTime().isAfter(maxRecordTime)) {
				maxRecordTime = event.getRecordDateTime();
			}
			
			//show report
			// event counter of received events within this report time frame
			eventCounter++;
			// event counter of all received events since the application was started
			eventCounterTotal++;
			long now = System.currentTimeMillis();
			if (config.getReportPeriodSec() > 0 && now > lastReportTimestamp + config.getReportPeriodSec() * 1000) {

				String msg =
						"Received " + eventCounter + "/" + eventCounterTotal +
						" events (a,u,d:" + eventCounterAdd + "," + eventCounterUpd + "," + eventCounterDel +
						") " +
						(lastReportTimestamp > 0 ? ("within " + (int)((now - lastReportTimestamp)/1000) + " sec") : "") +
						"\n";
				// reset the event counter within this report period
				eventCounter = 0;
				
				//display offset min/max
				for(int partition : minOffset.keySet()) {
					// find the oldest offset for this partition
					long minimumOffset = minOffset.get(partition);
					// find the newest offset for this partition
					long maximumOffset = maxOffset.get(partition);
					msg += "min/max for part " + partition + ": " + minimumOffset + "/" + maximumOffset + "; ";
				}
				msg += "event creation timestamp between " + minRecordTime + " and " + maxRecordTime;
				// remember the time when the report is shown
				lastReportTimestamp = now;

				logger.info(msg);
			}
	        
	        /*acknowledgment.acknowledge();*/
		} catch (Exception e) {
			logger.error("Error processing message: " + e.getMessage(), e);
        }
    }
	
	private boolean acceptEvent(EventModel event) {
		List<Integer> allowedExaminations = config.getExaminationTypeSc();
		boolean acceptExamination = (allowedExaminations.size() == 0 ||  
				allowedExaminations.contains(event.getExaminationTypeSc()));
		
		boolean acceptEventType = (EVENT_MEASUREMENT_ADDED.equals(event.getEventType()) && config.processAdditions()) ||
				(EVENT_MEASUREMENT_UPDATED.equals(event.getEventType()) && config.processUpdates()) ||
				(EVENT_MEASUREMENT_DELETED.equals(event.getEventType()) && config.processDeletions());
		
		return acceptExamination && acceptEventType;
	}
	
	// Start the listener programmatically
    public void startListener() {
        MessageListenerContainer listenerContainer = kafkaListenerEndpointRegistry.getListenerContainer("DMPEventHub");
        if (listenerContainer != null && !listenerContainer.isRunning()) {
            listenerContainer.start();  // Start the listener
            logger.info("Kafka Listener started...");
        }
    }
}
