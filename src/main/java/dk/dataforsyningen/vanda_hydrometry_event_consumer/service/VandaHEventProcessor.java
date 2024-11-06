package dk.dataforsyningen.vanda_hydrometry_event_consumer.service;

import java.time.OffsetDateTime;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

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
public class VandaHEventProcessor {
	
	public static final String EVENT_MEASUREMENT_ADDED = "MeasurementAdded";
	public static final String EVENT_MEASUREMENT_UPDATED = "MeasurementUpdated";
	public static final String EVENT_MEASUREMENT_DELETED = "MeasurementDeleted";
	
	@Autowired
	private DatabaseService dbService;
	
	private final Logger log = LoggerFactory.getLogger(VandaHEventProcessor.class);
	
	private HashMap<Integer, Long> minOffset = new HashMap<>();
	private HashMap<Integer, Long> maxOffset = new HashMap<>();
	
	private long lastReportTS = 0L;
	private long eventCounter = 0l;
	private long eventCounterAdd = 0l;
	private long eventCounterUpd = 0l;
	private long eventCounterDel = 0l;
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
		
		String rawMessage = "Raw Message -> Key: " + record.key() + 
				", Value: " + record.value() + 
				" [offset=" + record.offset() + 
				"; partition=" + record.partition() + 
				"; ts=" + record.timestamp() + "]";
		
		if (config.isDisplayAll()) {
	        System.out.println(rawMessage);
		}
		
		if (config.isLoggingAllEvents()) {
			VandaHUtility.logAndPrint(log, config.getLoggingEventsLevel(), false, rawMessage); 
		}
		
		try {
			// Process each message consumed from Azure Event Hub
			EventModel event = EventModel.fromJson(record.value());
			event.setPartition(record.partition());
			event.setOffset(record.offset());
			event.setRecordDateTime(VandaHUtility.timestampToOffsetDateTimeUtc(record.timestamp()));

			//skip undesired events
			if (acceptEvent(event)) {
				
				if (config.isLoggingEvents() && !config.isLoggingAllEvents()) {
					VandaHUtility.logAndPrint(log, config.getLoggingEventsLevel(), false, rawMessage);
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
				VandaHUtility.logAndPrint(null, null, config.isVerbose(), 
						(new Date()) + ": Received " + eventCounter + "/" + eventCounterTotal + 
						" events (a,u,d:" + eventCounterAdd + "," + eventCounterUpd + "," + eventCounterDel + 
						") " + 
						(lastReportTS > 0 ? ("within " + (int)((now - lastReportTS)/1000) + " sec") : "")
						); 
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
				lastReportTS = now;
			}
	        
	        /*acknowledgment.acknowledge();*/
		} catch (Exception e) {
			VandaHUtility.logAndPrint(log, Level.ERROR, false, "Error processing message: " + e.getMessage(), e);
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
