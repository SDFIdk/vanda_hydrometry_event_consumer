package dk.dataforsyningen.vanda_hydrometry_event_consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import dk.dataforsyningen.vanda_hydrometry_event_consumer.config.VandaHEventConsumerConfig;
import dk.dataforsyningen.vanda_hydrometry_event_consumer.service.VandaHEventProcessor;

@Component
public class VandaHEventConsumerRunner implements CommandLineRunner {

	private final Logger logger = LoggerFactory.getLogger(VandaHEventConsumerRunner.class);
	
	@Autowired
	VandaHEventConsumerConfig config;
	
	@Autowired
	VandaHEventProcessor eventProcessor;
	
	@Override
	public void run(String... args) throws Exception {
		logger.info("Application start ...");
		
		String command = config.parseCommands(args);
		logger.info("Execute command: " + command + "\nwith config: " + config.toString());
		
		try {
		
			if ("start".equalsIgnoreCase(command)) {
				eventProcessor.startListener();
				
				//TODO: implement logic to stop if necessary
				//eventConsumer.stopListener();
			} else {
				System.out.println("Vanda Hydrometry Event Consumer\n=====================\nUsage parameters: start [--options[=value]]");
				System.out.println(VandaHUtility.BOLD_ON + "start" + VandaHUtility.FORMAT_OFF + " commands will start the event hub client that will receive and process events.\n");
				
				System.out.println("Use the option --displayRawData to display the received (and processed) raw json events to the console.");
				System.out.println("Use the option --displayAll to display all the received (not only processed) raw json events to the console.");
				System.out.println("Use the option --displayData to display the mapped data at the console (only processed events).");
				System.out.println("Use the option --verbose to display more info at the console.");
				System.out.println("Use the option --saveDb to save the results in the defined database.");
			}
			
		} catch (Exception ex) {
			logger.error("Error executing command '" + command + "'", ex);
			System.exit(1);
		}
		
		logger.info("Application ended.");
	}

}
