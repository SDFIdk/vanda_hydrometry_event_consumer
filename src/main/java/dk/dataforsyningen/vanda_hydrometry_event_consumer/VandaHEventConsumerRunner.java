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

	private final Logger log = LoggerFactory.getLogger(VandaHEventConsumerRunner.class);
	
	@Autowired
	VandaHEventConsumerConfig config;
	
	@Autowired
	VandaHEventProcessor eventProcessor;
	
	@Override
	public void run(String... args) throws Exception {
		VandaHUtility.logAndPrint(log, Level.INFO, config.isVerbose(), "Application start ...");
		
		String command = config.parseCommands(args);
		VandaHUtility.logAndPrint(log, Level.DEBUG, false, "Execute command: " + command);
		VandaHUtility.logAndPrint(log, Level.DEBUG, false, "with config: " + config.toString());
		
		try {
		
			if ("start".equalsIgnoreCase(command)) {
				eventProcessor.startListener();
				
				//TODO: implement logic to stop if necessary
				//eventConsumer.stopListener();
			} else {
				VandaHUtility.logAndPrint(null, null, true, "Vanda Hydrometry Event Consumer\n=====================\nUsage parameters: start [--options[=value]]");
				VandaHUtility.logAndPrint(null, null, true, VandaHUtility.BOLD_ON + "start" + VandaHUtility.FORMAT_OFF + " commands will start the event hub client that will receive and process events.\n");
				
				VandaHUtility.logAndPrint(null, null, true, "Use the option --displayRawData to display the API results at the console.");
				VandaHUtility.logAndPrint(null, null, true, "Use the option --displayData to display the mapped data at the console.");
				VandaHUtility.logAndPrint(null, null, true, "Use the option --verbose to display more info at the console.");
				VandaHUtility.logAndPrint(null, null, true, "Use the option --saveDb to save the results in the defined database.");
			}
			
		} catch (Exception ex) {
			VandaHUtility.logAndPrint(log, Level.ERROR, false, "Error executing command '" + command + "'", ex);
			System.exit(1);
		}
		
		
		VandaHUtility.logAndPrint(log, Level.INFO, config.isVerbose(), "Application ended.");

	}

}
