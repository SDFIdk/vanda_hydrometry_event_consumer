package dk.dataforsyningen.vanda_hydrometry_event_consumer;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Field;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.MockedStatic;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;

import dk.dataforsyningen.vanda_hydrometry_event_consumer.config.VandaHEventConsumerConfig;
import dk.dataforsyningen.vanda_hydrometry_event_consumer.service.VandaHEventProcessor;

@SpringBootTest
public class VandaHEventConsumerRunnerTest {

	
	@MockBean
	VandaHEventProcessor eventProcessor;
	
	@Autowired
	VandaHEventConsumerConfig config;
	
	@Autowired
	@InjectMocks
	private VandaHEventConsumerRunner runner;
	
	@BeforeEach 
	public void setu() {
		config.setCommand(null);
	}
	
	@Test
	public void testNoArgs() throws Exception {
		
		String[] args = new String[0];
		
		final PrintStream oldStdout = System.out;
		ByteArrayOutputStream bo = new ByteArrayOutputStream();
        System.setOut(new PrintStream(bo));
		
		runner.run(args);
		
		bo.flush();
		System.setOut(oldStdout);
        String allWrittenLines = new String(bo.toByteArray()); 
        assertTrue(allWrittenLines.contains("Vanda Hydrometry Event Consumer"));
	}
	
	@Test
	public void testStart() throws Exception {
		
		String[] args = new String[1];
		args[0] = "start";
		
		runner.run(args);
		verify(eventProcessor, times(1)).startListener();
	}
	
}
