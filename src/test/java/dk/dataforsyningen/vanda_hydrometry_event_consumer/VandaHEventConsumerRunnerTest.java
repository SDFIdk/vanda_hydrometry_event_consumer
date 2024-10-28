package dk.dataforsyningen.vanda_hydrometry_event_consumer;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.MockedStatic;
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
		
		try (MockedStatic<VandaHUtility> mockedStatic = mockStatic(VandaHUtility.class)) {
			runner.run(args);
			mockedStatic.verify(() -> VandaHUtility.logAndPrint(eq(null), eq(null), eq(true), startsWith("Vanda Hydrometry Event Consumer")), times(1));
		}
	}
	
	@Test
	public void testStart() throws Exception {
		
		String[] args = new String[1];
		args[0] = "start";
		
		runner.run(args);
		verify(eventProcessor, times(1)).startListener();
	}
	
}
