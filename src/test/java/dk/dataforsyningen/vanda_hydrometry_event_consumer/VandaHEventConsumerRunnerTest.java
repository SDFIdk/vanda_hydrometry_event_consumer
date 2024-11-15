package dk.dataforsyningen.vanda_hydrometry_event_consumer;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import dk.dataforsyningen.vanda_hydrometry_event_consumer.config.VandaHEventConsumerConfig;
import dk.dataforsyningen.vanda_hydrometry_event_consumer.service.VandaHEventProcessor;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;

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
  public void setup() {
    config.setCommand(null);
  }

  @Test
  public void testNoArgs() throws Exception {

    String[] args = new String[0];

    // Save System.out
    final PrintStream oldStdout = System.out;

    // Make our own output console
    ByteArrayOutputStream bo = new ByteArrayOutputStream();
    // Set our console to be System.out for testing
    System.setOut(new PrintStream(bo));

    runner.run(args);

    bo.flush();
    // Set the Standard System.out back to original for not getting all other tests System.out - not messing up
    System.setOut(oldStdout);
    String allWrittenLines = bo.toString();
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
