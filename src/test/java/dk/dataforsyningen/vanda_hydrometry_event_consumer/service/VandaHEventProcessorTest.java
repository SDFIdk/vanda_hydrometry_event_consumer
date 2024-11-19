package dk.dataforsyningen.vanda_hydrometry_event_consumer.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import dk.dataforsyningen.vanda_hydrometry_event_consumer.VandaHUtility;
import dk.dataforsyningen.vanda_hydrometry_event_consumer.config.VandaHEventConsumerConfig;
import dk.dataforsyningen.vanda_hydrometry_event_consumer.model.EventModel;
import java.sql.SQLException;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.InjectMocks;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;

@SpringBootTest
public class VandaHEventProcessorTest {

  private final String stationId = "12345678";
  private final String operatorStationId = "WATSONC-773";
  private final int measurementPoint = 1;
  private final double result1 = 1376.9;
  private final double result2 = 82.2;
  private final int mtParamSc = 1233;
  private final int mtExamTypeSc = 25;
  private final int mtUnitSc = 19;
  private final int reasonCodeSc = 5;
  private final String dateTime = "2024-10-04T23:50:00.00Z";
  private final String recordDateTime = "1969-12-31T23:59:59.999Z";
  private final List<Integer> examinationTypes = List.of(25, 27);
  ConsumerRecord<String, String> recordAdd;
  ConsumerRecord<String, String> recordUpdate;
  ConsumerRecord<String, String> recordDelete;
  ConsumerRecord<String, String> recordIrrelevant;
  EventModel event;
  private final String topic = "measurements";
  private final String measurementAdded =
      "{\"EventType\":\"MeasurementAdded\",\"StationId\":\"12345678\",\"OperatorStationId\":\"WATSONC-773\",\"MeasurementPointNumber\":1,\"ExaminationTypeSc\":25,\"MeasurementDateTime\":\"2024-10-04T23:50:00.00Z\",\"LoggerId\":\"logger\",\"ParameterSc\":1233,\"UnitSc\":19,\"Result\":1376.9,\"ReasonCodeSc\":5}";
  private final String measurementUpdated =
      "{\"EventType\":\"MeasurementUpdated\",\"StationId\":\"12345678\",\"OperatorStationId\":\"WATSONC-773\",\"MeasurementPointNumber\":1,\"ExaminationTypeSc\":25,\"MeasurementDateTime\":\"2024-10-04T23:50:00.00Z\",\"Result\":82.2,\"ReasonCodeSc\":5}";
  private final String measurementDeleted =
      "{\"EventType\":\"MeasurementDeleted\",\"StationId\":\"12345678\",\"MeasurementPointNumber\":1,\"ExaminationTypeSc\":25,\"MeasurementDateTime\":\"2024-10-04T23:50:00.00Z\"}";
  private final String irrelevantMeasurement =
      "{\"EventType\":\"MeasurementAdded\",\"StationId\":\"70000278\",\"OperatorStationId\":\"WATSONC-1518\",\"MeasurementPointNumber\":1,\"ExaminationTypeSc\":29,\"MeasurementDateTime\":\"2024-10-05T00:00:00.00Z\",\"LoggerId\":\"logger\",\"ParameterSc\":1154,\"UnitSc\":29,\"Result\":11.1,\"ReasonCodeSc\":5}";
  @MockBean
  private DatabaseService dbService;

  @MockBean
  private VandaHEventConsumerConfig config;

  @InjectMocks
  @Autowired
  private VandaHEventProcessor processor;

  @BeforeEach
  public void setup() {
    when(config.isSaveDb()).thenReturn(true);
    when(config.getExaminationTypeSc()).thenReturn(examinationTypes);
    when(config.processAdditions()).thenReturn(true);
    when(config.processUpdates()).thenReturn(true);
    when(config.processDeletions()).thenReturn(true);

    recordAdd = new ConsumerRecord<>(topic, 1, 0, null, measurementAdded);
    recordUpdate = new ConsumerRecord<>(topic, 1, 0, null, measurementUpdated);
    recordDelete = new ConsumerRecord<>(topic, 1, 0, null, measurementDeleted);
    recordIrrelevant = new ConsumerRecord<>(topic, 1, 0, null, irrelevantMeasurement);

    event = new EventModel();

    event.setStationId(stationId);
    event.setOperatorStationId(operatorStationId);
    event.setMeasurementPointNumber(measurementPoint);
    event.setUnitSc(mtUnitSc);
    event.setParameterSc(mtParamSc);
    event.setExaminationTypeSc(mtExamTypeSc);
    event.setMeasurementDateTime(VandaHUtility.parseForAPI(dateTime));
    event.setReasonCodeSc(reasonCodeSc);
    event.setPartition(1);
    event.setRecordDateTime(VandaHUtility.parseToUtcOffsetDateTime(recordDateTime));

  }

  @Test
  public void testSkipMeasurement() throws SQLException {

    processor.consume(recordIrrelevant);

    verify(dbService, never()).addMeasurement(any());
  }

  @Test
  public void testAdd() throws SQLException {

    processor.consume(recordAdd);

    event.setEventType(VandaHEventProcessor.EVENT_MEASUREMENT_ADDED);
    event.setResult(result1);

    verify(dbService).addMeasurementFromEvent(event);
  }

  @Test
  public void testUpdate() throws SQLException {

    processor.consume(recordUpdate);

    event.setEventType(VandaHEventProcessor.EVENT_MEASUREMENT_UPDATED);
    event.setUnitSc(null);
    event.setParameterSc(null);
    event.setResult(result2);

    verify(dbService).updateMeasurementFromEvent(event);
  }

  @Test
  public void testDeleted() throws SQLException {

    processor.consume(recordDelete);

    event.setEventType(VandaHEventProcessor.EVENT_MEASUREMENT_DELETED);
    event.setUnitSc(null);
    event.setParameterSc(null);
    event.setResult(null);

    verify(dbService).deleteMeasurementFromEvent(argThat(new ArgumentMatcher<EventModel>() {

      @Override
      public boolean matches(EventModel e) {
        return event.getStationId().equals(e.getStationId()) &&
            event.getMeasurementPointNumber() == e.getMeasurementPointNumber() &&
            event.getExaminationTypeSc() == e.getExaminationTypeSc() &&
            event.getMeasurementDateTime().equals(e.getMeasurementDateTime());
      }

    }));
  }

}
