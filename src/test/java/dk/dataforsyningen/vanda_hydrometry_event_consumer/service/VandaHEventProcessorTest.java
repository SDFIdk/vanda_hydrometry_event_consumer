package dk.dataforsyningen.vanda_hydrometry_event_consumer.service;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;

import dk.dataforsyningen.vanda_hydrometry_event_consumer.VandaHUtility;
import dk.dataforsyningen.vanda_hydrometry_event_consumer.config.VandaHEventConsumerConfig;
import dk.dataforsyningen.vanda_hydrometry_event_consumer.model.EventModel;

@SpringBootTest
public class VandaHEventProcessorTest {

	private String topic = "measurements";
	
	private String measurementAdded = "{\"EventType\":\"MeasurementAdded\",\"StationId\":\"12345678\",\"OperatorStationId\":\"WATSONC-773\",\"MeasurementPointNumber\":1,\"ExaminationTypeSc\":25,\"MeasurementDateTime\":\"2024-10-04T23:50:00.00Z\",\"LoggerId\":\"logger\",\"ParameterSc\":1233,\"UnitSc\":19,\"Result\":1376.9,\"ReasonCodeSc\":5}";
	private String measurementUpdated = "{\"EventType\":\"MeasurementUpdated\",\"StationId\":\"12345678\",\"OperatorStationId\":\"WATSONC-1519\",\"MeasurementPointNumber\":1,\"ExaminationTypeSc\":25,\"MeasurementDateTime\":\"2024-10-04T23:50:00.00Z\",\"Result\":82.2,\"ReasonCodeSc\":5}";
	private String measurementDeleted = "{\\\"EventType\\\":\\\"MeasurementDeleted\\\",\\\"StationId\\\":\\\"12345678\\\",\\\"OperatorStationId\\\":\\\"WATSONC-1519\\\",\\\"MeasurementPointNumber\\\":1,\\\"ExaminationTypeSc\\\":25,\\\"MeasurementDateTime\\\":\\\"2024-10-04T23:50:00.00Z\\\",\\\"Result\\\":0,\\\"ReasonCodeSc\\\":5}";
	
	ConsumerRecord<String, String> recordAdd;
	ConsumerRecord<String, String> recordUpdate;
	ConsumerRecord<String, String> recordDelete;
	
	EventModel event;
	
	private final String stationId = "12345678";
	private final int measurementPoint = 1;
	private final double result1 = 1376.9;
	private final double result2 = 82.2;
	private final int mtParamSc = 1233;
	private final int mtExamTypeSc = 25;
	private final int mtUnitSc = 19;
	private final String dateTime = "2024-10-04T23:50:00.00Z";
	private final List<Integer> examinationTypes = List.of(25, 27);
	
	@MockBean
	private DatabaseService dbService;
	
	@MockBean
	VandaHEventConsumerConfig config;
	
	@InjectMocks
	private VandaHEventProcessor processor = new VandaHEventProcessor();
	
	@BeforeEach 
	public void setup() {
		when(config.isSaveDb()).thenReturn(true);
		when(config.getExaminationTypeSc()).thenReturn(examinationTypes);
		
		recordAdd = new ConsumerRecord<>(topic, 1, 0, null, measurementAdded);
		recordUpdate = new ConsumerRecord<>(topic, 1, 0, null, measurementUpdated);
		recordDelete = new ConsumerRecord<>(topic, 1, 0, null, measurementDeleted);
		
		event = new EventModel();
		
		event.setStationId(stationId);
		event.setMeasurementPointNumber(measurementPoint);
		event.setUnitSc(mtUnitSc);
		event.setParameterSc(mtParamSc);
		event.setExaminationTypeSc(mtExamTypeSc);
		event.setMeasurementDateTime(VandaHUtility.parseForAPI(dateTime));
	}
	
	@Test
	public void testAdd() throws SQLException {
		
		processor.consume(recordAdd);
		
		event.setEventType(VandaHEventProcessor.EVENT_MEASUREMENT_ADDED);
		event.setResult(result1);
		
		verify(dbService).addMeasurement(event);
	}
	
	@Test
	public void testUpdate() throws SQLException {
		
		processor.consume(recordUpdate);
		
		event.setEventType(VandaHEventProcessor.EVENT_MEASUREMENT_UPDATED);
		event.setResult(result2);
		
		verify(dbService).addMeasurement(event);
	}
	
	@Test
	public void testDeleted() throws SQLException {
		
		processor.consume(recordDelete);
		
		event.setEventType(VandaHEventProcessor.EVENT_MEASUREMENT_DELETED);
		event.setResult(0);
		
		verify(dbService).addMeasurement(event);
	}
	
}
