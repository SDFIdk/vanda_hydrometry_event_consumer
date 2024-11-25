package dk.dataforsyningen.vanda_hydrometry_evet_consumer.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;

import dk.dataforsyningen.vanda_hydrometry_event_consumer.VandaHUtility;
import dk.dataforsyningen.vanda_hydrometry_event_consumer.model.EventModel;

public class EventModelTest {

	private final String stationId = "12345678";
	private final String operatorStationId = "WATSONC-773";
	private final int measurementPoint = 1;
	private final double result1 = 1376.9;
	private final int mtParamSc = 1233;
	private final int mtExamTypeSc = 25;
	private final int mtUnitSc = 19;
	private final int reasonCodeSc = 5;
	private final String loggerId = "logger";
	private final String dateTime = "2024-10-04T23:50:00.00Z";
	private final String measurementAdded =
		      "{\"EventType\":\"MeasurementAdded\",\"StationId\":\"12345678\",\"OperatorStationId\":\"WATSONC-773\",\"MeasurementPointNumber\":1,\"ExaminationTypeSc\":25,\"MeasurementDateTime\":\"2024-10-04T23:50:00.00Z\",\"LoggerId\":\"logger\",\"ParameterSc\":1233,\"UnitSc\":19,\"Result\":1376.9,\"ReasonCodeSc\":5}";
	private final String measurementUpdated =
		      "{\"EventType\":\"MeasurementUpdated\",\"StationId\":\"12345678\",\"OperatorStationId\":\"WATSONC-773\",\"MeasurementPointNumber\":1,\"ExaminationTypeSc\":25,\"MeasurementDateTime\":\"2024-10-04T23:50:00.00Z\",\"Result\":1376.9,\"ReasonCodeSc\":5}";
	private final String measurementDeleted =
		      "{\"EventType\":\"MeasurementDeleted\",\"StationId\":\"12345678\",\"MeasurementPointNumber\":1,\"ExaminationTypeSc\":25,\"MeasurementDateTime\":\"2024-10-04T23:50:00.00Z\"}";

	
	@Test
	public void testMeasurementAddedDecoding() throws JsonProcessingException {
		EventModel event = EventModel.fromJson(measurementAdded);
		
		assertEquals("MeasurementAdded", event.getEventType());
		assertEquals(stationId, event.getStationId());
		assertEquals(operatorStationId, event.getOperatorStationId());
		assertEquals(measurementPoint, event.getMeasurementPointNumber());
		assertEquals(mtUnitSc, event.getUnitSc());
		assertEquals(mtParamSc, event.getParameterSc());
		assertEquals(mtExamTypeSc, event.getExaminationTypeSc());
		assertEquals(reasonCodeSc, event.getReasonCodeSc());
		assertEquals(result1, event.getResult());
		assertEquals(VandaHUtility.parseForAPI(dateTime), event.getMeasurementDateTime());
		assertEquals(loggerId, event.getLoggerId());
		assertNull(event.getRecordDateTime());
		assertEquals(0, event.getOffset());
		assertEquals(0, event.getPartition());	
	}
	
	@Test
	public void testMeasurementUpdatedDecoding() throws JsonProcessingException {
		EventModel event = EventModel.fromJson(measurementUpdated);
		
		assertEquals("MeasurementUpdated", event.getEventType());
		assertEquals(stationId, event.getStationId());
		assertEquals(operatorStationId, event.getOperatorStationId());
		assertEquals(measurementPoint, event.getMeasurementPointNumber());
		assertNull(event.getUnitSc());
		assertNull(event.getParameterSc());
		assertEquals(mtExamTypeSc, event.getExaminationTypeSc());
		assertEquals(reasonCodeSc, event.getReasonCodeSc());
		assertEquals(result1, event.getResult());
		assertEquals(VandaHUtility.parseForAPI(dateTime), event.getMeasurementDateTime());
		assertNull(event.getLoggerId());
		assertNull(event.getRecordDateTime());
		assertEquals(0, event.getOffset());
		assertEquals(0, event.getPartition());	
	}
	
	@Test
	public void testMeasurementDeletedDecoding() throws JsonProcessingException {
		EventModel event = EventModel.fromJson(measurementDeleted);
		
		assertEquals("MeasurementDeleted", event.getEventType());
		assertEquals(stationId, event.getStationId());
		assertNull(event.getOperatorStationId());
		assertEquals(measurementPoint, event.getMeasurementPointNumber());
		assertNull(event.getUnitSc());
		assertNull(event.getParameterSc());
		assertEquals(mtExamTypeSc, event.getExaminationTypeSc());
		assertNull(event.getReasonCodeSc());
		assertNull(event.getResult());
		assertEquals(VandaHUtility.parseForAPI(dateTime), event.getMeasurementDateTime());
		assertNull(event.getLoggerId());
		assertNull(event.getRecordDateTime());
		assertEquals(0, event.getOffset());
		assertEquals(0, event.getPartition());	
	}
	
}
