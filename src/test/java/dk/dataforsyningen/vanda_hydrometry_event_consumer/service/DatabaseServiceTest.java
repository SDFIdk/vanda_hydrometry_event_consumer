package dk.dataforsyningen.vanda_hydrometry_event_consumer.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.slf4j.event.Level;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import dk.dataforsyningen.vanda_hydrometry_event_consumer.VandaHUtility;
import dk.dataforsyningen.vanda_hydrometry_event_consumer.config.VandaHEventConsumerConfig;
import dk.dataforsyningen.vanda_hydrometry_event_consumer.model.EventModel;
import dk.dataforsyningen.vanda_hydrometry_event_consumer.model.Measurement;
import dk.dataforsyningen.vanda_hydrometry_event_consumer.model.MeasurementType;
import dk.dataforsyningen.vanda_hydrometry_event_consumer.model.Station;

@SpringBootTest
public class DatabaseServiceTest {

	private final String stationId = "S1234567";	//it is fake
	private final String stationName = "name";
	private final String stationOwner = "owner";
	private final String stationDescription = "description";
	private final String stationOldNumber = "12345678";
	private final double locationX = 12.34;
	private final double locationY = 56.78;
	private final int locationSrid = 25832;
	
	private final int mtParamSc1 = 1233;
	private final String mtParam1 = "WaterLevel";
	private final int mtExamTypeSc1 = 999925;	//it is fake
	private final String mtExamType1 = "WaterLevel";
	private final int mtUnitSc1 = 19;
	private final String mtUnit1 = "CM";
	
	private final int mtParamSc2 = 1155;
	private final String mtParam2 = "Vandføring";
	private final int mtExamTypeSc2 = 999927;	//it is fake
	private final String mtExamType2 = "Vandføring";
	private final int mtUnitSc2 = 55;
	private final String mtUnit2 = "l/s";
	
	private final int measurementPoint1 = 1;
	private final double result1 = 12.34;
	private final double result2 = 56.78;
	private final double result3 = 90.11;
	private final double resultEC1 = 12.34;
	private final double resultEC2 = 56.78;
	private final double resultEC3 = 90.11;
	
	private Station station1;
	private MeasurementType mt1;
	private MeasurementType mt2;
	private Measurement m1;
	private EventModel event;
	
	private OffsetDateTime dt1DayAgo;
	private OffsetDateTime dt10MinAgo;
	private OffsetDateTime dt5MinAgo;
	private OffsetDateTime dtNow;
	
	private boolean removeInsertedMeasurementTypes = true;
	
	private boolean enableTest = false; 
	
	@Autowired
	private DatabaseService dbService;
	
	@Autowired
	VandaHEventConsumerConfig config;
	
	@BeforeEach
	public void setup() {
		
		enableTest = config.isEnableDbTest();
		
		System.out.println("Database testing " + (enableTest ? "enabled" : "disabled"));
		if (!enableTest) return;
		
		dtNow = VandaHUtility.parseForAPI(OffsetDateTime.now().toString());
		dt5MinAgo = VandaHUtility.parseForAPI(dtNow.minusMinutes(5).toString());
		dt10MinAgo = VandaHUtility.parseForAPI(dtNow.minusMinutes(10).toString());
		dt1DayAgo = VandaHUtility.parseForAPI(dtNow.minusDays(1).toString());
		
		mt1 = new MeasurementType();
		mt1.setParameterSc(mtParamSc1);
		mt1.setParameter(mtParam1);
		mt1.setExaminationTypeSc(mtExamTypeSc1);
		mt1.setExaminationType(mtExamType1);
		mt1.setUnitSc(mtUnitSc1);
		mt1.setUnit(mtUnit1);
		
		mt2 = new MeasurementType();
		mt2.setParameterSc(mtParamSc2);
		mt2.setParameter(mtParam2);
		mt2.setExaminationTypeSc(mtExamTypeSc2);
		mt2.setExaminationType(mtExamType2);
		mt2.setUnitSc(mtUnitSc2);
		mt2.setUnit(mtUnit2);
		
		station1 = new Station();
		station1.setStationId(stationId);
		station1.setName(stationName);
		station1.setStationOwnerName(stationOwner);
		station1.setDescription(stationDescription);
		station1.setOldStationNumber(stationOldNumber);
		station1.setLocationX(locationX);
		station1.setLocationY(locationY);
		station1.setLocationSrid(locationSrid);
		station1.getMeasurementTypes().add(mt1);
		station1.getMeasurementTypes().add(mt2);
				
		m1 = new Measurement();
		m1.setStationId(stationId);
		m1.setIsCurrent(true);
		m1.setMeasurementPointNumber(measurementPoint1);
		m1.setResult(result1);
		m1.setResultElevationCorrected(resultEC1);
		m1.setExaminationTypeSc(mtExamTypeSc1);
		m1.setMeasurementDateTime(dt1DayAgo);
		
		event = new EventModel();
		event.setStationId(stationId);
		event.setMeasurementPointNumber(measurementPoint1);
		event.setUnitSc(mtUnitSc1);
		event.setParameterSc(mtParamSc1);
		event.setExaminationTypeSc(mtExamTypeSc1);
		event.setResult(result2);
		event.setMeasurementDateTime(dt1DayAgo);
				
		deleteAll(); //clean first if any left overs
		
		addStations();
	}
	
	/**
	 * Clean the inserted items
	 */
	@AfterEach
	public void deleteAll() {

		if (!enableTest) return;

		dbService.deleteStationMeasurementTypeRelation(stationId);
		
		testDeleteMeasurement();

		testDeleteStation();
		
		testDeleteMeasurementType();

	}	
		
	public void restOfTests() throws SQLException {
		
		//TODO clean up, move to other tests

		//new measurementAdded event, different measurement date => new measurement
		event.setMeasurementDateTime(dt10MinAgo);
		//invalid measurement type => will fail
		event.setExaminationTypeSc(0);
		event.setUnitSc(1);
		event.setParameterSc(1);
		
		addMissingMeasurementTypeByEvent(event);
		
		//db status:2 measurements on dt5MinAgo
		//			1 measurement on dtNow
		

		event = new EventModel();
		
		event.setEventType(VandaHEventProcessor.EVENT_MEASUREMENT_UPDATED);
		event.setStationId(stationId);
		event.setMeasurementPointNumber(measurementPoint1);
		event.setUnitSc(0);
		event.setParameterSc(0);
		event.setExaminationTypeSc(mtExamTypeSc1);
		event.setResult(result3);
		event.setMeasurementDateTime(dt5MinAgo);

		//new measurementUpdated event on existing measurement => update (new record)
		updateExistingMeasurement(event);
		
		//db status:3 measurements on dt5MinAgo
		//			1 measurement on dtNow
		
		//new measurementUpdated event, different measurement date => new measurement, fail
		event.setMeasurementDateTime(dt10MinAgo);
		
		updateNonexistingMeasurement(event);
		
		
		event = new EventModel();
		
		event.setEventType(VandaHEventProcessor.EVENT_MEASUREMENT_DELETED);
		event.setStationId(stationId);
		event.setMeasurementPointNumber(measurementPoint1);
		event.setUnitSc(0);
		event.setParameterSc(0);
		event.setExaminationTypeSc(mtExamTypeSc1);
		event.setResult(null);
		event.setMeasurementDateTime(dt5MinAgo);
		
		//new measurementDeleted event on existing measurement => all history gets isCurrent=false
		deleteExistingMeasurement(event);
		
		//new measurementDeleted event, different measurement date => fail
		event.setMeasurementDateTime(dt10MinAgo);
		
		deleteNonexistingMeasurement(event);
		
	}
	
	/**
	 * Test Scenario 1 assumes these steps:
	 * 	- received measurement added with TS NOW-10 min
	 * 	- received measurement update with TS NOW-5 min
	 * 	- received measurement deleted with TS NOW
	 * 
	 * @throws SQLException
	 * @throws InterruptedException 
	 */
	@Test
	public void testScenario1() throws SQLException, InterruptedException {
		
		if (!enableTest) return;
				
		////////////// Received event MeasurementAdded
		event.setEventType(VandaHEventProcessor.EVENT_MEASUREMENT_ADDED);
		event.setRecordDateTime(dt10MinAgo);
		Measurement measurement = dbService.addMeasurementFromEvent(event);
		
		Measurement currentMeasurement = dbService.getMeasurement(measurement.getStationId(), 
				measurement.getMeasurementPointNumber(), 
				measurement.getExaminationTypeSc(), 
				measurement.getMeasurementDateTime());
		
		List<Measurement> history = dbService.getMeasurementHistory(event.getStationId(), 
				event.getMeasurementPointNumber(), 
				event.getExaminationTypeSc(), 
				event.getMeasurementDateTime());
		
		assertEquals(1, history.size());
		
		assertEquals(currentMeasurement, history.getFirst());
		assertTrue(currentMeasurement.getIsCurrent());
		assertEquals(stationId, currentMeasurement.getStationId());
		assertEquals(result2, currentMeasurement.getResult());
		assertNull(currentMeasurement.getResultElevationCorrected());
		assertEquals(dt1DayAgo, currentMeasurement.getMeasurementDateTime());
		assertEquals(dt10MinAgo, currentMeasurement.getVandaEventTimestamp());
		
		//db status: 1 measurements from dt1DayAgo (TS = dt10MinAgo)
		
		////////////// Received event MeasurementUpdated
		event.setEventType(VandaHEventProcessor.EVENT_MEASUREMENT_UPDATED);
		event.setResult(result1); //same as the one from API
		event.setRecordDateTime(dt5MinAgo);
		Thread.sleep(300);
		measurement = dbService.updateMeasurementFromEvent(event);
		
		currentMeasurement = dbService.getMeasurement(event.getStationId(), 
				event.getMeasurementPointNumber(), 
				event.getExaminationTypeSc(), 
				event.getMeasurementDateTime());
		
		history = dbService.getMeasurementHistory(event.getStationId(), 
				event.getMeasurementPointNumber(), 
				event.getExaminationTypeSc(), 
				event.getMeasurementDateTime());
		
		assertEquals(2, history.size());
		Measurement oldM = history.getFirst();
		Measurement newM = history.getLast();
		assertFalse(oldM.getIsCurrent());
		assertTrue(newM.getIsCurrent());
		assertTrue(oldM.getCreated().isBefore(newM.getCreated()));
		
		assertEquals(stationId, oldM.getStationId());
		assertEquals(dt1DayAgo, oldM.getMeasurementDateTime());
		assertEquals(newM.getStationId(), oldM.getStationId());
		assertEquals(newM.getMeasurementDateTime(), oldM.getMeasurementDateTime());
		assertEquals(result2, oldM.getResult());
		assertNull(oldM.getResultElevationCorrected());
		assertEquals(result1, newM.getResult());
		assertNull(newM.getResultElevationCorrected());
		assertEquals(newM, currentMeasurement);
		assertEquals(dt10MinAgo, oldM.getVandaEventTimestamp());
		assertEquals(dt5MinAgo, newM.getVandaEventTimestamp());
		
		//db status: 2 measurements from dt1DayAgo (the active one with TS = dt5MinAgo)
		
		////////////// Received event MeasurementDeleted
		event.setEventType(VandaHEventProcessor.EVENT_MEASUREMENT_DELETED);
		event.setResult(null);
		event.setRecordDateTime(dtNow);
		Thread.sleep(300);
		dbService.deleteMeasurementFromEvent(event);
		
		currentMeasurement = dbService.getMeasurement(event.getStationId(), 
				event.getMeasurementPointNumber(), 
				event.getExaminationTypeSc(), 
				event.getMeasurementDateTime());
		
		assertNull(currentMeasurement);
		
		history = dbService.getMeasurementHistory(event.getStationId(), 
				event.getMeasurementPointNumber(), 
				event.getExaminationTypeSc(), 
				event.getMeasurementDateTime());
	
		assertEquals(2, history.size());
		oldM = history.getFirst();
		newM = history.getLast();
		assertFalse(oldM.getIsCurrent());
		assertFalse(newM.getIsCurrent());
		assertTrue(oldM.getCreated().isBefore(newM.getCreated()));
	}
	
	
	/**
	 * Test Scenario 2 assumes these steps:
	 * 	- received measurement added  with TS NOW-10 min
	 * 	- read measurement from API   at NOW-5 min
	 * 	- received measurement update with TS NOW
	 * 
	 * @throws SQLException
	 * @throws InterruptedException 
	 */
	@Test
	public void testScenario2() throws SQLException, InterruptedException {
		
		if (!enableTest) return;
		
		////////////// add measurement as if it would be read from API
		m1 = addMeasurement(m1);
		
		//status: 1 measurement from dt1DayAgo (TS = null)
		
		////////////// Received event MeasurementAdded
		event.setEventType(VandaHEventProcessor.EVENT_MEASUREMENT_ADDED);
		event.setRecordDateTime(dt10MinAgo);
		Thread.sleep(300);
		Measurement measurement = dbService.addMeasurementFromEvent(event);
		
		Measurement currentMeasurement = dbService.getMeasurement(measurement.getStationId(), 
				measurement.getMeasurementPointNumber(), 
				measurement.getExaminationTypeSc(), 
				measurement.getMeasurementDateTime());
		
		List<Measurement> history = dbService.getMeasurementHistory(event.getStationId(), 
				event.getMeasurementPointNumber(), 
				event.getExaminationTypeSc(), 
				event.getMeasurementDateTime());
		
		assertEquals(2, history.size());
		Measurement oldM = history.getFirst();
		Measurement newM = history.getLast();
		assertFalse(oldM.getIsCurrent());
		assertTrue(newM.getIsCurrent());
		assertTrue(oldM.getCreated().isBefore(newM.getCreated()));
		
		assertEquals(stationId, oldM.getStationId());
		assertEquals(dt1DayAgo, oldM.getMeasurementDateTime());
		assertEquals(newM.getStationId(), oldM.getStationId());
		assertEquals(newM.getMeasurementDateTime(), oldM.getMeasurementDateTime());
		assertEquals(result1, oldM.getResult());
		assertEquals(resultEC1, oldM.getResultElevationCorrected());		
		assertEquals(result2, newM.getResult());
		assertNull(newM.getResultElevationCorrected());
		assertEquals(newM, currentMeasurement);
		assertNull(oldM.getVandaEventTimestamp());
		assertEquals(dt10MinAgo, newM.getVandaEventTimestamp());
		
		//db status: 2 measurements from dt1DayAgo (the active one with TS = dt10MinAgo)
		
		////////////// Received event MeasurementUpdated
		event.setEventType(VandaHEventProcessor.EVENT_MEASUREMENT_UPDATED);
		event.setResult(result1); //same as the one from API
		event.setRecordDateTime(dt5MinAgo);
		Thread.sleep(300);
		measurement = dbService.updateMeasurementFromEvent(event);
		
		currentMeasurement = dbService.getMeasurement(event.getStationId(), 
				event.getMeasurementPointNumber(), 
				event.getExaminationTypeSc(), 
				event.getMeasurementDateTime());
		
		history = dbService.getMeasurementHistory(event.getStationId(), 
				event.getMeasurementPointNumber(), 
				event.getExaminationTypeSc(), 
				event.getMeasurementDateTime());
		
		assertEquals(3, history.size());
		Measurement olderM = history.getFirst();
		oldM = history.get(1);
		newM = history.getLast();
		assertFalse(olderM.getIsCurrent());
		assertFalse(oldM.getIsCurrent());
		assertTrue(newM.getIsCurrent());
		assertTrue(olderM.getCreated().isBefore(oldM.getCreated()));
		assertTrue(oldM.getCreated().isBefore(newM.getCreated()));
		
		assertEquals(stationId, olderM.getStationId());
		assertEquals(dt1DayAgo, olderM.getMeasurementDateTime());
		assertEquals(olderM.getStationId(), oldM.getStationId());
		assertEquals(olderM.getMeasurementDateTime(), oldM.getMeasurementDateTime());
		assertEquals(newM.getStationId(), oldM.getStationId());
		assertEquals(newM.getMeasurementDateTime(), oldM.getMeasurementDateTime());
		assertEquals(result1, olderM.getResult());
		assertEquals(resultEC1, olderM.getResultElevationCorrected());		
		assertEquals(result2, oldM.getResult());
		assertNull(oldM.getResultElevationCorrected());
		assertEquals(result1, newM.getResult());
		assertNull(newM.getResultElevationCorrected());
		assertEquals(newM, currentMeasurement);
		assertNull(olderM.getVandaEventTimestamp());
		assertEquals(dt10MinAgo, oldM.getVandaEventTimestamp());
		assertEquals(dt5MinAgo, newM.getVandaEventTimestamp());
		
		//db status: 3 measurements from dt1DayAgo (the active one with TS = dt5MinAgo)
		
		////////////// Received event MeasurementDeleted
		event.setEventType(VandaHEventProcessor.EVENT_MEASUREMENT_DELETED);
		event.setResult(null);
		event.setRecordDateTime(dtNow);
		Thread.sleep(300);
		dbService.deleteMeasurementFromEvent(event);
		
		currentMeasurement = dbService.getMeasurement(event.getStationId(), 
				event.getMeasurementPointNumber(), 
				event.getExaminationTypeSc(), 
				event.getMeasurementDateTime());
		
		assertNull(currentMeasurement);
		
		history = dbService.getMeasurementHistory(event.getStationId(), 
				event.getMeasurementPointNumber(), 
				event.getExaminationTypeSc(), 
				event.getMeasurementDateTime());

		assertEquals(3, history.size());
		olderM = history.getFirst();
		oldM = history.get(1);
		newM = history.getLast();
		assertFalse(olderM.getIsCurrent());
		assertFalse(oldM.getIsCurrent());
		assertFalse(newM.getIsCurrent());
		assertTrue(olderM.getCreated().isBefore(oldM.getCreated()));
		assertTrue(oldM.getCreated().isBefore(newM.getCreated()));
		
	}
	
	
	/**
	 * Test Scenario 3 assumes these steps:
	 * 	- received measurement added  with TS NOW-10 min
	 * 	- received measurement update with TS NOW-5 min
	 * 	- read measurement from API   at NOW min
	 * 
	 * @throws SQLException
	 */
	@Test
	public void testScenario3() throws SQLException {
		
		if (!enableTest) return;
		
		//TODO implement
	}
	
	/**
	 * Test Scenario 4 assumes these steps:
	 *  - read measurement from API   at NOW min
	 *  - received measurement added  with TS NOW-10 min - drop delayed event
	 * 	- received measurement update with TS NOW-5 min - drop delayed event
	 * 
	 * @throws SQLException
	 */
	@Test
	public void testScenario4() throws SQLException {
		
		if (!enableTest) return;
		
		//TODO implement
	}
	
	/**
	 * Test Scenario 5 assumes these steps:
	 * 	- received measurement update with TS NOW-5 min
	 * 	- received measurement update with TS NOW min
	 * 	- received measurement added  with TS NOW-10 min - drop delayed event
	 * 
	 * @throws SQLException
	 */
	@Test
	public void testScenario5() throws SQLException {
		
		if (!enableTest) return;
		
		//TODO implement
	}
	 
	
	private void addStations() {
		
		dbService.addStation(station1);
		
		Station station = dbService.getStation(station1.getStationId());
		
		assertNotNull(station);
		
	}
	
	private Measurement addMeasurement(Measurement measurement) {
	
		Measurement insertedMeasurement = dbService.addMeasurement(measurement);
		
		assertEquals(stationId, insertedMeasurement.getStationId());
		assertEquals(result1, insertedMeasurement.getResult());
		assertEquals(resultEC1, insertedMeasurement.getResultElevationCorrected());
		assertEquals(dt1DayAgo, insertedMeasurement.getMeasurementDateTime());
		
		Measurement readMeasurement = dbService.getMeasurement(insertedMeasurement.getStationId(), 
					insertedMeasurement.getMeasurementPointNumber(), 
					insertedMeasurement.getExaminationTypeSc(), 
					insertedMeasurement.getMeasurementDateTime());
		
		assertEquals(stationId, readMeasurement.getStationId());
		assertEquals(result1, readMeasurement.getResult());
		assertEquals(resultEC1, readMeasurement.getResultElevationCorrected());
		assertEquals(dt1DayAgo, readMeasurement.getMeasurementDateTime());
		
		return measurement;
	}
	
	/**
	 * Adding existing measurement again will perform an update (new record) and a WARN
	 * @param event
	 * @throws SQLException
	 */
	private void addExistingMeasurementByEvent(EventModel event) throws SQLException {
		
		Measurement measurement;
		
		try (MockedStatic<VandaHUtility> mockedStatic = mockStatic(VandaHUtility.class)) {
						
			measurement = dbService.addMeasurementFromEvent(event);
			
			mockedStatic.verify(() -> VandaHUtility.logAndPrint(any(), eq(Level.WARN), eq(false), startsWith("Added existing measurement")), times(1));
		}
		
		int nrMeas = dbService.countMeasurementHistory(event.getStationId(), 
				event.getMeasurementPointNumber(), 
				measurement.getExaminationTypeSc(), 
				event.getMeasurementDateTime());
			
		assertEquals(2, nrMeas);
	}
	
	
	private void addMissingMeasurementTypeByEvent(EventModel event) {
				
		assertThrows(SQLException.class, () -> dbService.addMeasurementFromEvent(event));
	}
	
	private void updateExistingMeasurement(EventModel event) throws SQLException {
		
		Measurement measurement = dbService.updateMeasurementFromEvent(event);
		
		int nrMeas = dbService.countMeasurementHistory(measurement.getStationId(), 
				measurement.getMeasurementPointNumber(), 
				measurement.getExaminationTypeSc(), 
				dtNow);
			
		assertEquals(1, nrMeas);
		
		nrMeas = dbService.countMeasurementHistory(measurement.getStationId(), 
				measurement.getMeasurementPointNumber(), 
				measurement.getExaminationTypeSc(), 
				dt5MinAgo);
			
		assertEquals(3, nrMeas);
		
	}
	
	private void updateNonexistingMeasurement(EventModel event) throws SQLException {
		Measurement measurement;
		
		try (MockedStatic<VandaHUtility> mockedStatic = mockStatic(VandaHUtility.class)) {
			
			measurement = dbService.updateMeasurementFromEvent(event);
			
			mockedStatic.verify(() -> VandaHUtility.logAndPrint(any(), eq(Level.WARN), eq(false), startsWith("Update on nonexistent measurement")), times(1));
		}
		
		assertNull(measurement);
				
		int nrMeas = dbService.countMeasurementHistory(station1.getStationId(), 
				measurementPoint1, 
				mt1.getExaminationTypeSc(), //we know it should be this one 
				dtNow);
					
		assertEquals(1, nrMeas);
		
		nrMeas = dbService.countMeasurementHistory(station1.getStationId(), 
				measurementPoint1, 
				mt1.getExaminationTypeSc(), //we know it should be this one 
				dt5MinAgo);
			
		assertEquals(3, nrMeas);
		
	}
	
	private void deleteExistingMeasurement(EventModel event) {
		
		Measurement measurementBefore = dbService.getMeasurement(event.getStationId(), 
				event.getMeasurementPointNumber(), 
				mt1.getExaminationTypeSc(), event.getMeasurementDateTime());
		
		assertTrue(measurementBefore.getIsCurrent());
		
		dbService.deleteMeasurementFromEvent(event);
		
		List<Measurement> measurementHistory = dbService.getMeasurementHistory(measurementBefore.getStationId(), 
				measurementBefore.getMeasurementPointNumber(), 
				measurementBefore.getExaminationTypeSc(), measurementBefore.getMeasurementDateTime());
		
		measurementHistory.stream().forEach(m -> assertFalse(m.getIsCurrent()));
				
		int nrMeas = dbService.countMeasurementHistory(station1.getStationId(), 
				measurementPoint1, 
				mt1.getExaminationTypeSc(), //we know it should be this one 
				dtNow);
							
		assertEquals(1, nrMeas);
				
		nrMeas = dbService.countMeasurementHistory(station1.getStationId(), 
				measurementPoint1, 
				mt1.getExaminationTypeSc(), //we know it should be this one 
				dt5MinAgo);
					
		assertEquals(3, nrMeas);
	}
	
	private void deleteNonexistingMeasurement(EventModel event) {
		
		try (MockedStatic<VandaHUtility> mockedStatic = mockStatic(VandaHUtility.class)) {
			
			dbService.deleteMeasurementFromEvent(event);
			
			mockedStatic.verify(() -> VandaHUtility.logAndPrint(any(), eq(Level.WARN), eq(false), startsWith("Delete of nonexistent measurement")), times(1));
		}
				
		int nrMeas = dbService.countMeasurementHistory(station1.getStationId(), 
				measurementPoint1, 
				mt1.getExaminationTypeSc(), //we know it should be this one 
				dtNow);
					
		assertEquals(1, nrMeas);
		
		nrMeas = dbService.countMeasurementHistory(station1.getStationId(), 
				measurementPoint1, 
				mt1.getExaminationTypeSc(), //we know it should be this one 
				dt5MinAgo);
			
		assertEquals(3, nrMeas);
		
	}
	
	
	
	private void testDeleteStation() {
		dbService.deleteStation(stationId);
		
		Station station = dbService.getStation(stationId);
		
		assertNull(station); 
	}
	
	private void testDeleteMeasurementType() {
		dbService.deleteMeasurementType(mtExamTypeSc1);
			
		MeasurementType mt = dbService.getMeasurementType(mtExamTypeSc1);
		assertNull(mt);
			
		dbService.deleteMeasurementType(mtExamTypeSc2);
			
		mt = dbService.getMeasurementType(mtExamTypeSc2);
		assertNull(mt);
	}
	
	private void testDeleteMeasurement() {
		dbService.deleteMeasurementHard(stationId);
			
		Measurement m = dbService.getMeasurement(stationId, measurementPoint1, mtExamTypeSc1, dt1DayAgo);
		assertNull(m);
	}
}
