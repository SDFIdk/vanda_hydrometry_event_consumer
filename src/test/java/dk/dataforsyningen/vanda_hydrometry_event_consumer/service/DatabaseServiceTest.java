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

	private final String stationId = "S1234567";
	private final String stationName = "name";
	private final String stationOwner = "owner";
	private final String stationDescription = "description";
	private final String stationOldNumber = "12345678";
	private final double locationX = 12.34;
	private final double locationY = 56.78;
	private final int locationSrid = 25832;
	
	private final String mtId1 = "1233-25-19";
	private final int mtParamSc1 = 1233;
	private final String mtParam1 = "WaterLevel";
	private final int mtExamTypeSc1 = 25;
	private final String mtExamType1 = "WaterLevel";
	private final int mtUnitSc1 = 19;
	private final String mtUnit1 = "CM";
	
	private final String mtId2 = "1155-27-55";
	private final int mtParamSc2 = 1155;
	private final String mtParam2 = "Vandføring";
	private final int mtExamTypeSc2 = 27;
	private final String mtExamType2 = "Vandføring";
	private final int mtUnitSc2 = 55;
	private final String mtUnit2 = "l/s";
	
	private final int measurementPoint1 = 1;
	private final double result1 = 12.34;
	private final double result2 = 56.78;
	private final double result3 = 90.11;
	
	private Station station1;
	private MeasurementType mt1;
	private MeasurementType mt2;
	private Measurement m1;
	private OffsetDateTime dt5MinAgo;
	private OffsetDateTime dt10MinAgo;
	private OffsetDateTime dtNow;
	private OffsetDateTime date1;
	
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
		
		mt1 = new MeasurementType();
		mt1.setMeasurementTypeId(mtId1);
		mt1.setParameterSc(mtParamSc1);
		mt1.setParameter(mtParam1);
		mt1.setExaminationTypeSc(mtExamTypeSc1);
		mt1.setExaminationType(mtExamType1);
		mt1.setUnitSc(mtUnitSc1);
		mt1.setUnit(mtUnit1);
		
		mt2 = new MeasurementType();
		mt2.setMeasurementTypeId(mtId2);
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
		m1.setMeasurementTypeId(mtId1);
		m1.setMeasurementDateTime(dt5MinAgo);
				
	}
	
	/**
	 * Clean the inserted items
	 */
	@AfterEach
	public void deleteAll() {

		if (!enableTest) return;

		testDeleteMeasurement();

		testDeleteStation();
		
		testDeleteMeasurementType();

	}
	
	@Test
	public void test() throws SQLException {
		
		if (!enableTest) return;
		
		addStations();
		
		//add measurement with date/time dt5MinAgo (as if it would be read from API)
		addMeasurement();
		
		//status: 1 measurement on dt5MinAgo
		
		EventModel event = new EventModel();
		
		event.setEventType(VandaHEventProcessor.EVENT_MEASUREMENT_ADDED);
		event.setStationId(stationId);
		event.setMeasurementPointNumber(measurementPoint1);
		event.setUnitSc(mtUnitSc1);
		event.setParameterSc(mtParamSc1);
		event.setExaminationTypeSc(mtExamTypeSc1);
		event.setResult(result2);
		event.setMeasurementDateTime(dt5MinAgo);

		//new measurementAdded event on existing measurement => update (new record) and WARN
		addExistingMeasurementByEvent(event);
		
		//db status: 2 measurements on dt5MinAgo
		
		//new measurementAdded event, different measurement date => new measurement
		event.setMeasurementDateTime(dtNow);
		
		addNewMeasurementByEvent(event);
		
		//db status:2 measurements on dt5MinAgo
		//			1 measurement on dtNow
		
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
		event.setResult(0);
		event.setMeasurementDateTime(dt5MinAgo);
		
		//new measurementDeleted event on existing measurement => all history gets isCurrent=false
		deleteExistingMeasurement(event);
		
		//new measurementDeleted event, different measurement date => fail
		event.setMeasurementDateTime(dt10MinAgo);
		
		deleteNonexistingMeasurement(event);
		
	}
	
	private void addStations() {
		
		int nrStations0 = dbService.countAllStations();
		int nrMeasType0 = dbService.countAllMeasurementTypes();
		
		dbService.addStation(station1);
		
		Station station = dbService.getStation(station1.getStationId());
		
		assertNotNull(station);
		
		int nrStations1 = dbService.countAllStations();
		int nrMeasType1 = dbService.countAllMeasurementTypes();
		
		assertEquals(nrStations0 + 1, nrStations1);
		
		if (nrMeasType0 == nrMeasType1) {
			//the measurement type existed already so do not delete them
			removeInsertedMeasurementTypes = false;
		}
	}
	
	private void addMeasurement() {
		int nrMeas0 = dbService.countAllMeasurements();
		
		Measurement insertedMeasurement = dbService.insertMeasurement(m1);
		
		Measurement readMeasurement = dbService.getMeasurement(insertedMeasurement.getStationId(), 
					insertedMeasurement.getMeasurementPointNumber(), 
					insertedMeasurement.getMeasurementTypeId(), 
					insertedMeasurement.getMeasurementDateTime());
		
		date1 = readMeasurement.getMeasurementDateTime();
		
		assertEquals(m1.getResult(), readMeasurement.getResult());
		assertNotNull(date1);
		
		int nrMeas1 = dbService.countAllMeasurements();
		
		assertEquals(nrMeas0 + 1, nrMeas1);
	}
	
	/**
	 * Adding existing measurement again will perform an update (new record) and a WARN
	 * @param event
	 * @throws SQLException
	 */
	private void addExistingMeasurementByEvent(EventModel event) throws SQLException {
		
		Measurement measurement;
		
		try (MockedStatic<VandaHUtility> mockedStatic = mockStatic(VandaHUtility.class)) {
						
			measurement = dbService.addMeasurement(event);
			
			mockedStatic.verify(() -> VandaHUtility.logAndPrint(any(), eq(Level.WARN), eq(false), startsWith("Added existing measurement")), times(1));
		}
		
		int nrMeas = dbService.countMeasurementHistory(event.getStationId(), 
				event.getMeasurementPointNumber(), 
				measurement.getMeasurementTypeId(), 
				event.getMeasurementDateTime());
			
		assertEquals(2, nrMeas);
	}
	
	/**
	 * Adding a new measurement should just add a new record in DB
	 * @param event
	 * @throws SQLException
	 */
	private void addNewMeasurementByEvent(EventModel event) throws SQLException {
		
		Measurement measurement = dbService.addMeasurement(event);
			
		int nrMeas = dbService.countMeasurementHistory(measurement.getStationId(), 
				measurement.getMeasurementPointNumber(), 
				measurement.getMeasurementTypeId(), 
				dtNow);
			
		assertEquals(1, nrMeas);
		
		nrMeas = dbService.countMeasurementHistory(measurement.getStationId(), 
				measurement.getMeasurementPointNumber(), 
				measurement.getMeasurementTypeId(), 
				dt5MinAgo);
			
		assertEquals(2, nrMeas);
	}
	
	private void addMissingMeasurementTypeByEvent(EventModel event) {
				
		assertThrows(SQLException.class, () -> dbService.addMeasurement(event));
	}
	
	private void updateExistingMeasurement(EventModel event) {
		
		Measurement measurement = dbService.updateMeasurement(event);
		
		int nrMeas = dbService.countMeasurementHistory(measurement.getStationId(), 
				measurement.getMeasurementPointNumber(), 
				measurement.getMeasurementTypeId(), 
				dtNow);
			
		assertEquals(1, nrMeas);
		
		nrMeas = dbService.countMeasurementHistory(measurement.getStationId(), 
				measurement.getMeasurementPointNumber(), 
				measurement.getMeasurementTypeId(), 
				dt5MinAgo);
			
		assertEquals(3, nrMeas);
		
	}
	
	private void updateNonexistingMeasurement(EventModel event) {
		Measurement measurement;
		
		try (MockedStatic<VandaHUtility> mockedStatic = mockStatic(VandaHUtility.class)) {
			
			measurement = dbService.updateMeasurement(event);
			
			mockedStatic.verify(() -> VandaHUtility.logAndPrint(any(), eq(Level.WARN), eq(false), startsWith("Update on nonexistent measurement")), times(1));
		}
		
		assertNull(measurement);
				
		int nrMeas = dbService.countMeasurementHistory(station1.getStationId(), 
				measurementPoint1, 
				mt1.getMeasurementTypeId(), //we know it should be this one 
				dtNow);
					
		assertEquals(1, nrMeas);
		
		nrMeas = dbService.countMeasurementHistory(station1.getStationId(), 
				measurementPoint1, 
				mt1.getMeasurementTypeId(), //we know it should be this one 
				dt5MinAgo);
			
		assertEquals(3, nrMeas);
		
	}
	
	private void deleteExistingMeasurement(EventModel event) {
		
		Measurement measurementBefore = dbService.getMeasurement(event.getStationId(), 
				event.getMeasurementPointNumber(), 
				mt1.getMeasurementTypeId(), event.getMeasurementDateTime());
		
		assertTrue(measurementBefore.getIsCurrent());
		
		dbService.deleteMeasurement(event);
		
		List<Measurement> measurementHistory = dbService.getMeasurementHistory(measurementBefore.getStationId(), 
				measurementBefore.getMeasurementPointNumber(), 
				measurementBefore.getMeasurementTypeId(), measurementBefore.getMeasurementDateTime());
		
		measurementHistory.stream().forEach(m -> assertFalse(m.getIsCurrent()));
				
		int nrMeas = dbService.countMeasurementHistory(station1.getStationId(), 
				measurementPoint1, 
				mt1.getMeasurementTypeId(), //we know it should be this one 
				dtNow);
							
		assertEquals(1, nrMeas);
				
		nrMeas = dbService.countMeasurementHistory(station1.getStationId(), 
				measurementPoint1, 
				mt1.getMeasurementTypeId(), //we know it should be this one 
				dt5MinAgo);
					
		assertEquals(3, nrMeas);
	}
	
	private void deleteNonexistingMeasurement(EventModel event) {
		
		try (MockedStatic<VandaHUtility> mockedStatic = mockStatic(VandaHUtility.class)) {
			
			dbService.deleteMeasurement(event);
			
			mockedStatic.verify(() -> VandaHUtility.logAndPrint(any(), eq(Level.WARN), eq(false), startsWith("Delete of nonexistent measurement")), times(1));
		}
				
		int nrMeas = dbService.countMeasurementHistory(station1.getStationId(), 
				measurementPoint1, 
				mt1.getMeasurementTypeId(), //we know it should be this one 
				dtNow);
					
		assertEquals(1, nrMeas);
		
		nrMeas = dbService.countMeasurementHistory(station1.getStationId(), 
				measurementPoint1, 
				mt1.getMeasurementTypeId(), //we know it should be this one 
				dt5MinAgo);
			
		assertEquals(3, nrMeas);
		
	}
	
	
	
	private void testDeleteStation() {
		dbService.deleteStation(stationId);
		
		Station station = dbService.getStation(stationId);
		
		assertNull(station); 
	}
	
	private void testDeleteMeasurementType() {
		
		if (removeInsertedMeasurementTypes) {
			dbService.deleteMeasurementType(mtId1);
			
			MeasurementType mt = dbService.getMeasurementType(mtId1);
			assertNull(mt);
			
			dbService.deleteMeasurementType(mtId2);
			
			mt = dbService.getMeasurementType(mtId2);
			assertNull(mt);
		}
	}
	
	private void testDeleteMeasurement() {
		
		Measurement m;
		
		if (date1 != null) {
		
			dbService.deleteHardMeasurement(stationId);
			
			m = dbService.getMeasurement(stationId, measurementPoint1, mtId1, date1);
			assertNull(m);
		}
	}
}
