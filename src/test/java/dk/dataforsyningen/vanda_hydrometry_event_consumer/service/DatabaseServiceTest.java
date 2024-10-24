package dk.dataforsyningen.vanda_hydrometry_event_consumer.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;

import java.sql.SQLException;
import java.time.OffsetDateTime;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.slf4j.event.Level;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import dk.dataforsyningen.vanda_hydrometry_event_consumer.VandaHUtility;
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
	private final int measurementPoint2 = 2;
	private final double result1 = 12.34;
	private final double result2 = 56.78;
	
	private Station station1;
	private MeasurementType mt1;
	private MeasurementType mt2;
	private Measurement m1;
	private Measurement m2;
	private OffsetDateTime dt5MinAgo;
	private OffsetDateTime dtNow;
	private OffsetDateTime date1;
	private OffsetDateTime date2;
	private EventModel eventAdd;
	private EventModel eventUpdate;
	private EventModel eventDelete;
	
	private boolean removeInsertedMeasurementTypes = true;
	
	private boolean enableTest = true; //TODO get from config
	
	@Autowired
	private DatabaseService dbService;
	
	@BeforeEach
	public void setup() {
		
		//enableTest = config.isEnableTest();
		
		System.out.println("Database testing " + (enableTest ? "enabled" : "disabled"));
		if (!enableTest) return;
		
		dtNow = VandaHUtility.parseForAPI(OffsetDateTime.now().toString());
		dt5MinAgo = VandaHUtility.parseForAPI(dtNow.minusMinutes(5).toString());
		
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
		
		m2 = new Measurement();
		m2.setStationId(stationId);
		m2.setIsCurrent(true);
		m2.setMeasurementPointNumber(measurementPoint2);
		m2.setResult(result2);
		m2.setMeasurementTypeId("fail");
		m2.setMeasurementDateTime(dt5MinAgo);
		
		eventAdd = new EventModel();
		
		eventAdd.setEventType(VandaHEventProcessor.EVENT_MEASUREMENT_ADDED);
		eventAdd.setStationId(stationId);
		eventAdd.setMeasurementPointNumber(measurementPoint1);
		eventAdd.setUnitSc(mtUnitSc1);
		eventAdd.setParameterSc(mtParamSc1);
		eventAdd.setExaminationTypeSc(mtExamTypeSc1);
		eventAdd.setResult(result1);
		eventAdd.setMeasurementDateTime(dt5MinAgo);
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
		
		addStations();
		
		addMeasurement();
		
		addExistingMeasurementByEvent();
		
		addNewMeasurementByEvent();
		
		addMissingMeasurementTypeByEvent();
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
	
	private void addExistingMeasurementByEvent() throws SQLException {
		
		Measurement measurement;
		
		try (MockedStatic<VandaHUtility> mockedStatic = mockStatic(VandaHUtility.class)) {
						
			measurement = dbService.addMeasurement(eventAdd);
			
			mockedStatic.verify(() -> VandaHUtility.logAndPrint(any(), eq(Level.WARN), eq(false), startsWith("Added existing measurement")), times(1));
		}
		
		int nrMeas = dbService.countMeasurementHistory(eventAdd.getStationId(), 
				eventAdd.getMeasurementPointNumber(), 
				measurement.getMeasurementTypeId(), 
				eventAdd.getMeasurementDateTime());
			
		assertEquals(2, nrMeas);
	}
	
	private void addNewMeasurementByEvent() throws SQLException {
		
		eventAdd.setMeasurementDateTime(dtNow);
		
		Measurement measurement = dbService.addMeasurement(eventAdd);
			
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
	
	private void addMissingMeasurementTypeByEvent() throws SQLException {
		//TODO implement
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
