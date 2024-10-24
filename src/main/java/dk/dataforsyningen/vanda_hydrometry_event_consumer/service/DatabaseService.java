package dk.dataforsyningen.vanda_hydrometry_event_consumer.service;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import dk.dataforsyningen.vanda_hydrometry_event_consumer.VandaHUtility;
import dk.dataforsyningen.vanda_hydrometry_event_consumer.dao.MeasurementDao;
import dk.dataforsyningen.vanda_hydrometry_event_consumer.dao.MeasurementTypeDao;
import dk.dataforsyningen.vanda_hydrometry_event_consumer.dao.StationDao;
import dk.dataforsyningen.vanda_hydrometry_event_consumer.model.EventModel;
import dk.dataforsyningen.vanda_hydrometry_event_consumer.model.Measurement;
import dk.dataforsyningen.vanda_hydrometry_event_consumer.model.MeasurementType;
import dk.dataforsyningen.vanda_hydrometry_event_consumer.model.Station;

/**
 * Service class providing DAO access.
 * 
 * @author Radu Dudici
 */
@Service
public class DatabaseService {

	private final Logger log = LoggerFactory.getLogger(DatabaseService.class);
	
	private StationDao stationDao;
	private MeasurementDao measurementDao;
	private MeasurementTypeDao measurementTypeDao;
	
	public DatabaseService(StationDao stationDao, MeasurementDao measurementDao, MeasurementTypeDao measurementTypeDao) {
		this.stationDao = stationDao;
		this.measurementDao = measurementDao;
		this.measurementTypeDao = measurementTypeDao;
	}

	
	/**
	 * Get measurement history
	 * 
	 * @param stationId
	 * @param measurementPointNumber
	 * @param measurementTypeId
	 * @param measurementDatetime
	 * @return list of measurement history for the given measurement
	 */
	public List<Measurement> getMeasurementHistory(String stationId,
			int measurementPointNumber,
			String measurementTypeId,
			OffsetDateTime measurementDatetime) {
		return measurementDao.getMeasurementHistory(stationId, measurementPointNumber, measurementTypeId, measurementDatetime);
	}

	/**
	 * Returns the active (there should be only one) measurement matching the given parameters.
	 * 
	 * @param stationId
	 * @param measurementPointNumber
	 * @param measurementTypeId
	 * @param measurementDatetime
	 * @return Measurement
	 */
	public Measurement getMeasurement(String stationId,
			int measurementPointNumber,
			String measurementTypeId,
			OffsetDateTime measurementDatetime) {
		return measurementDao.findCurrentMeasurement(stationId, measurementPointNumber, measurementTypeId, measurementDatetime);
	}
	
	/**
	 * Performs the following operations:
	 * 
	 * converts event to a measurement
	 * tries to inactivate previous versions of this measurement, there should be none
	 * check measurement type if it exists
	 * add active measurement
	 * 
	 * @param event
	 * @return inserted measurement or null
	 * @throws SQLException 
	 */
	@Transactional
	public Measurement addMeasurement(EventModel event) throws SQLException {
		
		Measurement measurement = Measurement.from(event);
		
		//check measurement type
		MeasurementType measurementType = measurementTypeDao.findMeasurementTypeById(measurement.getMeasurementTypeId());
				
		if (measurementType != null) {
					
			//inactivate previous versions
			int nr = measurementDao.inactivateMeasurement(measurement);
			
			if (nr > 0) {
				VandaHUtility.logAndPrint(log, Level.WARN, false, "Added existing measurement: " + measurement);
			}
			
			measurement.setIsCurrent(true); //make sure this will be the current measurement
			
			//add the new measurement
			return measurementDao.insertMeasurement(measurement);
			
		} else {
			throw new SQLException("The measurement type does not exist " + measurement.getMeasurementTypeId() + ". No insertion!");
		}
	}
	
	
	/**
	 * Performs the following operations:
	 * 
	 * converts event to a measurement
	 * read measurement type from current value, there should be one otherwise it fails
	 * inactivate previous versions of this measurement
	 * add active measurement
	 * 
	 * @param event
	 * @return inserted measurement or null
	 * @throws SQLException 
	 */
	@Transactional
	public Measurement updateMeasurement(EventModel event) {
		
		Measurement measurement = Measurement.from(event);
		
		Measurement oldMeasurement = measurementDao.findCurrentMeasurement(event.getStationId(), 
					event.getMeasurementPointNumber(), 
					event.getExaminationTypeSc(), event.getMeasurementDateTime());
						
		if (oldMeasurement != null) {
		
			measurement.setMeasurementTypeId(oldMeasurement.getMeasurementTypeId());
			
			//inactivate previous versions
			measurementDao.inactivateMeasurement(oldMeasurement);
						
			measurement.setIsCurrent(true); //make sure this will be the current measurement
			
			//add the new measurement
			return measurementDao.insertMeasurement(measurement);
			
		} else {
			//throw new SQLException("Update on nonexistent measurement " + measurement + ". No update.");
			//maybe we only want a warning but continue with the other events
			VandaHUtility.logAndPrint(log, Level.WARN, false, "Update on nonexistent measurement " + measurement + ". No update!");			
		}
		return null;
	}
	
	
	/**
	 * Performs the following operations:
	 * 
	 * converts event to a measurement
	 * read measurement type from current value, there should be one otherwise it fails
	 * inactivate previous versions of this measurement
	 * add active measurement
	 * 
	 * @param event
	 * @return inserted measurement or null
	 * @throws SQLException 
	 */
	@Transactional
	public Measurement deleteMeasurement(EventModel event) {
		
		Measurement measurement = Measurement.from(event);
		
		Measurement oldMeasurement = measurementDao.findCurrentMeasurement(event.getStationId(), 
					event.getMeasurementPointNumber(), 
					event.getExaminationTypeSc(), event.getMeasurementDateTime());
						
		if (oldMeasurement != null) {
		
			measurement.setMeasurementTypeId(oldMeasurement.getMeasurementTypeId());
			
			//inactivate previous versions
			measurementDao.inactivateMeasurement(oldMeasurement);
						
			measurement.setIsCurrent(true); //make sure this will be the current measurement
			
			//add the new measurement
			return measurementDao.insertMeasurement(measurement);
			
		} else {
			//throw new SQLException("Delete of nonexistent measurement " + measurement + ". No deletion!");
			//maybe we only want a warning but continue with the other events
			VandaHUtility.logAndPrint(log, Level.WARN, false, "Delete of nonexistent measurement " + measurement + ". No deletion!");			
		}
		return null;
	}
	
	public Station getStation(String id) {
		List<Station> stationsAndMeasurementTypes = stationDao.findStationByStationId(id);
		
		//merge into one station
		Station station = null;
		for(Station s : stationsAndMeasurementTypes) {
			if (station == null) {
				station = s;
			} else {
				assert Objects.equals(station.getStationId(), s.getStationId());
				if (s.getMeasurementTypes().size() > 0) {
					//after mapping from DB the station objects will only contain one (or none) measurements
					station.getMeasurementTypes().add(s.getMeasurementTypes().getFirst()); 
				}
			}
		}
	
		return station;
	}
	
	/**
	 * Insert station if it does not exist or updates it otherwise.
	 * @param station
	 */
	@Transactional
	public void addStation(Station station) {
		//add/update station
		stationDao.addStation(station);
		
		//save measurement types
		measurementTypeDao.addMeasurementTypes(station.getMeasurementTypes());
				
		//save station <-> measurement_type relation if it does not exist
		ArrayList<MeasurementType> measurementTypes = station.getMeasurementTypes();
		if (measurementTypes != null) {
			stationDao.addStationMeasurementTypeRelations(measurementTypes.stream().map(mt -> station.getStationId()).toList(), measurementTypes);
		}
	}
	
	@Transactional
	public void deleteStation(String id) {
		stationDao.deleteRelationToMeasurementTypeByStationId(id);
		stationDao.deleteStation(id);
	}
	
	public MeasurementType getMeasurementType(String id) {
		return measurementTypeDao.findMeasurementTypeById(id);
	}
	
	public void addMeasurementType(MeasurementType measurementType) {
		measurementTypeDao.addMeasurementType(measurementType);
	}
	
	/**
	 * Inserts (if it does not exist) or update the measurement from the given list.
	 * @param measurementTypes list
	 */
	@Transactional
	public void addMeasurementTypes(List<MeasurementType> measurementTypes) {
		measurementTypeDao.addMeasurementTypes(measurementTypes);
	}
	
	public int deleteMeasurementType(String measurementTypeId) {
		return measurementTypeDao.deleteMeasurementType(measurementTypeId);
	}
	
	/**
	 * Inserts measurement.
	 * @param measurements list
	 */
	public Measurement insertMeasurement(Measurement measurement) {
		return measurementDao.insertMeasurement(measurement);
	}
	
	public void inactivateMeasurement(Measurement measurement) {
		measurementDao.inactivateMeasurement(measurement);
	}
	
	
	public void deleteHardMeasurement(String stationId, int measurementPointNumber,
			String measurementTypeId, OffsetDateTime measurementDatetime
			) {
		measurementDao.deleteMeasurement(stationId, measurementPointNumber, measurementTypeId, measurementDatetime);
	}
	
	public void deleteHardMeasurement(String stationId) {
		measurementDao.deleteMeasurement(stationId);
	}
	
	public int countMeasurementHistory(String stationId, int measurementPointNumber,
			String measurementTypeId, OffsetDateTime measurementDatetime) {
		return measurementDao.countHistory(stationId, measurementPointNumber, measurementTypeId, measurementDatetime);
	}
	
	public int countAllMeasurements() {
		return measurementDao.countAll();
	}
	
	public int countAllMeasurementTypes() {
		return measurementTypeDao.count();
	}
	
	public int countAllStations() {
		return stationDao.count();
	}
	
}

/*
[lastEventType=MeasurementAdded, 
	stationId=47001196, 
	operatorStationId=null, 
	measurementPointNumber=1, 
	unitSc=19, 
	parameterSc=1233, 
	examinationTypeSc=25, 
	reasonCodeSc=0, 
	result=-2.4, 
	measurementDateTime=2024-10-12T00:30Z, 
	recordDateTime=2024-10-12T11:00:48.885Z, 
	offset=15586198, 
]

[lastEventType=MeasurementUpdated, 
	stationId=70000275, 
	operatorStationId=WATSONC-1519, 
	measurementPointNumber=1, 
	unitSc=0, 
	parameterSc=0, 
	examinationTypeSc=25, 
	reasonCodeSc=5, 
	result=78.3, 
	measurementDateTime=2024-10-06T05:25Z, 
	recordDateTime=2024-10-06T07:03:02.797Z, 
]
 
 */