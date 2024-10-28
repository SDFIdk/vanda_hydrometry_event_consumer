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
	 * Get measurement history, i.e. all records about the requested measurement
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
	 * Returns the current (there should be only one) measurement record matching the given parameters.
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
	 * - converts event to a measurement
	 * - check measurement type if it exists otherwise fail
	 * - tries to inactivate previous versions of this measurement, there should be none otherwise WARN
	 * - add current measurement
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
	 * - converts event to a measurement
	 * - read measurement type from current value, there should be one otherwise WARN
	 * - inactivate previous versions of this measurement
	 * - add current measurement
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
			VandaHUtility.logAndPrint(log, Level.WARN, false, "Update on nonexistent measurement (examinationType=" + event.getExaminationTypeSc() + ") " + measurement + ". No update!");			
		}
		return null;
	}
	
	
	/**
	 * Performs the following operations:
	 * 
	 * - converts event to a measurement
	 * - read measurement type from current value, there should be one otherwise WARN
	 * - inactivate previous (all) versions of this measurement
	 * 
	 * @param event
	 * @return inserted measurement or null
	 * @throws SQLException 
	 */
	@Transactional
	public void deleteMeasurement(EventModel event) {
		
		Measurement measurement = Measurement.from(event);
		
		Measurement oldMeasurement = measurementDao.findCurrentMeasurement(event.getStationId(), 
					event.getMeasurementPointNumber(), 
					event.getExaminationTypeSc(), event.getMeasurementDateTime());
						
		if (oldMeasurement != null) {
		
			measurement.setMeasurementTypeId(oldMeasurement.getMeasurementTypeId());
			
			//inactivate previous versions
			measurementDao.inactivateMeasurement(oldMeasurement);
									
		} else {
			//throw new SQLException("Delete of nonexistent measurement " + measurement + ". No deletion!");
			//maybe we only want a warning but continue with the other events
			VandaHUtility.logAndPrint(log, Level.WARN, false, "Delete of nonexistent measurement (examinationType=" + event.getExaminationTypeSc() + ") " + measurement + ". No deletion!");			
		}
		return;
	}
	
	/**
	 * Get station with the given station id.
	 * used in testing.
	 * 
	 * @param id
	 * @return Station or null
	 */
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
	 * Adds station if it does not exist or updates it otherwise.
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
	
	/**
	 * Deletes station from DB
	 * 
	 * @param id
	 */
	@Transactional
	public void deleteStation(String id) {
		stationDao.deleteRelationToMeasurementTypeByStationId(id);
		stationDao.deleteStation(id);
	}
	
	/**
	 * Read the measurement type from DB
	 * @param id
	 * @return
	 */
	public MeasurementType getMeasurementType(String id) {
		return measurementTypeDao.findMeasurementTypeById(id);
	}
	
	/**
	 * Adds measurement type into DB if it does not exist
	 * @param measurementType
	 */
	public void addMeasurementType(MeasurementType measurementType) {
		measurementTypeDao.addMeasurementType(measurementType);
	}
	
	/**
	 * Inserts (if it does not exist) or update the measurement types from the given list.
	 * @param measurementTypes list
	 */
	@Transactional
	public void addMeasurementTypes(List<MeasurementType> measurementTypes) {
		measurementTypeDao.addMeasurementTypes(measurementTypes);
	}
	
	/**
	 * Deletes the measurement type with the given id
	 * @param measurementTypeId
	 * @return
	 */
	public int deleteMeasurementType(String measurementTypeId) {
		return measurementTypeDao.deleteMeasurementType(measurementTypeId);
	}
	
	/**
	 * Inserts measurement into DB.
	 * @param measurements list
	 */
	public Measurement insertMeasurement(Measurement measurement) {
		return measurementDao.insertMeasurement(measurement);
	}
	
	/**
	 * Set isCurrent=false to all records belonging to the given measurement
	 * @param measurement
	 */
	public void inactivateMeasurement(Measurement measurement) {
		measurementDao.inactivateMeasurement(measurement);
	}
	
	/**
	 * Completely deletes the given measurement (and its history, i.e. all related records) from the DB.
	 * Used for testing.
	 * @param stationId
	 * @param measurementPointNumber
	 * @param measurementTypeId
	 * @param measurementDatetime
	 */
	public void deleteHardMeasurement(String stationId, int measurementPointNumber,
			String measurementTypeId, OffsetDateTime measurementDatetime
			) {
		measurementDao.deleteMeasurement(stationId, measurementPointNumber, measurementTypeId, measurementDatetime);
	}
	
	/**
	 * Completely deletes all the measurements from the given station (and its history, i.e. all related records) from the DB.
	 * Used for testing.
	 * @param stationId
	 */
	public void deleteHardMeasurement(String stationId) {
		measurementDao.deleteMeasurement(stationId);
	}

	/**
	 * Counts how many records (history) does the given measurement has.
	 * @param stationId
	 * @param measurementPointNumber
	 * @param measurementTypeId
	 * @param measurementDatetime
	 * @return nr records
	 */
	public int countMeasurementHistory(String stationId, int measurementPointNumber,
			String measurementTypeId, OffsetDateTime measurementDatetime) {
		return measurementDao.countHistory(stationId, measurementPointNumber, measurementTypeId, measurementDatetime);
	}
	
	/**
	 * Counts all measurements from the DB (current and non current)
	 * @return
	 */
	public int countAllMeasurements() {
		return measurementDao.countAll();
	}
	
	/**
	 * Counts all measurement types.
	 * @return
	 */
	public int countAllMeasurementTypes() {
		return measurementTypeDao.count();
	}
	
	/**
	 * Counts all stations from the DB.
	 * @return
	 */
	public int countAllStations() {
		return stationDao.count();
	}
	
}

