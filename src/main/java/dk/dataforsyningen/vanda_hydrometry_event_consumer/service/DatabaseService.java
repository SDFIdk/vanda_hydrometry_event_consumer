package dk.dataforsyningen.vanda_hydrometry_event_consumer.service;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import dk.dataforsyningen.vanda_hydrometry_event_consumer.dao.MeasurementDao;
import dk.dataforsyningen.vanda_hydrometry_event_consumer.dao.MeasurementTypeDao;
import dk.dataforsyningen.vanda_hydrometry_event_consumer.dao.StationDao;
import dk.dataforsyningen.vanda_hydrometry_event_consumer.mapper.EventMeasurementMapper;
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

	private static Logger logger = LoggerFactory.getLogger(DatabaseService.class);
	
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
			int examinationTypeSc,
			OffsetDateTime measurementDatetime) {
		return measurementDao.readMeasurementHistory(stationId, measurementPointNumber, examinationTypeSc, measurementDatetime);
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
			int examinationTypeSc,
			OffsetDateTime measurementDatetime) {
		return measurementDao.readCurrentMeasurement(stationId, measurementPointNumber, examinationTypeSc, measurementDatetime);
	}
	
	/**
	 * Performs the following operations:
	 * 
	 * - converts event to a measurement
	 * - check if it is a delayed event
	 * - if it is then drop it and WARN
	 * - otherwise inactivate previous versions of this measurement, there should be none otherwise WARN
	 * - add current measurement as active
	 * 
	 * @param event
	 * @return inserted measurement or null
	 * @throws SQLException 
	 */
	@Transactional
	public Measurement addMeasurementFromEvent(EventModel event) throws SQLException {
		
		Measurement newMeasurement = null;
		
		Measurement measurement = EventMeasurementMapper.measurementFrom(event);
		
		boolean delayed = isEventDelayed(event);
		
		if (!delayed) {
			//inactivate previous versions
			int nr = measurementDao.inactivateMeasurementHistory(measurement);
				
			if (nr > 0) {
				logger.warn("Added existing measurement: " + measurement);
			}
			
			measurement.setIsCurrent(true); //make sure this will be the current measurement
				
			//add the new measurement
			newMeasurement = measurementDao.insertMeasurement(measurement);
		} else {
			logger.warn("Delayed event received and dropped: " + event);
		}
		
		return newMeasurement;
	}
	
	
	/**
	 * Performs the following operations:
	 * 
	 * - converts event to a measurement
	 * - check if it is a delayed event
	 * - if it is then drop it and WARN
	 * - otherwise inactivate previous versions of this measurement, there should be some otherwise WARN
	 * - add current measurement as active
	 * 
	 * @param event
	 * @return inserted measurement or null
	 * @throws SQLException 
	 */
	@Transactional
	public Measurement updateMeasurementFromEvent(EventModel event) throws SQLException {
		
		Measurement newMeasurement = null;
		
		Measurement measurement = EventMeasurementMapper.measurementFrom(event);
								
		boolean delayed = isEventDelayed(event);
		
		if (!delayed) {
			//inactivate previous versions
			int nr = measurementDao.inactivateMeasurementHistory(measurement);
			
			if (nr == 0) {	
				logger.warn("Update on nonexistent measurement " + measurement + ". Measurement inserted as new!");
			}
				
			measurement.setIsCurrent(true); //make sure this will be the current measurement
				
			//add the new measurement
			newMeasurement = measurementDao.insertMeasurement(measurement);
		
		} else {
			logger.warn("Delayed event received and dropped: " + event);
		}
		
		return newMeasurement;
	}
	
	
	/**
	 * Performs the following operations:
	 * 
	 * - converts event to a measurement
	 * - inactivate previous versions of this measurement, there should be some otherwise WARN
	 * - add current measurement as inactive
	 * 
	 * @param event
	 * @return inserted measurement or null
	 * @throws SQLException 
	 */
	@Transactional
	public Measurement deleteMeasurementFromEvent(EventModel event) {
		
		Measurement newMeasurement = null;
		
		Measurement measurement = EventMeasurementMapper.measurementFrom(event);
		
		//inactivate previous versions
		int nr = measurementDao.inactivateMeasurementHistory(measurement);						
		
		if (nr == 0) {
			logger.warn("Delete of nonexistent measurement (examinationType=" + event.getExaminationTypeSc() + ") " + measurement + ". No deletion!");			
		} else {
			//add the new measurement as not current so that the timestamp is saved
			measurement.setIsCurrent(false); //no record is current on deletion
			newMeasurement = measurementDao.insertMeasurement(measurement);
		}
		
		return newMeasurement;
	}
	
	private boolean isEventDelayed(EventModel event) {
		return measurementDao.isEventDelayed(event.getRecordDateTime(), 
				event.getStationId(), event.getMeasurementPointNumber(), 
				event.getExaminationTypeSc(), event.getMeasurementDateTime());
	}
	
	/**
	 * Get station with the given station id.
	 * used in testing.
	 * 
	 * @param id
	 * @return Station or null
	 */
	public Station getStation(String id) {
		List<Station> stationsAndMeasurementTypes = stationDao.readStationByStationId(id);
		
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
		stationDao.insertStation(station);
		
		//save measurement types
		measurementTypeDao.insertMeasurementTypes(station.getMeasurementTypes());
				
		//save station <-> measurement_type relation if it does not exist
		ArrayList<MeasurementType> measurementTypes = station.getMeasurementTypes();
		if (measurementTypes != null) {
			stationDao.insertStationMeasurementTypeRelations(measurementTypes.stream().map(mt -> station.getStationId()).toList(), measurementTypes);
		}
	}
	
	/**
	 * Deletes station from DB
	 * 
	 * @param id
	 */
	@Transactional
	public void deleteStation(String id) {
		stationDao.deleteRelationToMeasurementTypeByStation(id);
		stationDao.deleteStation(id);
	}
	
	public void deleteStationMeasurementTypeRelation(String id) {
		stationDao.deleteRelationToMeasurementTypeByStation(id);
	}
	
	/**
	 * Read the measurement type from DB
	 * @param id
	 * @return
	 */
	public MeasurementType getMeasurementType(int examinationTypeSc) {
		return measurementTypeDao.readMeasurementTypeByExaminationType(examinationTypeSc);
	}
	
	/**
	 * Adds measurement type into DB if it does not exist
	 * @param measurementType
	 */
	public void addMeasurementType(MeasurementType measurementType) {
		measurementTypeDao.insertMeasurementType(measurementType);
	}
	
	/**
	 * Inserts (if it does not exist) or update the measurement types from the given list.
	 * @param measurementTypes list
	 */
	@Transactional
	public void addMeasurementTypes(List<MeasurementType> measurementTypes) {
		measurementTypeDao.insertMeasurementTypes(measurementTypes);
	}
	
	/**
	 * Deletes the measurement type with the given id
	 * @param measurementTypeId
	 * @return
	 */
	public void deleteMeasurementType(int examinationTypeSc) {
		measurementTypeDao.deleteMeasurementType(examinationTypeSc);
	}
	
	/**
	 * Inserts measurement into DB.
	 * @param measurements list
	 */
	public Measurement addMeasurement(Measurement measurement) {
		return measurementDao.insertMeasurement(measurement);
	}
	
	/**
	 * Set isCurrent=false to all records belonging to the given measurement
	 * @param measurement
	 */
	public void inactivateMeasurementHistory(Measurement measurement) {
		measurementDao.inactivateMeasurementHistory(measurement);
	}
	
	/**
	 * Completely deletes the given measurement (and its history, i.e. all related records) from the DB.
	 * Used for testing.
	 * @param stationId
	 * @param measurementPointNumber
	 * @param examinationTypeSc
	 * @param measurementDatetime
	 */
	public void deleteMeasurementHard(String stationId, int measurementPointNumber,
			int examinationTypeSc, OffsetDateTime measurementDatetime
			) {
		measurementDao.deleteMeasurementWithHistory(stationId, measurementPointNumber, examinationTypeSc, measurementDatetime);
	}
	
	/**
	 * Completely deletes all the measurements from the given station (and its history, i.e. all related records) from the DB.
	 * Used for testing.
	 * @param stationId
	 */
	public void deleteMeasurementHard(String stationId) {
		measurementDao.deleteMeasurementsForStation(stationId);
	}

	/**
	 * Counts how many records (history) does the given measurement has.
	 * @param stationId
	 * @param measurementPointNumber
	 * @param examinationTypeSc
	 * @param measurementDatetime
	 * @return nr records
	 */
	public int countMeasurementHistory(String stationId, int measurementPointNumber,
			int examinationTypeSc, OffsetDateTime measurementDatetime) {
		return measurementDao.countHistory(stationId, measurementPointNumber, examinationTypeSc, measurementDatetime);
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

