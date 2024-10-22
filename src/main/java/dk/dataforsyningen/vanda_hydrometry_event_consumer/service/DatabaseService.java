package dk.dataforsyningen.vanda_hydrometry_event_consumer.service;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import dk.dataforsyningen.vanda_hydrometry_event_consumer.dao.MeasurementDao;
import dk.dataforsyningen.vanda_hydrometry_event_consumer.dao.MeasurementTypeDao;
import dk.dataforsyningen.vanda_hydrometry_event_consumer.dao.StationDao;
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

	private StationDao stationDao;
	private MeasurementDao measurementDao;
	private MeasurementTypeDao measurementTypeDao;
	
	public DatabaseService(StationDao stationDao, MeasurementDao measurementDao, MeasurementTypeDao measurementTypeDao) {
		this.stationDao = stationDao;
		this.measurementDao = measurementDao;
		this.measurementTypeDao = measurementTypeDao;
	}
	
	/* STATION */
	
	public List<Station> getAllStations() {
		List<Station> stationsAndMeasurementTypes = stationDao.getAllStations();
		
		//merge on same station
		HashMap<String, Station> stations = new HashMap<>();
		for(Station s : stationsAndMeasurementTypes) {
			if (!stations.containsKey(s.getStationId())) {
				stations.put(s.getStationId(), s);
			} else {
				Station station = stations.get(s.getStationId());
				if (s.getMeasurementTypes().size() > 0) {
					//after mapping from DB the station objects will only contain one (or none) measurements
					station.getMeasurementTypes().add(s.getMeasurementTypes().getFirst()); 
				}
			}
		}
		
		return new ArrayList<Station>(stations.values());
	}
	
	public List<Station> getAllStationsByMeasurementType(int examinationTypeSc) {
		List<Station> stationsAndMeasurementTypes = stationDao.findStationByExaminationTypeSc(examinationTypeSc);
		
		//merge on same station
		HashMap<String, Station> stations = new HashMap<>();
		for(Station s : stationsAndMeasurementTypes) {
			if (!stations.containsKey(s.getStationId())) {
				stations.put(s.getStationId(), s);
			} else {
				Station station = stations.get(s.getStationId());
				if (s.getMeasurementTypes().size() > 0) {
					//after mapping from DB the station objects will only contain one (or none) measurements
					station.getMeasurementTypes().add(s.getMeasurementTypes().getFirst()); 
				}
			}
		}
		
		return new ArrayList<Station>(stations.values());
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
	
	public boolean isMeasurementSupported(String stationId, int examinationTypeSc) {
		return stationDao.isExaminationTypeScSupported(stationId, examinationTypeSc) > 0;
	}
	
	/**
	 * Inserts stations if it does not exist or updates it otherwise.
	 * @param stations list
	 */
	@Transactional
	public void saveStations(List<Station> stations) {
		//add/update station
		stationDao.addStations(stations);
		
		//save measurement types
		addMeasurementTypes(stations.stream().flatMap(station -> station.getMeasurementTypes().stream()).collect(Collectors.toList()));
		
		//save station <-> measurement_type relation if it does not exist
		stations.stream().forEach(
				station -> {
					ArrayList<MeasurementType> measurementTypes = station.getMeasurementTypes();
					if (measurementTypes != null) {
						stationDao.addStationMeasurementTypeRelations(measurementTypes.stream().map(mt -> station.getStationId()).toList(), measurementTypes);
					}
				}
				);
	}
	
	/**
	 * Insert station if it does not exist or updates it otherwise.
	 * @param station
	 */
	@Transactional
	public void saveStation(Station station) {
		//add/update station
		stationDao.addStation(station);
		
		//save measurement types
		addMeasurementTypes(station.getMeasurementTypes());
				
		//save station <-> measurement_type relation if it does not exist
		ArrayList<MeasurementType> measurementTypes = station.getMeasurementTypes();
		if (measurementTypes != null) {
			stationDao.addStationMeasurementTypeRelations(measurementTypes.stream().map(mt -> station.getStationId()).toList(), measurementTypes);
		}
	}
	
	public void deleteStation(String id) {
		stationDao.deleteRelationToMeasurementTypeByStationId(id);
		stationDao.deleteStation(id);
	}
	
	public void deleteStationMeasurementTypeRelation(String id) {
		stationDao.deleteRelationToMeasurementTypeByStationId(id);
	}
	
	public int countStations() {
		return stationDao.count();
	}

	/* MEASUREMENT */
	
	public List<Measurement> getAllMeasurements() {
		return measurementDao.getAllMeasurements();
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
	 * Inserts measurements if they do not exist or updates them otherwise.
	 * @param measurements list
	 */
	@Transactional
	public void saveMeasurements(List<Measurement> measurements) {
		//do an update in case they exist
		measurementDao.updateMeasurements(measurements);
		
		//add the measurements if they are missing
		measurementDao.addMeasurements(measurements);
	}
	
	/**
	 * Tries to update the result of a measurement if the measurement exists in which case it returns null.
	 * 
	 * If it does not exist it will be inserted and the measurement returned.
	 * 
	 * @param measurement
	 * @return inserted measurement or null
	 */
	@Transactional
	public Measurement saveMeasurement(Measurement measurement) {
		//do an update in case it exists
		measurementDao.updateMeasurement(measurement);
				
		//add the measurement if is is missing
		return measurementDao.addMeasurement(measurement);
	}
	
	public void deleteMeasurement(String stationId, int measurementPointNumber,
			String measurementTypeId,
			OffsetDateTime measurementDatetime
			) {
		measurementDao.deleteMeasurement(stationId, measurementPointNumber, measurementTypeId, measurementDatetime);
	}
	
	public int countMeasurements() {
		return measurementDao.count();
	}
	
	/* MEASUREMENT TYPE */
	
	public List<MeasurementType> getAllMeasurementTypes() {
		return measurementTypeDao.getAllMeasurementTypes();
	}
	
	public MeasurementType getMeasurementType(String id) {
		return measurementTypeDao.findMeasurementTypeById(id);
	}
	
	/**
	 * Inserts (if it does not exist) or updates the measurement type.
	 * @param measurementType
	 */
	@Transactional
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
	
	public void deleteMeasurementType(String id) {
		measurementTypeDao.deleteMeasurementType(id);
	}
	
	public int countMeasurementTypes() {
		return measurementTypeDao.count();
	}
	
}
