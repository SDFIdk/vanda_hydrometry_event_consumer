package dk.dataforsyningen.vanda_hydrometry_event_consumer.dao;

import dk.dataforsyningen.vanda_hydrometry_event_consumer.config.LogSqlFactory;
import dk.dataforsyningen.vanda_hydrometry_event_consumer.mapper.MeasurementMapper;
import dk.dataforsyningen.vanda_hydrometry_event_consumer.model.Measurement;
import java.time.OffsetDateTime;
import java.util.List;
import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindBean;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

@LogSqlFactory
public interface MeasurementDao {

  @SqlQuery("""
      select
      	station_id,
      	measurement_point_number,
      	measurement_date_time,
      	vanda_event_timestamp,
      	examination_type_sc,
      	result,
      	result_elevation_corrected,
      	is_current,
      	created
      from hydrometry.measurement
      where
      	station_id = :stationId
      	and examination_type_sc = :examinationTypeSc
      	and measurement_point_number = :measurementPointNumber
      	and measurement_date_time = :measurementDateTime
      order by created
      """)
  @RegisterRowMapper(MeasurementMapper.class)
  List<Measurement> readMeasurementHistory(@Bind String stationId,
                                           @Bind int measurementPointNumber,
                                           @Bind int examinationTypeSc,
                                           @Bind OffsetDateTime measurementDateTime
  );

  @SqlQuery("""
      select
      	station_id,
      	measurement_point_number,
      	measurement_date_time,
      	vanda_event_timestamp,
      	examination_type_sc,
      	result,
      	result_elevation_corrected,
      	is_current,
      	created
      from hydrometry.measurement
      where
      	station_id = :stationId
      	and examination_type_sc = :examinationTypeSc
      	and measurement_point_number = :measurementPointNumber
      	and measurement_date_time = :measurementDateTime
      	and is_current = true
      """)
  @RegisterRowMapper(MeasurementMapper.class)
  Measurement readCurrentMeasurement(@Bind String stationId,
                                     @Bind int measurementPointNumber,
                                     @Bind int examinationTypeSc,
                                     @Bind OffsetDateTime measurementDateTime
  );

  @SqlQuery("""
      select count(*) > 0
      from hydrometry.measurement
      where
      	station_id = :stationId
      	and examination_type_sc = :examinationTypeSc
      	and measurement_point_number = :measurementPointNumber
      	and measurement_date_time = :measurementDateTime
      	and ((vanda_event_timestamp is not null and vanda_event_timestamp >= :eventTimestamp)
      		or (vanda_event_timestamp is null and created >= :eventTimestamp))
      """)
  boolean isEventDelayed(@Bind OffsetDateTime eventTimestamp,
                         @Bind String stationId,
                         @Bind int measurementPointNumber,
                         @Bind int examinationTypeSc,
                         @Bind OffsetDateTime measurementDateTime
  );

  /**
   * Add a new record for the given measurement if its timestamp is null
   * or newer than the timestamp of the current record
   *
   * @param measurement
   */
  @SqlQuery("""
      insert into hydrometry.measurement (station_id, measurement_date_time, vanda_event_timestamp, measurement_point_number, examination_type_sc, result, result_elevation_corrected, is_current, created)
      values (:stationId, :measurementDateTime, :vandaEventTimestamp, :measurementPointNumber, :examinationTypeSc, :result, :resultElevationCorrected, :isCurrent, now())
      returning *
      """)
  @RegisterRowMapper(MeasurementMapper.class)
  Measurement insertMeasurement(@BindBean Measurement measurement);


  /**
   * Set is_current to false on all records from the given measurement
   * (all records in the given measurement's history)
   *
   * @param measurement
   */
  @SqlUpdate("""
      update hydrometry.measurement set is_current = false
      where
      	station_id = :stationId
      	and measurement_date_time = :measurementDateTime
      	and measurement_point_number = :measurementPointNumber
      	and examination_type_sc = :examinationTypeSc
      """)
  int inactivateMeasurementHistory(@BindBean Measurement measurement);

  /**
   * Deletes all measurements related to the given station
   *
   * @param stationId
   */
  @SqlUpdate("""
      delete
      from hydrometry.measurement
      where
      	station_id = :stationId
      """)
  void deleteMeasurementsForStation(@Bind String stationId);

  /**
   * Counts the number of records in the given measurement's history
   *
   * @param stationId
   * @param measurementPointNumber
   * @param examinationTypeSc
   * @param measurementDateTime
   * @return number of records
   */
  @SqlQuery("""
      select count(*) from hydrometry.measurement
      where
      	station_id = :stationId
      	and measurement_date_time = :measurementDateTime
      	and measurement_point_number = :measurementPointNumber
      	and examination_type_sc = :examinationTypeSc
      """)
  int countHistory(@Bind String stationId, @Bind int measurementPointNumber,
                   @Bind int examinationTypeSc,
                   @Bind OffsetDateTime measurementDateTime);
}
