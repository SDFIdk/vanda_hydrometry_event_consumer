package dk.dataforsyningen.vanda_hydrometry_event_consumer.dao;

import java.time.OffsetDateTime;
import java.util.List;

import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindBean;
import org.jdbi.v3.sqlobject.statement.SqlBatch;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import dk.dataforsyningen.vanda_hydrometry_event_consumer.config.LogSqlFactory;
import dk.dataforsyningen.vanda_hydrometry_event_consumer.model.Measurement;


@RegisterRowMapper(MeasurementMapper.class)
@LogSqlFactory
public interface MeasurementDao {

	@SqlQuery("""
			select
				station_id,
				measurement_point_number,
				measurement_date_time,
				measurement_type_id,
				result,
				is_current,
				created
			from hydrometry.measurement
			where
				station_id = :stationId
				and measurement_type_id = :measurementTypeId
				and measurement_point_number = :measurementPointNumber
				and measurement_date_time = :measurementDateTime
			""")
	List<Measurement> getMeasurementHistory(@Bind String stationId,
			@Bind int measurementPointNumber,
			@Bind String measurementTypeId,
			@Bind OffsetDateTime measurementDateTime);
	
	@SqlQuery("""
			select
				station_id,
				measurement_point_number,
				measurement_date_time,
				measurement_type_id,
				result,
				is_current,
				created
			from hydrometry.measurement
			where
				station_id = :stationId
				and measurement_type_id = :measurementTypeId
				and measurement_point_number = :measurementPointNumber
				and measurement_date_time = :measurementDateTime
				and is_current = true
			""")
	Measurement findCurrentMeasurement(@Bind String stationId,
			@Bind int measurementPointNumber,
			@Bind String measurementTypeId,
			@Bind OffsetDateTime measurementDateTime
			);
	
	@SqlQuery("""
			select
				station_id,
				measurement_point_number,
				measurement_date_time,
				measurement_type_id,
				result,
				is_current,
				created
			from hydrometry.measurement
			where
				station_id = :stationId
				and measurement_type_id like '%-:measurementTypeId-%'
				and measurement_point_number = :measurementPointNumber
				and measurement_date_time = :measurementDateTime
				and is_current = true
			""")
	Measurement findCurrentMeasurement(@Bind String stationId,
			@Bind int measurementPointNumber,
			@Bind int examinationTypeSc,
			@Bind OffsetDateTime measurementDateTime
			);
	
	/**
	 * Add measurement if it does not exists
	 * 
	 * @param measurement
	 */
	@SqlQuery("""
			insert into hydrometry.measurement 
			(station_id, measurement_date_time, measurement_point_number, measurement_type_id, result, is_current, created)
			values 
			(:stationId, :measurementDateTime, :measurementPointNumber, :measurementTypeId, :result, :isCurrent, now())
			returning *
			""")
	Measurement insertMeasurement(@BindBean Measurement measurement);
	
	/**
	 * Update active status on matching measurements
	 * 
	 * @param measurement
	 */
	@SqlUpdate("""
			update hydrometry.measurement set is_current = false
			where
				station_id = :stationId
				and measurement_date_time = :measurementDateTime
				and measurement_point_number = :measurementPointNumber
				and measurement_type_id = :measurementTypeId
			""")
	int inactivateMeasurement(@BindBean Measurement measurement);
		
	@SqlUpdate("""
			delete
			from hydrometry.measurement
			where
				station_id = :stationId
				and measurement_date_time = :measurementDateTime
				and measurement_point_number = :measurementPointNumber
				and measurement_type_id = :measurementTypeId
			""")
	int deleteMeasurement(@Bind String stationId, @Bind int measurementPointNumber,
			@Bind String measurementTypeId,
			@Bind OffsetDateTime measurementDateTime
			);
	
	@SqlUpdate("""
			delete
			from hydrometry.measurement
			where
				station_id = :stationId
			""")
	int deleteMeasurement(@Bind String stationId);
	
	@SqlQuery("""
			select count(*) from hydrometry.measurement
			where
				station_id = :stationId
				and measurement_date_time = :measurementDateTime
				and measurement_point_number = :measurementPointNumber
				and measurement_type_id = :measurementTypeId
			""")
	int countHistory(@Bind String stationId, @Bind int measurementPointNumber,
			@Bind String measurementTypeId,
			@Bind OffsetDateTime measurementDateTime);
	
	@SqlQuery("select count(*) from hydrometry.measurement")
	int countAll();
	
}
