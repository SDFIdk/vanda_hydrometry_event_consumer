package dk.dataforsyningen.vanda_hydrometry_event_consumer.dao;

import java.util.List;

import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindBean;
import org.jdbi.v3.sqlobject.statement.SqlBatch;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import dk.dataforsyningen.vanda_hydrometry_event_consumer.config.LogSqlFactory;
import dk.dataforsyningen.vanda_hydrometry_event_consumer.model.MeasurementType;
import dk.dataforsyningen.vanda_hydrometry_event_consumer.mapper.MeasurementTypeMapper;


@LogSqlFactory
public interface MeasurementTypeDao {
	
	/**
	 * Read measurement type with the given examination type Sc
	 * @param examinationTypeSc
	 * @return measurement type
	 */
	@SqlQuery("""
			select
				examination_type_sc,
				examination_type,
				parameter_sc,
				parameter,
				unit_sc,
				unit
			from hydrometry.measurement_type
			where examination_type_sc = :examinationTypeSc
			""")
	@RegisterRowMapper(MeasurementTypeMapper.class)
	MeasurementType readMeasurementTypeByExaminationType(@Bind int examinationTypeSc);

	/**
	 * Inserts (or updates if exists) the measurement types from the given list
	 * @param measurementTypes List
	 */
	@SqlBatch("""
			insert into hydrometry.measurement_type
			(parameter_sc, parameter, examination_type_sc, examination_type, unit_sc, unit)
			values (:parameterSc, :parameter, :examinationTypeSc, :examinationType, :unitSc, :unit)
			on conflict (examination_type_sc) do update
				set parameter = EXCLUDED.parameter,
					examination_type = EXCLUDED.examination_type,
					unit = EXCLUDED.unit
			""")
	void insertMeasurementTypes(@BindBean List<MeasurementType> measurementTypes);
	
	/**
	 * Deletes the measurement type with the given examination type sc
	 * 
	 * @param examinationTypeSc
	 */
	@SqlUpdate("delete from hydrometry.measurement_type where examination_type_sc = :examinationTypeSc")
	void deleteMeasurementType(@Bind int examinationTypeSc);
}
