package dk.dataforsyningen.vanda_hydrometry_event_consumer.dao;

import java.util.List;

import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindBean;
import org.jdbi.v3.sqlobject.statement.GetGeneratedKeys;
import org.jdbi.v3.sqlobject.statement.SqlBatch;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import dk.dataforsyningen.vanda_hydrometry_event_consumer.config.LogSqlFactory;
import dk.dataforsyningen.vanda_hydrometry_event_consumer.model.MeasurementType;

@RegisterRowMapper(MeasurementTypeMapper.class)
@LogSqlFactory
public interface MeasurementTypeDao {

	@SqlQuery("""
			select
				measurement_type_id,
				parameter_sc,
				parameter,
				examination_type_sc,
				examination_type,
				unit_sc,
				unit
			from hydrometry.measurement_type
			where measurement_type_id = :measurementTypeId
			""")
	MeasurementType findMeasurementTypeById(@Bind String measurementTypeId);
	
	@SqlUpdate("""
			insert into hydrometry.measurement_type
			(measurement_type_id, parameter_sc, parameter, examination_type_sc, examination_type, unit_sc, unit)
			values (:measurementTypeId, :parameterSc, :parameter, :examinationTypeSc, :examinationType, :unitSc, :unit)
			on conflict do nothing
			""")
	void addMeasurementType(@BindBean MeasurementType measurementType);
	
	@SqlBatch("""
			insert into hydrometry.measurement_type
			(measurement_type_id, parameter_sc, parameter, examination_type_sc, examination_type, unit_sc, unit)
			values (:measurementTypeId, :parameterSc, :parameter, :examinationTypeSc, :examinationType, :unitSc, :unit)
			on conflict do nothing
			""")
	void addMeasurementTypes(@BindBean List<MeasurementType> measurementTypes);
	
	@SqlUpdate("""
			delete from hydrometry.measurement_type
			where measurement_type_id = :measurementTypeId
			""")
	int deleteMeasurementType(@Bind String measurementTypeId);
	
	@SqlQuery("select count(*) from hydrometry.measurement_type")
	int count();

}
