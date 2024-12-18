package dk.dataforsyningen.vanda_hydrometry_event_consumer.dao;

import dk.dataforsyningen.vanda_hydrometry_event_consumer.config.LogSqlFactory;
import dk.dataforsyningen.vanda_hydrometry_event_consumer.mapper.StationMapper;
import dk.dataforsyningen.vanda_hydrometry_event_consumer.model.MeasurementType;
import dk.dataforsyningen.vanda_hydrometry_event_consumer.model.Station;
import java.util.List;
import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindBean;
import org.jdbi.v3.sqlobject.statement.SqlBatch;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

@LogSqlFactory
public interface StationDao {

  /**
   * Read all station from DB
   *
   * @return list of stations
   */
  @SqlQuery("""
      select
      	s.station_id,
      	s.name,
      	s.station_id_sav,
      	s.station_owner_name,
      	s.geometry,
      	ST_X(geometry) as geometry_x,
      	ST_Y(geometry) as geometry_y,
      	ST_SRID(geometry) as geometry_srid,
      	s.location_type,
      	s.description,
      	s.created,
      	s.updated,
      	mt.parameter_sc,
      	mt.parameter,
      	mt.examination_type_sc,
      	mt.examination_type,
      	mt.unit_sc,
      	mt.unit
      from vanda.station s left join vanda.station_measurement_type smt
      	on s.station_id = smt.station_id
      	left join vanda.measurement_type mt
      	on smt.examination_type_sc = mt.examination_type_sc
      """)
  @RegisterRowMapper(StationMapper.class)
  List<Station> readAllStations();

  /**
   * Read the station with the given station id
   *
   * @param stationId
   * @return the station
   */
  @SqlQuery("""
      select
      	s.station_id,
      	s.name,
      	s.station_id_sav,
      	s.station_owner_name,
      	s.geometry,
      	ST_X(geometry) as geometry_x,
      	ST_Y(geometry) as geometry_y,
      	ST_SRID(geometry) as geometry_srid,
      	s.location_type,
      	s.description,
      	s.created,
      	s.updated,
      	mt.parameter_sc,
      	mt.parameter,
      	mt.examination_type_sc,
      	mt.examination_type,
      	mt.unit_sc,
      	mt.unit
      from vanda.station s left join vanda.station_measurement_type smt
      	on s.station_id = smt.station_id
      	left join vanda.measurement_type mt
      	on smt.examination_type_sc = mt.examination_type_sc 
      where s.station_id = :stationId
      """)
  @RegisterRowMapper(StationMapper.class)
  List<Station> readStationByStationId(@Bind String stationId);

  /**
   * Read all stations that are supporting the given examination type
   *
   * @param examinationTypeSc
   * @return list of stations
   */
  @SqlQuery("""
      select
      	s.station_id,
      	s.name,
      	s.station_id_sav,
      	s.station_owner_name,
      	s.geometry,
      	ST_X(geometry) as geometry_x,
      	ST_Y(geometry) as geometry_y,
      	ST_SRID(geometry) as geometry_srid,
      	s.location_type,
      	s.description,
      	s.created,
      	s.updated,
      	mt.parameter_sc,
      	mt.parameter,
      	mt.examination_type_sc,
      	mt.examination_type,
      	mt.unit_sc,
      	mt.unit
      from vanda.station s left join vanda.station_measurement_type smt
      	on s.station_id = smt.station_id
      	left join vanda.measurement_type mt
      	on smt.examination_type_sc = mt.examination_type_sc 
      where mt.examination_type_sc = :examinationTypeSc
      """)
  @RegisterRowMapper(StationMapper.class)
  List<Station> readStationByExaminationTypeSc(@Bind int examinationTypeSc);

  /**
   * Checks if the given station supports the given examination type
   *
   * @param stationId
   * @param examinationTypeSc
   * @return true | false
   */
  @SqlQuery("""
      select count(*) > 0 as is_supported
      from vanda.station_measurement_type smt
      where smt.station_id = :stationId 
      	and smt.examination_type_sc = :examinationTypeSc
      """)
  @RegisterRowMapper(StationMapper.class)
  boolean isExaminationTypeScSupported(@Bind String stationId, @Bind int examinationTypeSc);

  /**
   * Adds (or updates if exists) (only) the station if it does not exist.
   *
   * @param station
   * @return the station
   */
  @SqlUpdate("""
      insert into vanda.station
      (station_id, station_id_sav, name, station_owner_name, geometry, location_type, description, created, updated)
      values ( :stationId, :stationIdSav, :name, :stationOwnerName, (ST_SetSRID(ST_MakePoint(:geometryX, :geometryY), :geometrySrid::int)), :locationType, :description, now(), now())
      on conflict (station_id) do update
      	set station_id_sav = :stationIdSav,
      	name = :name,
      	station_owner_name = :stationOwnerName,
      	geometry = (ST_SetSRID(ST_MakePoint(:geometryX, :geometryY), :geometrySrid::int)),
      	location_type = :locationType,
      	description = :description,
      	updated = now()
      """)
  void insertStation(@BindBean Station station);

  /**
   * Add (or update if they exist) the stations (only) from the list if they do not exist.
   *
   * @param stations
   */
  @SqlBatch("""
      insert into vanda.station
      (station_id, station_id_sav, name, station_owner_name, geometry, location_type, description, created, updated)
      values ( :stationId, :stationIdSav, :name, :stationOwnerName, (ST_SetSRID(ST_MakePoint(:geometryX, :geometryY), :geometrySrid::int)), :locationType, :description, now(), now())
      on conflict (station_id) do update
      	set station_id_sav = :stationIdSav,
      	name = :name,
      	station_owner_name = :stationOwnerName,
      	geometry = (ST_SetSRID(ST_MakePoint(:geometryX, :geometryY), :geometrySrid::int)),
      	location_type = :locationType,
      	description = :description,
      	updated = now()
      """)
  void insertStations(@BindBean List<Station> stations);

  /**
   * Adds the relation between station and measurement type
   *
   * @param stationId
   * @param examinationTypeSc
   */
  @SqlUpdate("""
      insert into vanda.station_measurement_type (station_id, examination_type_sc)
      values (:stationId, :examinationTypeSc)
      on conflict do nothing
      """)
  void insertStationMeasurementTypeRelation(@Bind String stationId, @Bind String examinationTypeSc);

  /**
   * Adds the relation between station and measurement type from the given relations list
   *
   * @param stationId
   * @param examinationTypeSc
   */
  @SqlBatch("""
      insert into vanda.station_measurement_type (station_id, examination_type_sc)
      values (:stationId, :examinationTypeSc)
      on conflict do nothing
      """)
  void insertStationMeasurementTypeRelations(@Bind List<String> stationId,
                                             @BindBean List<MeasurementType> examinationTypeSc);

  /**
   * Deletes the given station (without relations)
   *
   * @param station id
   */
  @SqlUpdate("delete from vanda.station where station_id = :id")
  void deleteStation(@Bind String id);

  /**
   * Deletes the station/measurement type relation for the given station
   *
   * @param station id
   */
  @SqlUpdate("delete from vanda.station_measurement_type where station_id = :id")
  void deleteRelationToMeasurementTypeByStation(@Bind String id);

  @SqlQuery("select count(*) from vanda.station")
  int count();

}
