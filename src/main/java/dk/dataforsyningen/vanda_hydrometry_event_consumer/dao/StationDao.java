package dk.dataforsyningen.vanda_hydrometry_event_consumer.dao;

import java.util.List;

import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindBean;
import org.jdbi.v3.sqlobject.statement.SqlBatch;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import dk.dataforsyningen.vanda_hydrometry_event_consumer.config.LogSqlFactory;
import dk.dataforsyningen.vanda_hydrometry_event_consumer.model.Station;

@RegisterRowMapper(StationMapper.class)
@LogSqlFactory
public interface StationDao {
	
	@SqlQuery("""
			select
				station_id,
				name,
				old_station_number,
				station_owner_name,
				location,
				ST_AsEWKT(location) as location_as_ewkt,
				description,
				created,
				updated
			from hydrometry.station
			""")
	List<Station> getAllStations();
	
	@SqlQuery("""
			select
				station_id,
				name,
				old_station_number,
				station_owner_name,
				location,
				ST_AsEWKT(location) as location_as_ewkt,
				description,
				created,
				updated
			from hydrometry.station
			where station_id = :stationId
			""")
	Station findStationByStationId(@Bind String stationId);

	/**
	 * Add station if not exists
	 * @param station
	 */
	@SqlUpdate("""
			insert into station
			(station_id, old_station_number, name, station_owner_name, location, description, created, updated)
			values ( :stationId, :oldStationNumber, :name, :stationOwnerName, (ST_SetSRID(ST_MakePoint(:location.x, :location.y), :location.sridAsInt)), :description, now(), now())
			on conflict (station_id) do update
				set old_station_number = :oldStationNumber,
				name = :name,
				station_owner_name = :stationOwnerName,
				location = (ST_SetSRID(ST_MakePoint(:location.x, :location.y), :location.sridAsInt)),
				description = :description,
				updated = now()
			""")
	void addStation(@BindBean Station station);
	
	@SqlBatch("""
			insert into hydrometry.station
			(station_id, old_station_number, name, station_owner_name, location, description, created, updated)
			values ( :stationId, :oldStationNumber, :name, :stationOwnerName, (ST_SetSRID(ST_MakePoint(:location.x, :location.y), :location.sridAsInt)), :description, now(), now())
			on conflict (station_id) do update
				set old_station_number = :oldStationNumber,
				name = :name,
				station_owner_name = :stationOwnerName,
				location = (ST_SetSRID(ST_MakePoint(:location.x, :location.y), :location.sridAsInt)),
				description = :description,
				updated = now()
			""")
	void addStations(@BindBean List<Station> stations);
	
	@SqlUpdate("delete from hydrometry.station where station_id = :id")
	void deleteStation(@Bind String id);
	
	@SqlQuery("select count(*) from hydrometry.station")
	int count();
	
}
