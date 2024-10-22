package dk.dataforsyningen.vanda_hydrometry_event_consumer.dao;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;

import dk.dataforsyningen.vanda_hydrometry_event_consumer.VandaHUtility;
import dk.dataforsyningen.vanda_hydrometry_event_consumer.model.Location;
import dk.dataforsyningen.vanda_hydrometry_event_consumer.model.MeasurementType;
import dk.dataforsyningen.vanda_hydrometry_event_consumer.model.Station;


public class StationMapper implements RowMapper<Station> {

	@Override
	public Station map(ResultSet rs, StatementContext ctx) throws SQLException {
		Station station = new Station();
		
		station.setStationId(rs.getString("station_id"));
		station.setOldStationNumber(rs.getString("old_station_number"));
		station.setName(rs.getString("name"));
		station.setStationOwnerName(rs.getString("station_owner_name"));

		Location location = new Location();

		location.setX((Double) rs.getObject("location_x"));
		location.setY((Double) rs.getObject("location_y"));
		location.setSrid(rs.getString("location_srid"));
		station.setLocation(location);

		station.setDescription(rs.getString("description"));
		station.setCreated(VandaHUtility.toOffsetDate(rs.getTimestamp("created"), false));
		station.setUpdated(VandaHUtility.toOffsetDate(rs.getTimestamp("updated"), false));
		
		String mtid = rs.getString("measurement_type_id");
		
		if (mtid != null) {
			MeasurementType mt = new MeasurementType();
		
			mt.setMeasurementTypeId(mtid);
			mt.setParameterSc(rs.getInt("parameter_sc"));
			mt.setParameter(rs.getString("parameter"));
			mt.setExaminationTypeSc(rs.getInt("examination_type_sc"));
			mt.setExaminationType(rs.getString("examination_type"));
			mt.setUnitSc(rs.getInt("unit_sc"));
			mt.setUnit(rs.getString("unit"));
			
			station.getMeasurementTypes().add(mt);
		}
				
		return station;
	}

}
