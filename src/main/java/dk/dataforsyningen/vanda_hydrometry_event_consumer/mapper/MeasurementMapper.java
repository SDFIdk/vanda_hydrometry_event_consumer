package dk.dataforsyningen.vanda_hydrometry_event_consumer.mapper;

import dk.dataforsyningen.vanda_hydrometry_event_consumer.VandaHUtility;
import dk.dataforsyningen.vanda_hydrometry_event_consumer.model.Measurement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;


public class MeasurementMapper implements RowMapper<Measurement> {

  @Override
  public Measurement map(ResultSet rs, StatementContext ctx) throws SQLException {

    Measurement m = new Measurement();

    m.setStationId(rs.getString("station_id"));
    m.setResult((Double) rs.getObject("result"));
    m.setResultElevationCorrected((Double) rs.getObject("result_elevation_corrected"));
    m.setIsCurrent(rs.getBoolean("is_current"));
    m.setMeasurementDateTime(
        VandaHUtility.toOffsetDate(rs.getTimestamp("measurement_date_time"), true));
    m.setCreated(VandaHUtility.toOffsetDate(rs.getTimestamp("created"), false));
    m.setVandaEventTimestamp(
        VandaHUtility.toOffsetDate(rs.getTimestamp("vanda_event_timestamp"), true));
    m.setExaminationTypeSc((Integer) rs.getObject("examination_type_sc"));
    m.setMeasurementPointNumber(rs.getInt("measurement_point_number"));

    return m;
  }
}

