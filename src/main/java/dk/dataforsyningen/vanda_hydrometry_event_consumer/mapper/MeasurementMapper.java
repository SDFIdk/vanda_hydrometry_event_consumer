package dk.dataforsyningen.vanda_hydrometry_event_consumer.mapper;

import dk.dataforsyningen.vanda_hydrometry_event_consumer.model.Measurement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;

public class MeasurementMapper implements RowMapper<Measurement> {

  @Override
  public Measurement map(ResultSet rs, StatementContext ctx) throws SQLException {

    Measurement m = new Measurement();

    m.setStationId(rs.getString("station_id"));
    m.setValue(rs.getObject("value", Double.class));
    m.setValueElevationCorrected(rs.getObject("value_elevation_corrected", Double.class));
    m.setIsCurrent(rs.getBoolean("is_current"));
    m.setMeasurementDateTime(rs.getObject("measurement_date_time", OffsetDateTime.class));
    m.setCreated(rs.getObject("created", OffsetDateTime.class));
    m.setVandaEventTimestamp(rs.getObject("vanda_event_timestamp", OffsetDateTime.class));
    m.setExaminationTypeSc(rs.getObject("examination_type_sc", Integer.class));
    m.setMeasurementPointNumber(rs.getInt("measurement_point_number"));

    return m;
  }
}

