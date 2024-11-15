package dk.dataforsyningen.vanda_hydrometry_event_consumer.mapper;

import dk.dataforsyningen.vanda_hydrometry_event_consumer.model.EventModel;
import dk.dataforsyningen.vanda_hydrometry_event_consumer.model.Measurement;

public class EventMeasurementMapper {

  public static Measurement measurementFrom(EventModel event) {
    Measurement measurement = new Measurement();

    measurement.setStationId(event.getStationId());
    measurement.setMeasurementPointNumber(event.getMeasurementPointNumber());
    measurement.setResult(event.getResult());
    measurement.setVandaEventTimestamp(event.getRecordDateTime());
    measurement.setMeasurementDateTime(event.getMeasurementDateTime());
    measurement.setExaminationTypeSc(event.getExaminationTypeSc());

    return measurement;
  }
}
