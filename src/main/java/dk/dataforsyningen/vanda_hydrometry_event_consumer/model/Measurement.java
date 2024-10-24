package dk.dataforsyningen.vanda_hydrometry_event_consumer.model;

import java.time.OffsetDateTime;
import java.util.Objects;


public class Measurement {

	Integer measurementPointNumber = null;

	OffsetDateTime measurementDateTime = null;

	Double result = null;
	
	OffsetDateTime created = null;
	
	Boolean isCurrent = null;
	
	String stationId = null; //FK

	String measurementTypeId = null; //FK	
	
	public static Measurement from(EventModel event) {
		Measurement measurement = new Measurement();
		
		measurement.setStationId(event.getStationId());
		measurement.setMeasurementPointNumber(event.getMeasurementPointNumber());
		measurement.setResult(event.getResult());
		measurement.setMeasurementDateTime(event.getMeasurementDateTime());
		
		if (event.getParameterSc() != 0 && event.getExaminationTypeSc() != 0 && event.getUnitSc() != 0) {
			measurement.setMeasurementTypeId(
				event.getParameterSc() + "-" +
				event.getExaminationTypeSc() + "-" +
				event.getUnitSc()
				);
		}
		
		return measurement;
	}
	
	public Integer getMeasurementPointNumber() {
		return measurementPointNumber;
	}

	public void setMeasurementPointNumber(Integer measurementPointNumber) {
		this.measurementPointNumber = measurementPointNumber;
	}

	public OffsetDateTime getMeasurementDateTime() {
		return measurementDateTime;
	}

	public void setMeasurementDateTime(OffsetDateTime measurementDateTime) {
		this.measurementDateTime = measurementDateTime;
	}

	public Double getResult() {
		return result;
	}

	public void setResult(Double result) {
		this.result = result;
	}

	public OffsetDateTime getCreated() {
		return created;
	}

	public void setCreated(OffsetDateTime created) {
		this.created = created;
	}

	public Boolean getIsCurrent() {
		return isCurrent;
	}

	public void setIsCurrent(Boolean isCurrent) {
		this.isCurrent = isCurrent;
	}

	public String getStationId() {
		return stationId;
	}

	public void setStationId(String stationId) {
		this.stationId = stationId;
	}

	public String getMeasurementTypeId() {
		return measurementTypeId;
	}

	public void setMeasurementTypeId(String measurementTypeId) {
		this.measurementTypeId = measurementTypeId;
	}

	@Override
	public String toString() {
		return "Measurement [" +
				"\n\tstationId=" + stationId +
				",\n\tmeasurementPointNumber=" + measurementPointNumber + 
				",\n\tmeasurementDateTime=" + measurementDateTime + 
				",\n\tresult=" + result + 
				",\n\tcreated=" + created + 
				",\n\tisCurrent=" + isCurrent +
				",\n\tmeasurementTypeId=" + measurementTypeId + "]";
	}

	@Override
	public int hashCode() {
		return Objects.hash(created, isCurrent, measurementDateTime, measurementPointNumber, measurementTypeId, result,
				stationId);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Measurement other = (Measurement) obj;
		return Objects.equals(created, other.created) && Objects.equals(isCurrent, other.isCurrent)
				&& Objects.equals(measurementDateTime, other.measurementDateTime)
				&& Objects.equals(measurementPointNumber, other.measurementPointNumber)
				&& Objects.equals(measurementTypeId, other.measurementTypeId) && Objects.equals(result, other.result)
				&& Objects.equals(stationId, other.stationId);
	}
	
	
}
