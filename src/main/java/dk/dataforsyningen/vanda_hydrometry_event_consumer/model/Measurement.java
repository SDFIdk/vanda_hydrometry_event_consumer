package dk.dataforsyningen.vanda_hydrometry_event_consumer.model;

import java.time.OffsetDateTime;
import java.util.Objects;

public class Measurement {

  Integer measurementPointNumber = null;

  OffsetDateTime measurementDateTime = null;

  Double valueElevationCorrected = null;

  Double value = null;

  OffsetDateTime created = null;

  OffsetDateTime updated = null;

  OffsetDateTime vandaEventTimestamp = null;

  Boolean isCurrent = null;

  String stationId = null; //FK

  Integer examinationTypeSc = null; //FK


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

  public Double getValue() {
    return value;
  }

  public void setValue(Double value) {
    this.value = value;
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


  public Double getValueElevationCorrected() {
    return valueElevationCorrected;
  }

  public void setValueElevationCorrected(Double valueElevationCorrected) {
    this.valueElevationCorrected = valueElevationCorrected;
  }

  public OffsetDateTime getUpdated() {
    return updated;
  }

  public void setUpdated(OffsetDateTime updated) {
    this.updated = updated;
  }

  public OffsetDateTime getVandaEventTimestamp() {
    return vandaEventTimestamp;
  }

  public void setVandaEventTimestamp(OffsetDateTime vandaEventTimestamp) {
    this.vandaEventTimestamp = vandaEventTimestamp;
  }

  public Integer getExaminationTypeSc() {
    return examinationTypeSc;
  }

  public void setExaminationTypeSc(Integer examinationTypeSc) {
    this.examinationTypeSc = examinationTypeSc;
  }

  @Override
  public String toString() {
    return "Measurement [measurementPointNumber=" + measurementPointNumber
        + ", measurementDateTime=" + measurementDateTime
        + ", valueElevationCorrected=" + valueElevationCorrected
        + ", value=" + value
        + ", created=" + created
        + ", updated=" + updated
        + ", vandaEventTimestamp=" + vandaEventTimestamp
        + ", isCurrent=" + isCurrent
        + ", stationId=" + stationId
        + ", examinationTypeSc=" + examinationTypeSc
        + "]";
  }

  @Override
  public int hashCode() {
    return Objects.hash(created, examinationTypeSc, isCurrent, measurementDateTime,
        measurementPointNumber, value,
        valueElevationCorrected, stationId, updated, vandaEventTimestamp);
  }

  @Override
  public boolean equals(Object obj) {
	  if (this == obj) {
		  return true;
	  }
	  if (obj == null) {
		  return false;
	  }
	  if (getClass() != obj.getClass()) {
		  return false;
	  }
    Measurement other = (Measurement) obj;
    return Objects.equals(created, other.created) &&
        Objects.equals(examinationTypeSc, other.examinationTypeSc)
        && Objects.equals(isCurrent, other.isCurrent)
        && Objects.equals(measurementDateTime, other.measurementDateTime)
        && Objects.equals(measurementPointNumber, other.measurementPointNumber)
        && Objects.equals(value, other.value)
        && Objects.equals(valueElevationCorrected, other.valueElevationCorrected)
        && Objects.equals(stationId, other.stationId) && Objects.equals(updated, other.updated)
        && Objects.equals(vandaEventTimestamp, other.vandaEventTimestamp);
  }


}
