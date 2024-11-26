package dk.dataforsyningen.vanda_hydrometry_event_consumer.model;

import java.time.OffsetDateTime;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

public class EventModel {

  @JsonProperty(value = "EventType")
  private String eventType;
  @JsonProperty(value = "StationId")
  private String stationId;
  @JsonProperty(value = "OperatorStationId")
  private String operatorStationId;
  @JsonProperty(value = "MeasurementPointNumber")
  private Integer measurementPointNumber;
  @JsonProperty(value = "UnitSc")
  private Integer unitSc;
  @JsonProperty(value = "ParameterSc")
  private Integer parameterSc;
  @JsonProperty(value = "ExaminationTypeSc")
  private Integer examinationTypeSc;
  @JsonProperty(value = "ReasonCodeSc")
  private Integer reasonCodeSc;
  @JsonProperty(value = "Result")
  private Double result;
  @JsonProperty(value = "MeasurementDateTime")
  private OffsetDateTime measurementDateTime;
  @JsonProperty(value = "LoggerId")
  private String loggerId;
  private OffsetDateTime recordDateTime;
  private long offset;
  private int partition;

  public EventModel() {
  }

  public EventModel(EventModel eventModel) {
    this.eventType = eventModel.getEventType();
    this.stationId = eventModel.getStationId();
    this.operatorStationId = eventModel.getOperatorStationId();
    this.measurementPointNumber = eventModel.getMeasurementPointNumber();
    this.unitSc = eventModel.getUnitSc();
    this.parameterSc = eventModel.getParameterSc();
    this.examinationTypeSc = eventModel.getExaminationTypeSc();
    this.reasonCodeSc = eventModel.getReasonCodeSc();
    this.result = eventModel.getResult();
    this.measurementDateTime = eventModel.getMeasurementDateTime();
    this.loggerId = eventModel.getLoggerId();
    this.recordDateTime = eventModel.getRecordDateTime();
    this.offset = eventModel.getOffset();
    this.partition = eventModel.getPartition();
  }

  public String getEventType() {
    return eventType;
  }

  public void setEventType(String eventType) {
    this.eventType = eventType;
  }

  public String getStationId() {
    return stationId;
  }

  public void setStationId(String stationId) {
    this.stationId = stationId;
  }

  public String getOperatorStationId() {
    return operatorStationId;
  }

  public void setOperatorStationId(String operatorStationId) {
    this.operatorStationId = operatorStationId;
  }

  public int getMeasurementPointNumber() {
    return measurementPointNumber;
  }

  public void setMeasurementPointNumber(Integer measurementPointNumber) {
    this.measurementPointNumber = measurementPointNumber;
  }

  public Integer getUnitSc() {
    return unitSc;
  }

  public void setUnitSc(Integer unitSc) {
    this.unitSc = unitSc;
  }

  public Integer getParameterSc() {
    return parameterSc;
  }

  public void setParameterSc(Integer parameterSc) {
    this.parameterSc = parameterSc;
  }

  public Integer getExaminationTypeSc() {
    return examinationTypeSc;
  }

  public void setExaminationTypeSc(Integer examinationTypeSc) {
    this.examinationTypeSc = examinationTypeSc;
  }

  public Integer getReasonCodeSc() {
    return reasonCodeSc;
  }

  public void setReasonCodeSc(Integer reasonCodeSc) {
    this.reasonCodeSc = reasonCodeSc;
  }

  public Double getResult() {
    return result;
  }

  public void setResult(Double result) {
    this.result = result;
  }

  public OffsetDateTime getMeasurementDateTime() {
    return measurementDateTime;
  }

  public void setMeasurementDateTime(String measurementDateTime) {
    this.measurementDateTime = OffsetDateTime.parse(measurementDateTime);
  }
  
  public String getLoggerId() {
	return loggerId;
  }

  public void setLoggerId(String loggerId) {
	this.loggerId = loggerId;
  }

  public OffsetDateTime getRecordDateTime() {
    return recordDateTime;
  }

  public void setRecordDateTime(OffsetDateTime recordDateTime) {
    this.recordDateTime = recordDateTime;
  }

  public long getOffset() {
    return offset;
  }

  public void setOffset(long offset) {
    this.offset = offset;
  }

  public int getPartition() {
    return partition;
  }

  public void setPartition(int partition) {
    this.partition = partition;
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
    EventModel other = (EventModel) obj;
    return Objects.equals(examinationTypeSc, other.examinationTypeSc)
        && Objects.equals(eventType, other.eventType)
        && Objects.equals(measurementDateTime, other.measurementDateTime)
        && Objects.equals(measurementPointNumber, other.measurementPointNumber) 
        && offset == other.offset
        && Objects.equals(operatorStationId, other.operatorStationId)
        && Objects.equals(parameterSc, other.parameterSc) 
        && partition == other.partition
        && Objects.equals(reasonCodeSc, other.reasonCodeSc)
        && Objects.equals(recordDateTime, other.recordDateTime)
        && Double.doubleToLongBits(result) == Double.doubleToLongBits(other.result)
        && Objects.equals(stationId, other.stationId)
        && Objects.equals(loggerId, other.loggerId) 
        && Objects.equals(unitSc, other.unitSc);
  }

  public boolean isSameMeasurement(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
    EventModel other = (EventModel) obj;
    return examinationTypeSc == other.examinationTypeSc
        && Objects.equals(measurementDateTime, other.measurementDateTime)
        && measurementPointNumber == other.measurementPointNumber
        && Objects.equals(stationId, other.stationId)
        && Objects.equals(operatorStationId, other.operatorStationId);
  }

  @Override
  public String toString() {
    return "EventModel [eventType=" + eventType + ", stationId=" + stationId +
        ", operatorStationId="
        + operatorStationId + ", measurementPointNumber=" + measurementPointNumber + ", unitSc=" +
        unitSc
        + ", parameterSc=" + parameterSc + ", examinationTypeSc=" + examinationTypeSc +
        ", reasonCodeSc="
        + reasonCodeSc + ", result=" + result + ", measurementDateTime=" + measurementDateTime
        + ", loggerId=" + loggerId
        + ", recordDateTime=" + recordDateTime + ", offset=" + offset + ", partition=" + partition
        + "]";
  }


}
