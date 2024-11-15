package dk.dataforsyningen.vanda_hydrometry_event_consumer.model;

import dk.dataforsyningen.vanda_hydrometry_event_consumer.VandaHUtility;
import java.time.OffsetDateTime;
import java.util.Objects;
import org.json.JSONObject;

public class EventModel {

  private String eventType;
  private String stationId;
  private String operatorStationId;
  private int measurementPointNumber;
  private int unitSc;
  private int parameterSc;
  private int examinationTypeSc;
  private int reasonCodeSc;
  private Double result;
  private OffsetDateTime measurementDateTime;
  private OffsetDateTime recordDateTime;
  private long offset;
  private int partition;

  public EventModel() {
  }

  public EventModel(EventModel obj) {
    this.eventType = obj.getEventType();
    this.stationId = obj.getStationId();
    this.operatorStationId = obj.getOperatorStationId();
    this.measurementPointNumber = obj.getMeasurementPointNumber();
    this.unitSc = obj.getUnitSc();
    this.parameterSc = obj.getParameterSc();
    this.examinationTypeSc = obj.getExaminationTypeSc();
    this.reasonCodeSc = obj.getReasonCodeSc();
    this.result = obj.getResult();
    this.measurementDateTime = obj.getMeasurementDateTime();
    this.recordDateTime = obj.getRecordDateTime();
    this.offset = obj.getOffset();
    this.partition = obj.getPartition();
  }


  public static EventModel fromJson(String json) {
    EventModel event = new EventModel();
		if (json == null || json.length() == 0) {
			return null;
		}
    JSONObject bodyObj = new JSONObject(json);
    event.setEventType(bodyObj.has("EventType") ? "" + bodyObj.get("EventType") : null);
    event.setStationId(bodyObj.has("StationId") ? "" + bodyObj.get("StationId") : null);
    event.setOperatorStationId(
        bodyObj.has("OperatorStationId") ? "" + bodyObj.get("OperatorStationId") : null);
    event.setMeasurementPointNumber(VandaHUtility.toInt(
        bodyObj.has("MeasurementPointNumber") ? "" + bodyObj.get("MeasurementPointNumber") : "0"));
    event.setUnitSc(VandaHUtility.toInt(bodyObj.has("UnitSc") ? "" + bodyObj.get("UnitSc") : "0"));
    event.setParameterSc(
        VandaHUtility.toInt(bodyObj.has("ParameterSc") ? "" + bodyObj.get("ParameterSc") : "0"));
    event.setExaminationTypeSc(VandaHUtility.toInt(
        bodyObj.has("ExaminationTypeSc") ? "" + bodyObj.get("ExaminationTypeSc") : "0"));
    event.setReasonCodeSc(
        VandaHUtility.toInt(bodyObj.has("ReasonCodeSc") ? "" + bodyObj.get("ReasonCodeSc") : "0"));
    event.setResult(
        VandaHUtility.toDouble(bodyObj.has("Result") ? "" + bodyObj.get("Result") : "0.0"));
    event.setMeasurementDateTime(VandaHUtility.parseToUtcOffsetDateTime(
        bodyObj.has("MeasurementDateTime") ? "" + bodyObj.get("MeasurementDateTime") : null));
    event.setRecordDateTime(null);
    event.setOffset(0L);
    event.setPartition(0);

    return event;
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

  public void setMeasurementPointNumber(int measurementPointNumber) {
    this.measurementPointNumber = measurementPointNumber;
  }

  public int getUnitSc() {
    return unitSc;
  }

  public void setUnitSc(int unitSc) {
    this.unitSc = unitSc;
  }

  public int getParameterSc() {
    return parameterSc;
  }

  public void setParameterSc(int parameterSc) {
    this.parameterSc = parameterSc;
  }

  public int getExaminationTypeSc() {
    return examinationTypeSc;
  }

  public void setExaminationTypeSc(int examinationTypeSc) {
    this.examinationTypeSc = examinationTypeSc;
  }

  public int getReasonCodeSc() {
    return reasonCodeSc;
  }

  public void setReasonCodeSc(int reasonCodeSc) {
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

  public void setMeasurementDateTime(OffsetDateTime measurementDateTime) {
    this.measurementDateTime = measurementDateTime;
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
    return examinationTypeSc == other.examinationTypeSc
        && Objects.equals(eventType, other.eventType)
        && Objects.equals(measurementDateTime, other.measurementDateTime)
        && measurementPointNumber == other.measurementPointNumber && offset == other.offset
        && Objects.equals(operatorStationId, other.operatorStationId)
        && parameterSc == other.parameterSc && partition == other.partition
        && reasonCodeSc == other.reasonCodeSc
        && Objects.equals(recordDateTime, other.recordDateTime)
        && Double.doubleToLongBits(result) == Double.doubleToLongBits(other.result)
        && Objects.equals(stationId, other.stationId) && unitSc == other.unitSc;
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
        + ", recordDateTime=" + recordDateTime + ", offset=" + offset + ", partition=" + partition
        + "]";
  }


}
