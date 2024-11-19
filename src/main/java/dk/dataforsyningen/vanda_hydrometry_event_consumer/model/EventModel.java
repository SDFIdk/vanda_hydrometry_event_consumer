package dk.dataforsyningen.vanda_hydrometry_event_consumer.model;

import dk.dataforsyningen.vanda_hydrometry_event_consumer.VandaHUtility;
import java.time.OffsetDateTime;
import java.util.Objects;
import org.json.JSONObject;

public class EventModel {

  private String eventType;
  private String stationId;
  private String operatorStationId;
  private Integer measurementPointNumber;
  private Integer unitSc;
  private Integer parameterSc;
  private Integer examinationTypeSc;
  private Integer reasonCodeSc;
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
    event.setMeasurementPointNumber(bodyObj.has("MeasurementPointNumber") ? 
    		VandaHUtility.toInt("" + bodyObj.get("MeasurementPointNumber")) : null);
    event.setUnitSc(bodyObj.has("UnitSc") ? VandaHUtility.toInt("" + bodyObj.get("UnitSc")) : null);
    event.setParameterSc(bodyObj.has("ParameterSc") ?
        VandaHUtility.toInt("" + bodyObj.get("ParameterSc")) : null);
    event.setExaminationTypeSc(bodyObj.has("ExaminationTypeSc") ? 
    		VandaHUtility.toInt("" + bodyObj.get("ExaminationTypeSc")) : null);
    event.setReasonCodeSc(bodyObj.has("ReasonCodeSc") ?
        VandaHUtility.toInt("" + bodyObj.get("ReasonCodeSc")) : null);
    event.setResult(bodyObj.has("Result") ?
        VandaHUtility.toDouble("" + bodyObj.get("Result")) : null);
    event.setMeasurementDateTime(bodyObj.has("MeasurementDateTime") ? 
    		VandaHUtility.parseToUtcOffsetDateTime("" + bodyObj.get("MeasurementDateTime")) : null);
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
        + ", recordDateTime=" + recordDateTime + ", offset=" + offset + ", partition=" + partition
        + "]";
  }


}
