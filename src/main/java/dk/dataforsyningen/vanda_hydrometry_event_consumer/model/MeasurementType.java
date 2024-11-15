package dk.dataforsyningen.vanda_hydrometry_event_consumer.model;

import java.util.Objects;

public class MeasurementType {

  Integer examinationTypeSc = null;

  String examinationType = null;

  Integer parameterSc = null;

  String parameter = null;

  Integer unitSc = null;

  String unit = null;


  public String getUnit() {
    return unit;
  }

  public void setUnit(String unit) {
    this.unit = unit;
  }

  public Integer getUnitSc() {
    return unitSc;
  }

  public void setUnitSc(Integer unitSc) {
    this.unitSc = unitSc;
  }

  public String getParameter() {
    return parameter;
  }

  public void setParameter(String parameter) {
    this.parameter = parameter;
  }

  public Integer getParameterSc() {
    return parameterSc;
  }

  public void setParameterSc(Integer parameterSc) {
    this.parameterSc = parameterSc;
  }

  public String getExaminationType() {
    return examinationType;
  }

  public void setExaminationType(String examinationType) {
    this.examinationType = examinationType;
  }

  public Integer getExaminationTypeSc() {
    return examinationTypeSc;
  }

  public void setExaminationTypeSc(Integer examinationTypeSc) {
    this.examinationTypeSc = examinationTypeSc;
  }

  @Override
  public String toString() {
    return "MeasurementType [" +
        ",\n\texaminationTypeSc=" + examinationTypeSc +
        ",\n\texaminationType=" + examinationType +
        ",\n\tparameterSc=" + parameterSc +
        ",\n\tparameter=" + parameter +
        ",\n\tunitSc=" + unitSc +
        ",\n\tunit=" + unit +
        "]";
  }

  @Override
  public int hashCode() {
    return Objects.hash(examinationType, examinationTypeSc, parameter, parameterSc, unit, unitSc);
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
    MeasurementType other = (MeasurementType) obj;
    return Objects.equals(examinationType, other.examinationType)
        && Objects.equals(examinationTypeSc, other.examinationTypeSc)
        && Objects.equals(parameter, other.parameter)
        && Objects.equals(parameterSc, other.parameterSc)
        && Objects.equals(unit, other.unit)
        && Objects.equals(unitSc, other.unitSc);
  }


}
