package dk.dataforsyningen.vanda_hydrometry_event_consumer.model;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Objects;

public class Station {

  String stationUid = null;

  String stationId = null; //Key

  String operatorStationId = null;

  String stationIdSav = null;

  String name = null;

  String stationOwnerName = null;

  Double geometryX;

  Double geometryY;

  Integer geometrySrid;

  String locationType = null;

  String description = null;

  OffsetDateTime created = null;

  OffsetDateTime updated = null;

  ArrayList<MeasurementType> measurementTypes = new ArrayList<>();


  public String getStationUid() {
    return stationUid;
  }

  public void setStationUid(String stationUid) {
    this.stationUid = stationUid;
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

  public String getStationIdSav() {
    return stationIdSav;
  }

  public void setStationIdSav(String stationIdSav) {
    this.stationIdSav = stationIdSav;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getStationOwnerName() {
    return stationOwnerName;
  }

  public void setStationOwnerName(String stationOwnerName) {
    this.stationOwnerName = stationOwnerName;
  }

  public Double getGeometryX() {
    return geometryX;
  }

  public void setGeometryX(Double x) {
    this.geometryX = x;
  }

  public Double getGeometryY() {
    return geometryY;
  }

  public void setGeometryY(Double y) {
    this.geometryY = y;
  }

  public Integer getGeometrySrid() {
    return geometrySrid;
  }

  public void setGeometrySrid(Integer srid) {
    this.geometrySrid = srid;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public OffsetDateTime getCreated() {
    return created;
  }

  public void setCreated(OffsetDateTime created) {
    this.created = created;
  }

  public OffsetDateTime getUpdated() {
    return updated;
  }

  public void setUpdated(OffsetDateTime updated) {
    this.updated = updated;
  }

  public ArrayList<MeasurementType> getMeasurementTypes() {
    return measurementTypes;
  }

  public void setMeasurementTypes(ArrayList<MeasurementType> measurementTypes) {
    this.measurementTypes = measurementTypes;
  }

  public String getLocationType() {
    return locationType;
  }

  public void setLocationType(String locationType) {
    this.locationType = locationType;
  }

  @Override
  public String toString() {
    return "Station [" +
        "\n\tstationUid=" + stationUid +
        ",\n\tstationId=" + stationId +
        ",\n\toperatorStationId=" + operatorStationId +
        ",\n\tstationIdSav=" + stationIdSav +
        ",\n\tname=" + name +
        ",\n\tstationOwnerName=" + stationOwnerName +
        ",\n\tgeometry= [x=" + geometryX + ", y=" + geometryY + ", srid=" + geometrySrid + "]" +
        ",\n\tlocationType=" + locationType +
        ",\n\tdescription=" + description +
        ",\n\tcreated=" + created +
        ",\n\tupdated=" + updated +
        ",\n\tmeasurementTypes=" + measurementTypes +
        "]";
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, created, description, geometryX, geometryY, geometrySrid,
        locationType, stationIdSav,
        operatorStationId, stationId, stationOwnerName, stationUid, updated);
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
    Station other = (Station) obj;
    return Objects.equals(stationUid, other.stationUid)
        && Objects.equals(stationId, other.stationId)
        && Objects.equals(stationOwnerName, other.stationOwnerName)
        && Objects.equals(name, other.name)
        && Objects.equals(locationType, other.locationType)
        && Objects.equals(description, other.description)
        && Objects.equals(geometrySrid, other.geometrySrid) &&
        Objects.equals(geometryX, other.geometryX) && Objects.equals(geometryY, other.geometryY)
        && Objects.equals(stationIdSav, other.stationIdSav)
        && Objects.equals(operatorStationId, other.operatorStationId)
        && Objects.equals(created, other.created)
        && Objects.equals(updated, other.updated);
  }


}
