package dk.dataforsyningen.vanda_hydrometry_event_consumer.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import dk.dataforsyningen.vanda_hydrometry_event_consumer.config.VandaHEventConsumerConfig;
import dk.dataforsyningen.vanda_hydrometry_event_consumer.model.EventModel;
import dk.dataforsyningen.vanda_hydrometry_event_consumer.model.Measurement;
import dk.dataforsyningen.vanda_hydrometry_event_consumer.model.MeasurementType;
import dk.dataforsyningen.vanda_hydrometry_event_consumer.model.Station;
import java.lang.reflect.Field;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import org.jdbi.v3.core.statement.UnableToExecuteStatementException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.springframework.aop.framework.AopProxyUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class DatabaseServiceTest {

  private final String stationId = "S1234567";  //it is fake
  private final String stationName = "name";
  private final String stationOwner = "owner";
  private final String stationDescription = "description";
  private final String stationIdSav = "12345678";
  private final double locationX = 12.34;
  private final double locationY = 56.78;
  private final int locationSrid = 25832;

  private final int mtParamSc1 = 1233;
  private final String mtParam1 = "WaterLevel";
  private final int mtExamTypeSc1 = 999925;  //it is fake
  private final String mtExamType1 = "WaterLevel";
  private final int mtUnitSc1 = 19;
  private final String mtUnit1 = "CM";

  private final int mtParamSc2 = 1155;
  private final String mtParam2 = "Vandføring";
  private final int mtExamTypeSc2 = 999927;  //it is fake
  private final String mtExamType2 = "Vandføring";
  private final int mtUnitSc2 = 55;
  private final String mtUnit2 = "l/s";

  private final int mtExamTypeSc0 = 999900;  //it is fake

  private final int measurementPoint1 = 1;
  private final double resultA = 12.34;
  private final double resultB = 56.78;
  private final double resultC = 90.11;
  private final double resultECA = 12.34;
  private final double resultECB = 56.78;

  private Station station1;
  private MeasurementType mt1;
  private MeasurementType mt2;
  private Measurement m1;
  private EventModel event;

  private String dt1DayAgoString;
  private String dt10MinAgoString;
  private String dt5MinAgoString;
  private String dtNowString;
  private String dt5MinAfterString;

  private OffsetDateTime dt1DayAgo;
  private OffsetDateTime dt10MinAgo;
  private OffsetDateTime dt5MinAgo;
  private OffsetDateTime dtNow;
  private OffsetDateTime dt5MinAfter;

  private boolean enableTest = false;

  @Autowired
  private DatabaseService dbService;

  @Autowired
  private VandaHEventConsumerConfig config;

  @BeforeEach
  public void setup() {

    enableTest = config.isEnableDbTest();

    System.out.println("Database testing " + (enableTest ? "enabled" : "disabled"));
    if (!enableTest) {
      return;
    }

    dtNowString = OffsetDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"));
    dt5MinAgoString = OffsetDateTime.now(ZoneOffset.UTC).minusMinutes(5).format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"));
    dt10MinAgoString = OffsetDateTime.now(ZoneOffset.UTC).minusMinutes(10).format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"));
    dt1DayAgoString = OffsetDateTime.now(ZoneOffset.UTC).minusDays(1).format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"));
    dt5MinAfterString = OffsetDateTime.now(ZoneOffset.UTC).plusMinutes(5).format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"));

    dtNow = OffsetDateTime.parse(dtNowString);
    dt5MinAgo = OffsetDateTime.parse(dt5MinAgoString);
    dt10MinAgo = OffsetDateTime.parse(dt10MinAgoString);
    dt1DayAgo = OffsetDateTime.parse(dt1DayAgoString);
    dt5MinAfter = OffsetDateTime.parse(dt5MinAfterString);

    mt1 = new MeasurementType();
    mt1.setParameterSc(mtParamSc1);
    mt1.setParameter(mtParam1);
    mt1.setExaminationTypeSc(mtExamTypeSc1);
    mt1.setExaminationType(mtExamType1);
    mt1.setUnitSc(mtUnitSc1);
    mt1.setUnit(mtUnit1);

    mt2 = new MeasurementType();
    mt2.setParameterSc(mtParamSc2);
    mt2.setParameter(mtParam2);
    mt2.setExaminationTypeSc(mtExamTypeSc2);
    mt2.setExaminationType(mtExamType2);
    mt2.setUnitSc(mtUnitSc2);
    mt2.setUnit(mtUnit2);

    station1 = new Station();
    station1.setStationId(stationId);
    station1.setName(stationName);
    station1.setStationOwnerName(stationOwner);
    station1.setDescription(stationDescription);
    station1.setStationIdSav(stationIdSav);
    station1.setLocationX(locationX);
    station1.setLocationY(locationY);
    station1.setLocationSrid(locationSrid);
    station1.getMeasurementTypes().add(mt1);
    station1.getMeasurementTypes().add(mt2);

    m1 = new Measurement();
    m1.setStationId(stationId);
    m1.setIsCurrent(true);
    m1.setMeasurementPointNumber(measurementPoint1);
    m1.setExaminationTypeSc(mtExamTypeSc1);
    m1.setMeasurementDateTime(dt1DayAgo);

    event = new EventModel();
    event.setStationId(stationId);
    event.setMeasurementPointNumber(measurementPoint1);
    event.setUnitSc(mtUnitSc1);
    event.setParameterSc(mtParamSc1);
    event.setExaminationTypeSc(mtExamTypeSc1);
    event.setMeasurementDateTime(dt1DayAgoString);

    deleteAll(); //clean first if any leftovers

    addStations();
  }

  /**
   * Clean the inserted items
   */
  @AfterEach
  public void deleteAll() {

    if (!enableTest) {
      return;
    }

    dbService.deleteStationMeasurementTypeRelation(stationId);

    testDeleteMeasurement();

    testDeleteStation();

    testDeleteMeasurementType();

  }


  /**
   * Test Scenario 1 assumes these steps (in this order):
   * - received measurement added with TS NOW-10 min
   * - received measurement update with TS NOW-5 min
   * - received measurement deleted with TS NOW
   *
   * @throws SQLException
   * @throws InterruptedException
   */
  @Test
  public void testScenario1() throws SQLException, InterruptedException {

    if (!enableTest) {
      return;
    }

    ////////////// Received event MeasurementAdded
    event.setEventType(VandaHEventProcessor.EVENT_MEASUREMENT_ADDED);
    event.setResult(resultA);
    event.setRecordDateTime(dt10MinAgo);
    Measurement measurement = dbService.addMeasurementFromEvent(event);

    Measurement currentMeasurement = dbService.getMeasurement(measurement.getStationId(),
        measurement.getMeasurementPointNumber(),
        measurement.getExaminationTypeSc(),
        measurement.getMeasurementDateTime());

    List<Measurement> history = dbService.getMeasurementHistory(event.getStationId(),
        event.getMeasurementPointNumber(),
        event.getExaminationTypeSc(),
        event.getMeasurementDateTime());

    assertEquals(1, history.size());

    assertEquals(currentMeasurement, history.getFirst());
    assertTrue(currentMeasurement.getIsCurrent());
    assertEquals(stationId, currentMeasurement.getStationId());
    assertEquals(resultA, currentMeasurement.getValue());
    assertNull(currentMeasurement.getValueElevationCorrected());
    assertEquals(dt1DayAgo, currentMeasurement.getMeasurementDateTime());
    assertEquals(dt10MinAgo, currentMeasurement.getVandaEventTimestamp());

    //db status: 1 measurements from dt1DayAgo (TS = dt10MinAgo)

    ////////////// Received event MeasurementUpdated
    event.setEventType(VandaHEventProcessor.EVENT_MEASUREMENT_UPDATED);
    event.setResult(resultB); //same as the one from API
    event.setRecordDateTime(dt5MinAgo);
    event.setUnitSc(null);
    event.setParameterSc(null);
    Thread.sleep(300);
    measurement = dbService.updateMeasurementFromEvent(event);

    currentMeasurement = dbService.getMeasurement(event.getStationId(),
        event.getMeasurementPointNumber(),
        event.getExaminationTypeSc(),
        event.getMeasurementDateTime());

    history = dbService.getMeasurementHistory(event.getStationId(),
        event.getMeasurementPointNumber(),
        event.getExaminationTypeSc(),
        event.getMeasurementDateTime());

    assertEquals(2, history.size());
    Measurement oldM = history.getFirst();
    Measurement newM = history.getLast();

    assertFalse(oldM.getIsCurrent());
    assertEquals(stationId, oldM.getStationId());
    assertEquals(resultA, oldM.getValue());
    assertNull(oldM.getValueElevationCorrected());
    assertEquals(dt1DayAgo, oldM.getMeasurementDateTime());
    assertEquals(dt10MinAgo, oldM.getVandaEventTimestamp());

    assertTrue(newM.getIsCurrent());
    assertEquals(resultB, newM.getValue());
    assertNull(newM.getValueElevationCorrected());
    assertEquals(dt5MinAgo, newM.getVandaEventTimestamp());

    assertEquals(newM.getStationId(), oldM.getStationId());
    assertEquals(newM.getMeasurementDateTime(), oldM.getMeasurementDateTime());
    assertEquals(newM, currentMeasurement);
    assertTrue(oldM.getCreated().isBefore(newM.getCreated()));

    //db status: 2 measurements from dt1DayAgo (the active one with TS = dt5MinAgo)

    ////////////// Received event MeasurementDeleted
    event.setEventType(VandaHEventProcessor.EVENT_MEASUREMENT_DELETED);
    event.setResult(null);
    event.setUnitSc(null);
    event.setParameterSc(null);
    event.setRecordDateTime(dtNow);
    Thread.sleep(300);
    dbService.deleteMeasurementFromEvent(event);

    currentMeasurement = dbService.getMeasurement(event.getStationId(),
        event.getMeasurementPointNumber(),
        event.getExaminationTypeSc(),
        event.getMeasurementDateTime());

    assertNull(currentMeasurement);

    history = dbService.getMeasurementHistory(event.getStationId(),
        event.getMeasurementPointNumber(),
        event.getExaminationTypeSc(),
        event.getMeasurementDateTime());

    assertEquals(3, history.size());
    Measurement olderM = history.getFirst();
    oldM = history.get(1);
    newM = history.getLast();

    assertFalse(olderM.getIsCurrent());
    assertEquals(stationId, olderM.getStationId());
    assertEquals(resultA, olderM.getValue());
    assertNull(olderM.getValueElevationCorrected());
    assertEquals(dt1DayAgo, olderM.getMeasurementDateTime());
    assertEquals(dt10MinAgo, olderM.getVandaEventTimestamp());

    assertFalse(oldM.getIsCurrent());
    assertEquals(resultB, oldM.getValue());
    assertNull(oldM.getValueElevationCorrected());
    assertEquals(dt5MinAgo, oldM.getVandaEventTimestamp());

    assertFalse(newM.getIsCurrent());
    assertNull(newM.getValue());
    assertNull(newM.getValueElevationCorrected());
    assertEquals(dtNow, newM.getVandaEventTimestamp());

    assertEquals(olderM.getStationId(), oldM.getStationId());
    assertEquals(olderM.getMeasurementDateTime(), oldM.getMeasurementDateTime());
    assertTrue(olderM.getCreated().isBefore(oldM.getCreated()));
    assertTrue(oldM.getCreated().isBefore(newM.getCreated()));
    assertEquals(newM.getStationId(), oldM.getStationId());
    assertEquals(newM.getMeasurementDateTime(), oldM.getMeasurementDateTime());

    //db status: 3 measurements from dt1DayAgo (all are inactive)
  }


  /**
   * Test Scenario 2 assumes these steps (in this order):
   * - received measurement added  with TS NOW-10 min
   * - read measurement from API   at NOW min
   * - received measurement update with TS NOW+5
   *
   * @throws SQLException
   * @throws InterruptedException
   */
  @Test
  public void testScenario2() throws SQLException, InterruptedException {

    if (!enableTest) {
      return;
    }

    //////////////Received event MeasurementAdded
    event.setEventType(VandaHEventProcessor.EVENT_MEASUREMENT_ADDED);
    event.setResult(resultA);
    event.setRecordDateTime(dt10MinAgo);
    Measurement measurement = dbService.addMeasurementFromEvent(event);

    Measurement currentMeasurement = dbService.getMeasurement(measurement.getStationId(),
        measurement.getMeasurementPointNumber(),
        measurement.getExaminationTypeSc(),
        measurement.getMeasurementDateTime());

    List<Measurement> history = dbService.getMeasurementHistory(event.getStationId(),
        event.getMeasurementPointNumber(),
        event.getExaminationTypeSc(),
        event.getMeasurementDateTime());

    assertEquals(1, history.size());

    assertEquals(currentMeasurement, history.getFirst());
    assertTrue(currentMeasurement.getIsCurrent());
    assertEquals(stationId, currentMeasurement.getStationId());
    assertEquals(resultA, currentMeasurement.getValue());
    assertNull(currentMeasurement.getValueElevationCorrected());
    assertEquals(dt1DayAgo, currentMeasurement.getMeasurementDateTime());
    assertEquals(dt10MinAgo, currentMeasurement.getVandaEventTimestamp());

    //db status: 1 measurements from dt1DayAgo (TS = dt10MinAgo)

    //////////////add measurement as if it would be read from API
    Thread.sleep(300);
    m1.setValue(resultA);
    m1.setValueElevationCorrected(resultECA);
    Measurement insertedMeasurement = addMeasurement(m1);

    currentMeasurement = dbService.getMeasurement(insertedMeasurement.getStationId(),
        insertedMeasurement.getMeasurementPointNumber(),
        insertedMeasurement.getExaminationTypeSc(),
        insertedMeasurement.getMeasurementDateTime());

    assertEquals(insertedMeasurement, currentMeasurement);

    history = dbService.getMeasurementHistory(event.getStationId(),
        event.getMeasurementPointNumber(),
        event.getExaminationTypeSc(),
        event.getMeasurementDateTime());

    assertEquals(2, history.size());
    Measurement oldM = history.getFirst();
    Measurement newM = history.getLast();

    assertFalse(oldM.getIsCurrent());
    assertEquals(stationId, oldM.getStationId());
    assertEquals(resultA, oldM.getValue());
    assertNull(oldM.getValueElevationCorrected());
    assertEquals(dt1DayAgo, oldM.getMeasurementDateTime());
    assertEquals(dt10MinAgo, oldM.getVandaEventTimestamp());

    assertTrue(newM.getIsCurrent());
    assertEquals(resultA, newM.getValue());
    assertEquals(resultECA, newM.getValueElevationCorrected());
    assertNull(newM.getVandaEventTimestamp());

    assertTrue(oldM.getCreated().isBefore(newM.getCreated()));
    assertEquals(newM, currentMeasurement);
    assertEquals(newM.getStationId(), oldM.getStationId());
    assertEquals(newM.getMeasurementDateTime(), oldM.getMeasurementDateTime());

    //status: 2 measurement from dt1DayAgo (the active one with TS = null)

    ////////////// Received event MeasurementUpdated
    event.setEventType(VandaHEventProcessor.EVENT_MEASUREMENT_UPDATED);
    event.setResult(resultB); //same as the one from API
    event.setRecordDateTime(dt5MinAfter);
    event.setUnitSc(null);
    event.setParameterSc(null);
    Thread.sleep(300);
    measurement = dbService.updateMeasurementFromEvent(event);

    currentMeasurement = dbService.getMeasurement(event.getStationId(),
        event.getMeasurementPointNumber(),
        event.getExaminationTypeSc(),
        event.getMeasurementDateTime());

    history = dbService.getMeasurementHistory(event.getStationId(),
        event.getMeasurementPointNumber(),
        event.getExaminationTypeSc(),
        event.getMeasurementDateTime());

    assertEquals(3, history.size());
    Measurement olderM = history.getFirst();
    oldM = history.get(1);
    newM = history.getLast();

    assertFalse(olderM.getIsCurrent());
    assertEquals(stationId, olderM.getStationId());
    assertEquals(resultA, olderM.getValue());
    assertNull(olderM.getValueElevationCorrected());
    assertEquals(dt1DayAgo, olderM.getMeasurementDateTime());
    assertEquals(dt10MinAgo, olderM.getVandaEventTimestamp());

    assertFalse(oldM.getIsCurrent());
    assertEquals(resultECA, oldM.getValueElevationCorrected());
    assertEquals(resultA, oldM.getValue());
    assertNull(oldM.getVandaEventTimestamp());

    assertTrue(newM.getIsCurrent());
    assertEquals(resultB, newM.getValue());
    assertNull(newM.getValueElevationCorrected());
    assertEquals(dt5MinAfter, newM.getVandaEventTimestamp());

    assertEquals(olderM.getStationId(), oldM.getStationId());
    assertEquals(olderM.getMeasurementDateTime(), oldM.getMeasurementDateTime());
    assertTrue(olderM.getCreated().isBefore(oldM.getCreated()));
    assertTrue(oldM.getCreated().isBefore(newM.getCreated()));
    assertEquals(newM.getStationId(), oldM.getStationId());
    assertEquals(newM.getMeasurementDateTime(), oldM.getMeasurementDateTime());
    assertEquals(newM, currentMeasurement);

    //db status: 3 measurements from dt1DayAgo (the active one with TS = dt5MinAfter)

  }


  /**
   * Test Scenario 3 assumes these steps (in this order):
   * - received measurement added  with TS NOW-10 min
   * - received measurement update with TS NOW-5 min
   * - read measurement from API   at NOW min
   *
   * @throws SQLException
   * @throws InterruptedException
   */
  @Test
  public void testScenario3() throws SQLException, InterruptedException {

    if (!enableTest) {
      return;
    }

    //////////////Received event MeasurementAdded
    event.setEventType(VandaHEventProcessor.EVENT_MEASUREMENT_ADDED);
    event.setResult(resultA);
    event.setRecordDateTime(dt10MinAgo);
    Measurement measurement = dbService.addMeasurementFromEvent(event);

    Measurement currentMeasurement = dbService.getMeasurement(measurement.getStationId(),
        measurement.getMeasurementPointNumber(),
        measurement.getExaminationTypeSc(),
        measurement.getMeasurementDateTime());

    List<Measurement> history = dbService.getMeasurementHistory(event.getStationId(),
        event.getMeasurementPointNumber(),
        event.getExaminationTypeSc(),
        event.getMeasurementDateTime());

    assertEquals(1, history.size());

    assertEquals(currentMeasurement, history.getFirst());
    assertTrue(currentMeasurement.getIsCurrent());
    assertEquals(stationId, currentMeasurement.getStationId());
    assertEquals(resultA, currentMeasurement.getValue());
    assertNull(currentMeasurement.getValueElevationCorrected());
    assertEquals(dt1DayAgo, currentMeasurement.getMeasurementDateTime());
    assertEquals(dt10MinAgo, currentMeasurement.getVandaEventTimestamp());

    //db status: 1 measurements from dt1DayAgo (TS = dt10MinAgo)

    ////////////// Received event MeasurementUpdated
    event.setEventType(VandaHEventProcessor.EVENT_MEASUREMENT_UPDATED);
    event.setResult(resultB); //same as the one from API
    event.setRecordDateTime(dt5MinAgo);
    event.setUnitSc(null);
    event.setParameterSc(null);
    Thread.sleep(300);
    measurement = dbService.updateMeasurementFromEvent(event);

    currentMeasurement = dbService.getMeasurement(event.getStationId(),
        event.getMeasurementPointNumber(),
        event.getExaminationTypeSc(),
        event.getMeasurementDateTime());

    history = dbService.getMeasurementHistory(event.getStationId(),
        event.getMeasurementPointNumber(),
        event.getExaminationTypeSc(),
        event.getMeasurementDateTime());

    assertEquals(2, history.size());
    Measurement oldM = history.getFirst();
    Measurement newM = history.getLast();

    assertFalse(oldM.getIsCurrent());
    assertEquals(stationId, oldM.getStationId());
    assertEquals(resultA, oldM.getValue());
    assertNull(oldM.getValueElevationCorrected());
    assertEquals(dt1DayAgo, oldM.getMeasurementDateTime());
    assertEquals(dt10MinAgo, oldM.getVandaEventTimestamp());

    assertTrue(newM.getIsCurrent());
    assertEquals(resultB, newM.getValue());
    assertNull(newM.getValueElevationCorrected());
    assertEquals(dt5MinAgo, newM.getVandaEventTimestamp());

    assertTrue(oldM.getCreated().isBefore(newM.getCreated()));
    assertEquals(newM.getStationId(), oldM.getStationId());
    assertEquals(newM.getMeasurementDateTime(), oldM.getMeasurementDateTime());
    assertEquals(newM, currentMeasurement);

    //db status: 2 measurements from dt1DayAgo (the active one with TS = dt5MinAgo)

    //////////////add measurement as if it would be read from API
    Thread.sleep(300);
    m1.setValue(resultB);
    m1.setValueElevationCorrected(resultECB);
    Measurement insertedMeasurement = addMeasurement(m1);

    currentMeasurement = dbService.getMeasurement(insertedMeasurement.getStationId(),
        insertedMeasurement.getMeasurementPointNumber(),
        insertedMeasurement.getExaminationTypeSc(),
        insertedMeasurement.getMeasurementDateTime());

    assertEquals(insertedMeasurement, currentMeasurement);

    history = dbService.getMeasurementHistory(event.getStationId(),
        event.getMeasurementPointNumber(),
        event.getExaminationTypeSc(),
        event.getMeasurementDateTime());

    assertEquals(3, history.size());
    Measurement olderM = history.getFirst();
    oldM = history.get(1);
    newM = history.getLast();

    assertFalse(olderM.getIsCurrent());
    assertEquals(stationId, olderM.getStationId());
    assertEquals(resultA, olderM.getValue());
    assertNull(olderM.getValueElevationCorrected());
    assertEquals(dt1DayAgo, olderM.getMeasurementDateTime());
    assertEquals(dt10MinAgo, olderM.getVandaEventTimestamp());

    assertFalse(oldM.getIsCurrent());
    assertNull(oldM.getValueElevationCorrected());
    assertEquals(resultB, oldM.getValue());
    assertEquals(dt5MinAgo, oldM.getVandaEventTimestamp());

    assertTrue(newM.getIsCurrent());
    assertEquals(resultB, newM.getValue());
    assertEquals(resultECB, newM.getValueElevationCorrected());
    assertNull(newM.getVandaEventTimestamp());

    assertEquals(olderM.getStationId(), oldM.getStationId());
    assertTrue(olderM.getCreated().isBefore(oldM.getCreated()));
    assertTrue(oldM.getCreated().isBefore(newM.getCreated()));
    assertEquals(olderM.getMeasurementDateTime(), oldM.getMeasurementDateTime());
    assertEquals(newM.getStationId(), oldM.getStationId());
    assertEquals(newM.getMeasurementDateTime(), oldM.getMeasurementDateTime());
    assertEquals(newM, currentMeasurement);

    //status: 3 measurement from dt1DayAgo (the active one with TS = null)
  }

  /**
   * Test Scenario 4 assumes these steps (in this order):
   * - read measurement from API   at NOW min
   * - received measurement added  with TS NOW-10 min - drop delayed event
   * - received measurement update with TS NOW-5 min - drop delayed event
   *
   * @throws SQLException
   * @throws InterruptedException
   */
  @Test
  public void testScenario4() throws SQLException, InterruptedException {

    if (!enableTest) {
      return;
    }

    //////////////add measurement as if it would be read from API
    m1.setValue(resultB);
    m1.setValueElevationCorrected(resultECB);
    Measurement insertedMeasurement = addMeasurement(m1);

    Measurement currentMeasurement = dbService.getMeasurement(insertedMeasurement.getStationId(),
        insertedMeasurement.getMeasurementPointNumber(),
        insertedMeasurement.getExaminationTypeSc(),
        insertedMeasurement.getMeasurementDateTime());

    assertEquals(insertedMeasurement, currentMeasurement);

    List<Measurement> history = dbService.getMeasurementHistory(event.getStationId(),
        event.getMeasurementPointNumber(),
        event.getExaminationTypeSc(),
        event.getMeasurementDateTime());

    assertEquals(1, history.size());

    assertEquals(currentMeasurement, history.getFirst());
    assertTrue(currentMeasurement.getIsCurrent());
    assertEquals(stationId, currentMeasurement.getStationId());
    assertEquals(resultB, currentMeasurement.getValue());
    assertEquals(resultECB, currentMeasurement.getValueElevationCorrected());
    assertEquals(dt1DayAgo, currentMeasurement.getMeasurementDateTime());
    assertNull(currentMeasurement.getVandaEventTimestamp());

    //status: 1 measurement from dt1DayAgo (TS = null)

    ////////////// Received event MeasurementAdded
    event.setEventType(VandaHEventProcessor.EVENT_MEASUREMENT_ADDED);
    event.setResult(resultA);
    event.setRecordDateTime(dt10MinAgo);
    Thread.sleep(300);
    Measurement measurement = dbService.addMeasurementFromEvent(event);

    assertNull(measurement);

    currentMeasurement = dbService.getMeasurement(event.getStationId(),
        event.getMeasurementPointNumber(),
        event.getExaminationTypeSc(),
        event.getMeasurementDateTime());

    history = dbService.getMeasurementHistory(event.getStationId(),
        event.getMeasurementPointNumber(),
        event.getExaminationTypeSc(),
        event.getMeasurementDateTime());

    assertEquals(1, history.size());

    assertEquals(currentMeasurement, history.getFirst());
    assertEquals(insertedMeasurement, currentMeasurement);

    //db status: 1 measurement, same history, the event was delayed and so dropped

    ////////////// Received event MeasurementUpdated
    event.setEventType(VandaHEventProcessor.EVENT_MEASUREMENT_UPDATED);
    event.setResult(resultB); //same as the one from API
    event.setUnitSc(null);
    event.setParameterSc(null);
    event.setRecordDateTime(dt5MinAgo);
    Thread.sleep(300);
    measurement = dbService.updateMeasurementFromEvent(event);

    currentMeasurement = dbService.getMeasurement(event.getStationId(),
        event.getMeasurementPointNumber(),
        event.getExaminationTypeSc(),
        event.getMeasurementDateTime());

    history = dbService.getMeasurementHistory(event.getStationId(),
        event.getMeasurementPointNumber(),
        event.getExaminationTypeSc(),
        event.getMeasurementDateTime());

    assertEquals(1, history.size());

    assertEquals(currentMeasurement, history.getFirst());
    assertEquals(insertedMeasurement, currentMeasurement);
  }

  /**
   * Test Scenario 5 assumes these steps (in this order):
   * - received measurement update with TS NOW-5 min
   * - received measurement update with TS NOW min
   * - received measurement added  with TS NOW-10 min - drop delayed event
   *
   * @throws SQLException
   * @throws InterruptedException
   */
  @Test
  public void testScenario5() throws SQLException, InterruptedException {

    if (!enableTest) {
      return;
    }

    //////////////Received event MeasurementUpdated
    event.setEventType(VandaHEventProcessor.EVENT_MEASUREMENT_UPDATED);
    event.setResult(resultB); //same as the one from API
    event.setUnitSc(null);
    event.setParameterSc(null);
    event.setRecordDateTime(dt5MinAgo);
    Measurement measurement = dbService.updateMeasurementFromEvent(event);

    Measurement currentMeasurement = dbService.getMeasurement(measurement.getStationId(),
        measurement.getMeasurementPointNumber(),
        measurement.getExaminationTypeSc(),
        measurement.getMeasurementDateTime());

    List<Measurement> history = dbService.getMeasurementHistory(measurement.getStationId(),
        measurement.getMeasurementPointNumber(),
        measurement.getExaminationTypeSc(),
        measurement.getMeasurementDateTime());

    assertEquals(1, history.size());

    assertEquals(currentMeasurement, history.getFirst());
    assertTrue(currentMeasurement.getIsCurrent());
    assertEquals(stationId, currentMeasurement.getStationId());
    assertEquals(resultB, currentMeasurement.getValue());
    assertNull(currentMeasurement.getValueElevationCorrected());
    assertEquals(dt1DayAgo, currentMeasurement.getMeasurementDateTime());
    assertEquals(dt5MinAgo, currentMeasurement.getVandaEventTimestamp());

    //db status: 1 measurements from dt1DayAgo (TS = dt5MinAgo)

    //////////////Received event MeasurementUpdated
    event.setEventType(VandaHEventProcessor.EVENT_MEASUREMENT_UPDATED);
    event.setResult(resultC);
    event.setUnitSc(null);
    event.setParameterSc(null);
    event.setRecordDateTime(dtNow);
    measurement = dbService.updateMeasurementFromEvent(event);

    currentMeasurement = dbService.getMeasurement(measurement.getStationId(),
        measurement.getMeasurementPointNumber(),
        measurement.getExaminationTypeSc(),
        measurement.getMeasurementDateTime());

    history = dbService.getMeasurementHistory(measurement.getStationId(),
        measurement.getMeasurementPointNumber(),
        measurement.getExaminationTypeSc(),
        measurement.getMeasurementDateTime());

    assertEquals(2, history.size());
    Measurement oldM = history.getFirst();
    Measurement newM = history.getLast();

    assertFalse(oldM.getIsCurrent());
    assertEquals(stationId, oldM.getStationId());
    assertEquals(resultB, oldM.getValue());
    assertNull(oldM.getValueElevationCorrected());
    assertEquals(dt1DayAgo, oldM.getMeasurementDateTime());
    assertEquals(dt5MinAgo, oldM.getVandaEventTimestamp());

    assertTrue(newM.getIsCurrent());
    assertEquals(resultC, newM.getValue());
    assertNull(newM.getValueElevationCorrected());
    assertEquals(dtNow, newM.getVandaEventTimestamp());

    assertTrue(oldM.getCreated().isBefore(newM.getCreated()));
    assertEquals(newM.getStationId(), oldM.getStationId());
    assertEquals(newM.getMeasurementDateTime(), oldM.getMeasurementDateTime());
    assertEquals(newM, currentMeasurement);

    //db status: 2 measurements from dt1DayAgo (TS = dtNow)

    //////////////Received event MeasurementAdded
    event.setEventType(VandaHEventProcessor.EVENT_MEASUREMENT_ADDED);
    event.setResult(resultA);
    event.setUnitSc(mtUnitSc1);
    event.setParameterSc(mtParamSc1);
    event.setRecordDateTime(dt10MinAgo);
    Thread.sleep(300);
    measurement = dbService.addMeasurementFromEvent(event);

    assertNull(measurement);

    currentMeasurement = dbService.getMeasurement(event.getStationId(),
        event.getMeasurementPointNumber(),
        event.getExaminationTypeSc(),
        event.getMeasurementDateTime());

    history = dbService.getMeasurementHistory(event.getStationId(),
        event.getMeasurementPointNumber(),
        event.getExaminationTypeSc(),
        event.getMeasurementDateTime());

    assertEquals(2, history.size());
    oldM = history.getFirst();
    newM = history.getLast();

    assertFalse(oldM.getIsCurrent());
    assertEquals(stationId, oldM.getStationId());
    assertEquals(resultB, oldM.getValue());
    assertNull(oldM.getValueElevationCorrected());
    assertEquals(dt1DayAgo, oldM.getMeasurementDateTime());
    assertEquals(dt5MinAgo, oldM.getVandaEventTimestamp());

    assertTrue(newM.getIsCurrent());
    assertEquals(resultC, newM.getValue());
    assertNull(newM.getValueElevationCorrected());
    assertEquals(dtNow, newM.getVandaEventTimestamp());

    assertTrue(oldM.getCreated().isBefore(newM.getCreated()));
    assertEquals(newM.getStationId(), oldM.getStationId());
    assertEquals(newM.getMeasurementDateTime(), oldM.getMeasurementDateTime());
    assertEquals(newM, currentMeasurement);

    //db status: same history, delayed event dropped, 2 measurements from dt1DayAgo (TS = dtNow)
  }


  /**
   * Test Scenario 6 assumes these steps (in this order):
   * - received measurement added  with TS NOW-10 min
   * - received measurement update with TS NOW-5 min
   * - received measurement update with TS NOW min
   *
   * @throws SQLException
   * @throws InterruptedException
   */
  @Test
  public void testScenario6() throws SQLException, InterruptedException {

    if (!enableTest) {
      return;
    }

    //////////////Received event MeasurementAdded
    event.setEventType(VandaHEventProcessor.EVENT_MEASUREMENT_ADDED);
    event.setResult(resultA);
    event.setRecordDateTime(dt10MinAgo);
    Measurement measurement = dbService.addMeasurementFromEvent(event);

    Measurement currentMeasurement = dbService.getMeasurement(event.getStationId(),
        event.getMeasurementPointNumber(),
        event.getExaminationTypeSc(),
        event.getMeasurementDateTime());

    List<Measurement> history = dbService.getMeasurementHistory(event.getStationId(),
        event.getMeasurementPointNumber(),
        event.getExaminationTypeSc(),
        event.getMeasurementDateTime());

    assertEquals(1, history.size());

    assertEquals(currentMeasurement, history.getFirst());
    assertTrue(currentMeasurement.getIsCurrent());
    assertEquals(stationId, currentMeasurement.getStationId());
    assertEquals(resultA, currentMeasurement.getValue());
    assertNull(currentMeasurement.getValueElevationCorrected());
    assertEquals(dt1DayAgo, currentMeasurement.getMeasurementDateTime());
    assertEquals(dt10MinAgo, currentMeasurement.getVandaEventTimestamp());

    //db status: 1 measurements from dt1DayAgo (TS = dt10MinAgo)

    //////////////Received event MeasurementUpdated
    event.setEventType(VandaHEventProcessor.EVENT_MEASUREMENT_UPDATED);
    event.setResult(resultB);
    event.setUnitSc(null);
    event.setParameterSc(null);
    event.setRecordDateTime(dt5MinAgo);
    Thread.sleep(300);
    measurement = dbService.updateMeasurementFromEvent(event);

    currentMeasurement = dbService.getMeasurement(measurement.getStationId(),
        measurement.getMeasurementPointNumber(),
        measurement.getExaminationTypeSc(),
        measurement.getMeasurementDateTime());

    history = dbService.getMeasurementHistory(measurement.getStationId(),
        measurement.getMeasurementPointNumber(),
        measurement.getExaminationTypeSc(),
        measurement.getMeasurementDateTime());

    assertEquals(2, history.size());
    Measurement oldM = history.getFirst();
    Measurement newM = history.getLast();

    assertFalse(oldM.getIsCurrent());
    assertEquals(stationId, oldM.getStationId());
    assertEquals(resultA, oldM.getValue());
    assertNull(oldM.getValueElevationCorrected());
    assertEquals(dt1DayAgo, oldM.getMeasurementDateTime());
    assertEquals(dt10MinAgo, oldM.getVandaEventTimestamp());

    assertTrue(newM.getIsCurrent());
    assertEquals(resultB, newM.getValue());
    assertNull(newM.getValueElevationCorrected());
    assertEquals(dt5MinAgo, newM.getVandaEventTimestamp());

    assertTrue(oldM.getCreated().isBefore(newM.getCreated()));
    assertEquals(newM.getStationId(), oldM.getStationId());
    assertEquals(newM.getMeasurementDateTime(), oldM.getMeasurementDateTime());
    assertEquals(newM, currentMeasurement);

    //db status: 2 measurements from dt1DayAgo (TS = dtNow)


    //////////////Received event MeasurementUpdated
    event.setEventType(VandaHEventProcessor.EVENT_MEASUREMENT_UPDATED);
    event.setResult(resultA);
    event.setUnitSc(null);
    event.setParameterSc(null);
    event.setRecordDateTime(dtNow);
    measurement = dbService.updateMeasurementFromEvent(event);

    currentMeasurement = dbService.getMeasurement(measurement.getStationId(),
        measurement.getMeasurementPointNumber(),
        measurement.getExaminationTypeSc(),
        measurement.getMeasurementDateTime());

    history = dbService.getMeasurementHistory(measurement.getStationId(),
        measurement.getMeasurementPointNumber(),
        measurement.getExaminationTypeSc(),
        measurement.getMeasurementDateTime());


    assertEquals(3, history.size());
    Measurement olderM = history.getFirst();
    oldM = history.get(1);
    newM = history.getLast();

    assertFalse(olderM.getIsCurrent());
    assertEquals(stationId, olderM.getStationId());
    assertEquals(resultA, olderM.getValue());
    assertNull(olderM.getValueElevationCorrected());
    assertEquals(dt1DayAgo, olderM.getMeasurementDateTime());
    assertEquals(dt10MinAgo, olderM.getVandaEventTimestamp());

    assertFalse(oldM.getIsCurrent());
    assertNull(oldM.getValueElevationCorrected());
    assertEquals(resultB, oldM.getValue());
    assertEquals(dt5MinAgo, oldM.getVandaEventTimestamp());

    assertTrue(newM.getIsCurrent());
    assertEquals(resultA, newM.getValue());
    assertNull(newM.getValueElevationCorrected());
    assertEquals(dtNow, newM.getVandaEventTimestamp());

    assertEquals(olderM.getStationId(), oldM.getStationId());
    assertTrue(olderM.getCreated().isBefore(oldM.getCreated()));
    assertTrue(oldM.getCreated().isBefore(newM.getCreated()));
    assertEquals(olderM.getMeasurementDateTime(), oldM.getMeasurementDateTime());
    assertEquals(newM.getStationId(), oldM.getStationId());
    assertEquals(newM.getMeasurementDateTime(), oldM.getMeasurementDateTime());
    assertEquals(newM, currentMeasurement);

    //status: 3 measurement from dt1DayAgo (the active one with TS = null)
  }

  /**
   * Adding existing measurement again will add the data but generate a WARN
   * This is a hypothetical non-real case designed to test the code behaviour.
   * It should not occur in the real case.
   *
   * @throws SQLException
   * @throws InterruptedException
   * @throws SecurityException
   * @throws NoSuchFieldException
   * @throws IllegalAccessException
   * @throws IllegalArgumentException
   */
  @Test
  public void testAddExistingMeasurement()
      throws SQLException, InterruptedException, NoSuchFieldException, SecurityException,
      IllegalArgumentException, IllegalAccessException {

    if (!enableTest) {
      return;
    }

    // Get the real class and not the Spring proxy so we can access the private logger
    Class<?> databaseServiceClass = AopProxyUtils.ultimateTargetClass(dbService);
    Field loggerField = databaseServiceClass.getDeclaredField("logger");
    loggerField.setAccessible(true);
    loggerField.set(dbService, mock(Logger.class));
    Logger log = (Logger) loggerField.get(dbService);

    //////////////Received event MeasurementAdded
    event.setEventType(VandaHEventProcessor.EVENT_MEASUREMENT_ADDED);
    event.setResult(resultA);
    event.setRecordDateTime(dt10MinAgo);
    Measurement measurement1 = dbService.addMeasurementFromEvent(event);

    int measurementHistory = dbService.countMeasurementHistory(measurement1.getStationId(),
        measurement1.getMeasurementPointNumber(),
        measurement1.getExaminationTypeSc(),
        measurement1.getMeasurementDateTime());

    assertEquals(1, measurementHistory);

    //////////////Received event MeasurementAdded
    Thread.sleep(300);
    event.setRecordDateTime(dt5MinAgo); //this is fake to test hypothetical case
    Measurement measurement2 = dbService.addMeasurementFromEvent(event);

    measurementHistory = dbService.countMeasurementHistory(measurement2.getStationId(),
        measurement2.getMeasurementPointNumber(),
        measurement2.getExaminationTypeSc(),
        measurement2.getMeasurementDateTime());

    assertEquals(2, measurementHistory);

    verify(log, times(1)).warn(startsWith("Added existing measurement"));

  }


  /**
   * Test adding event where the examination type is missing from the DB should cast exception
   */
  @Test
  public void testAddMissingMeasurementType() {

    if (!enableTest) {
      return;
    }

    //////////////Received event MeasurementAdded
    event.setEventType(VandaHEventProcessor.EVENT_MEASUREMENT_ADDED);
    event.setResult(resultA);
    event.setExaminationTypeSc(mtExamTypeSc0);
    event.setRecordDateTime(dt10MinAgo);

    assertThrows(UnableToExecuteStatementException.class,
        () -> dbService.addMeasurementFromEvent(event));
  }


  /**
   * Receiving the same event twice should drop (ignore) the latest
   *
   * @throws SQLException
   * @throws InterruptedException
   * @throws SecurityException
   * @throws NoSuchFieldException
   * @throws IllegalAccessException
   * @throws IllegalArgumentException
   */
  @Test
  public void testReceivingEventTwice()
      throws SQLException, InterruptedException, NoSuchFieldException, SecurityException,
      IllegalArgumentException, IllegalAccessException {

    if (!enableTest) {
      return;
    }

    // Get the real class and not the Spring proxy so we can access the private logger
    Class<?> databaseServiceClass = AopProxyUtils.ultimateTargetClass(dbService);
    Field loggerField = databaseServiceClass.getDeclaredField("logger");
    loggerField.setAccessible(true);
    loggerField.set(dbService, mock(Logger.class));
    Logger log = (Logger) loggerField.get(dbService);

    //////////////Received event MeasurementAdded
    event.setEventType(VandaHEventProcessor.EVENT_MEASUREMENT_ADDED);
    event.setResult(resultA);
    event.setRecordDateTime(dt10MinAgo);
    Measurement measurement1 = dbService.addMeasurementFromEvent(event);

    assertNotNull(measurement1);

    int measurementHistory = dbService.countMeasurementHistory(event.getStationId(),
        event.getMeasurementPointNumber(),
        event.getExaminationTypeSc(),
        event.getMeasurementDateTime());

    assertEquals(1, measurementHistory);

    //same event again
    Thread.sleep(300);
    Measurement measurement2 = dbService.addMeasurementFromEvent(event);

    assertNull(measurement2);

    measurementHistory = dbService.countMeasurementHistory(event.getStationId(),
        event.getMeasurementPointNumber(),
        event.getExaminationTypeSc(),
        event.getMeasurementDateTime());

    assertEquals(1, measurementHistory);

    verify(log, times(1)).warn(startsWith("Delayed event received and dropped"));
  }


  /**
   * A delayed event will be dropped and will generate a WARN
   *
   * @throws SQLException
   * @throws InterruptedException
   * @throws SecurityException
   * @throws NoSuchFieldException
   * @throws IllegalAccessException
   * @throws IllegalArgumentException
   */
  @Test
  public void testDelayedAddEvent()
      throws SQLException, InterruptedException, NoSuchFieldException, SecurityException,
      IllegalArgumentException, IllegalAccessException {

    if (!enableTest) {
      return;
    }

    // Get the real class and not the Spring proxy so we can access the private logger
    Class<?> databaseServiceClass = AopProxyUtils.ultimateTargetClass(dbService);
    Field loggerField = databaseServiceClass.getDeclaredField("logger");
    loggerField.setAccessible(true);
    loggerField.set(dbService, mock(Logger.class));
    Logger log = (Logger) loggerField.get(dbService);

    //////////////Received event MeasurementUpdated
    event.setEventType(VandaHEventProcessor.EVENT_MEASUREMENT_UPDATED);
    event.setResult(resultB); //same as the one from API
    event.setUnitSc(null);
    event.setParameterSc(null);
    event.setRecordDateTime(dt5MinAgo);
    Measurement measurement1 = dbService.updateMeasurementFromEvent(event);

    int measurementHistory = dbService.countMeasurementHistory(measurement1.getStationId(),
        measurement1.getMeasurementPointNumber(),
        measurement1.getExaminationTypeSc(),
        measurement1.getMeasurementDateTime());

    assertEquals(1, measurementHistory);

    //////////////Received event MeasurementAdded
    event.setEventType(VandaHEventProcessor.EVENT_MEASUREMENT_ADDED);
    event.setResult(resultA);
    event.setRecordDateTime(dt10MinAgo);
    Thread.sleep(300);
    Measurement measurement2 = dbService.addMeasurementFromEvent(event);

    assertNull(measurement2);

    measurementHistory = dbService.countMeasurementHistory(event.getStationId(),
        event.getMeasurementPointNumber(),
        event.getExaminationTypeSc(),
        event.getMeasurementDateTime());

    assertEquals(1, measurementHistory);

    verify(log, times(1)).warn(startsWith("Delayed event received and dropped"));
  }


  /**
   * Updating a non-existent measurement will add the measurement but generate a WARN
   *
   * @throws SQLException
   * @throws NoSuchFieldException
   * @throws SecurityException
   * @throws IllegalAccessException
   * @throws IllegalArgumentException
   */
  @Test
  public void testUpdateNonExistingEvent()
      throws SQLException, NoSuchFieldException, SecurityException, IllegalArgumentException,
      IllegalAccessException {

    if (!enableTest) {
      return;
    }

    // Get the real class and not the Spring proxy so we can access the private logger
    Class<?> databaseServiceClass = AopProxyUtils.ultimateTargetClass(dbService);
    Field loggerField = databaseServiceClass.getDeclaredField("logger");
    loggerField.setAccessible(true);
    loggerField.set(dbService, mock(Logger.class));
    Logger log = (Logger) loggerField.get(dbService);

    //////////////Received event MeasurementUpdated
    event.setEventType(VandaHEventProcessor.EVENT_MEASUREMENT_UPDATED);
    event.setResult(resultB); //same as the one from API
    event.setUnitSc(null);
    event.setParameterSc(null);
    event.setRecordDateTime(dt5MinAgo);
    Measurement measurement1 = dbService.updateMeasurementFromEvent(event);

    int measurementHistory = dbService.countMeasurementHistory(measurement1.getStationId(),
        measurement1.getMeasurementPointNumber(),
        measurement1.getExaminationTypeSc(),
        measurement1.getMeasurementDateTime());

    assertEquals(1, measurementHistory);

    verify(log, times(1)).warn(startsWith("Update on nonexistent measurement"));
  }


  /**
   * Test update event where the examination type is missing from the DB should cast exception
   */
  @Test
  public void testUpdateMissingMeasurementType() {

    if (!enableTest) {
      return;
    }

    //////////////Received event MeasurementAdded
    event.setEventType(VandaHEventProcessor.EVENT_MEASUREMENT_UPDATED);
    event.setResult(resultA);
    event.setUnitSc(null);
    event.setParameterSc(null);
    event.setExaminationTypeSc(mtExamTypeSc0);
    event.setRecordDateTime(dt10MinAgo);

    assertThrows(UnableToExecuteStatementException.class,
        () -> dbService.updateMeasurementFromEvent(event));
  }


  /**
   * Delayed update event is dropped and a WARN is generated
   *
   * @throws InterruptedException
   * @throws SQLException
   * @throws SecurityException
   * @throws NoSuchFieldException
   * @throws IllegalAccessException
   * @throws IllegalArgumentException
   */
  @Test
  public void testDelayedUpdateEvent()
      throws InterruptedException, SQLException, NoSuchFieldException, SecurityException,
      IllegalArgumentException, IllegalAccessException {

    if (!enableTest) {
      return;
    }

    // Get the real class and not the Spring proxy so we can access the private logger
    Class<?> databaseServiceClass = AopProxyUtils.ultimateTargetClass(dbService);
    Field loggerField = databaseServiceClass.getDeclaredField("logger");
    loggerField.setAccessible(true);
    loggerField.set(dbService, mock(Logger.class));
    Logger log = (Logger) loggerField.get(dbService);

    //////////////add measurement as if it would be read from API
    m1.setValue(resultA);
    m1.setValueElevationCorrected(resultECA);
    addMeasurement(m1);

    //////////////Received event MeasurementUpdated
    event.setEventType(VandaHEventProcessor.EVENT_MEASUREMENT_UPDATED);
    event.setResult(resultA); //same as the one from API
    event.setUnitSc(null);
    event.setParameterSc(null);
    event.setRecordDateTime(dt5MinAgo);
    Thread.sleep(300);
    Measurement measurement2 = dbService.updateMeasurementFromEvent(event);

    assertNull(measurement2);

    verify(log, times(1)).warn(startsWith("Delayed event received and dropped"));
  }

  /**
   * Delete event on non-existing measurement should WARN
   *
   * @throws SecurityException
   * @throws NoSuchFieldException
   * @throws IllegalAccessException
   * @throws IllegalArgumentException
   */
  @Test
  public void testDeleteNonExistingMeasurement()
      throws NoSuchFieldException, SecurityException, IllegalArgumentException,
      IllegalAccessException {

    if (!enableTest) {
      return;
    }

    // Get the real class and not the Spring proxy so we can access the private logger
    Class<?> databaseServiceClass = AopProxyUtils.ultimateTargetClass(dbService);
    Field loggerField = databaseServiceClass.getDeclaredField("logger");
    loggerField.setAccessible(true);
    loggerField.set(dbService, mock(Logger.class));
    Logger log = (Logger) loggerField.get(dbService);

    //////////////Received event MeasurementDelete
    event.setEventType(VandaHEventProcessor.EVENT_MEASUREMENT_DELETED);
    event.setResult(null);
    event.setUnitSc(null);
    event.setParameterSc(null);
    event.setExaminationTypeSc(mtExamTypeSc1);
    event.setRecordDateTime(dt10MinAgo);
    dbService.deleteMeasurementFromEvent(event);

    verify(log, times(1)).warn(startsWith("Delete of nonexistent measurement"));
  }


  /**
   * A delayed event after a delete event will be dropped and no active record is registered
   *
   * @throws SQLException
   * @throws InterruptedException
   * @throws SecurityException
   * @throws NoSuchFieldException
   * @throws IllegalAccessException
   * @throws IllegalArgumentException
   */
  @Test
  public void testEarlyDeleteEvent()
      throws SQLException, InterruptedException, NoSuchFieldException, SecurityException,
      IllegalArgumentException, IllegalAccessException {

    if (!enableTest) {
      return;
    }

    // Get the real class and not the Spring proxy so we can access the private logger
    Class<?> databaseServiceClass = AopProxyUtils.ultimateTargetClass(dbService);
    Field loggerField = databaseServiceClass.getDeclaredField("logger");
    loggerField.setAccessible(true);
    loggerField.set(dbService, mock(Logger.class));
    Logger log = (Logger) loggerField.get(dbService);

    //////////////Received event MeasurementAdded
    event.setEventType(VandaHEventProcessor.EVENT_MEASUREMENT_ADDED);
    event.setResult(resultA);
    event.setRecordDateTime(dt10MinAgo);
    Measurement measurement1 = dbService.addMeasurementFromEvent(event);

    assertNotNull(measurement1);

    //////////////Received event MeasurementDelete
    event.setEventType(VandaHEventProcessor.EVENT_MEASUREMENT_DELETED);
    event.setResult(null);
    event.setUnitSc(null);
    event.setParameterSc(null);
    event.setExaminationTypeSc(mtExamTypeSc1);
    event.setRecordDateTime(dtNow);
    Thread.sleep(300);
    dbService.deleteMeasurementFromEvent(event);

    //////////////Received event MeasurementUpdated
    event.setEventType(VandaHEventProcessor.EVENT_MEASUREMENT_UPDATED);
    event.setResult(resultB);
    event.setUnitSc(null);
    event.setParameterSc(null);
    event.setRecordDateTime(dt5MinAgo);
    Thread.sleep(300);
    Measurement measurement2 = dbService.updateMeasurementFromEvent(event);

    assertNull(measurement2);

    Measurement currentMeasurement = dbService.getMeasurement(event.getStationId(),
        event.getMeasurementPointNumber(),
        event.getExaminationTypeSc(),
        event.getMeasurementDateTime());

    assertNull(currentMeasurement);

    verify(log, times(1)).warn(startsWith("Delayed event received and dropped"));
  }

  private void addStations() {

    dbService.addStation(station1);

    Station station = dbService.getStation(station1.getStationId());

    assertNotNull(station);

  }

  private Measurement addMeasurement(Measurement measurement) {

    dbService.inactivateMeasurementHistory(measurement);
    return dbService.addMeasurement(measurement);
  }


  private void testDeleteStation() {
    dbService.deleteStation(stationId);

    Station station = dbService.getStation(stationId);

    assertNull(station);
  }

  private void testDeleteMeasurementType() {
    dbService.deleteMeasurementType(mtExamTypeSc1);

    MeasurementType mt = dbService.getMeasurementType(mtExamTypeSc1);
    assertNull(mt);

    dbService.deleteMeasurementType(mtExamTypeSc2);

    mt = dbService.getMeasurementType(mtExamTypeSc2);
    assertNull(mt);
  }

  private void testDeleteMeasurement() {
    dbService.deleteMeasurementHard(stationId);

    Measurement m =
        dbService.getMeasurement(stationId, measurementPoint1, mtExamTypeSc1, dt1DayAgo);
    assertNull(m);
  }
}
