package dk.dataforsyningen.vanda_hydrometry_event_consumer.config;

import dk.dataforsyningen.vanda_hydrometry_event_consumer.VandaHEventConsumerApplication;
import java.util.ArrayList;
import java.util.List;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
public class VandaHEventConsumerConfig {

  //enables DAO and database service testing - needs a DB connection
  @Value("${dk.dataforsyningen.vanda_hydrometry_event_consumer.database.test:#{false}}")
  public boolean enableDbTest; //used only within testing
  private String command;
  @Value("${displaydata:#{null}}")
  private String displayData;  //boolean
  @Value("${displayall:#{null}}")
  private String displayAll;  //boolean
  @Value("${savedb:#{null}}")
  private String saveDb;  //boolean
  @Value("${events:#{null}}")
  private String events;
  @Value("${dk.dataforsyningen.vanda_hydrometry_event_consumer.reportPeriodSec:#{0}}")
  private int reportPeriodSec;
  @Value("${dk.dataforsyningen.vanda_hydrometry_event_consumer.examinationTypeSc:#{null}}")
  private String examinationTypeSc;

  /**
   * parse the arguments list and retrieves the first command
   *
   * @param args
   * @return the command
   */
  public String parseCommands(String... args) {
    for (String arg : args) {
      if (arg != null && !arg.isEmpty() && !arg.startsWith("--")) {
        //do not consider application's startup class as a command.
        if (!arg.equalsIgnoreCase(VandaHEventConsumerApplication.class.getCanonicalName())) {
          command = (arg.toLowerCase());
          break;
        }
      }
    }
    return command;
  }

  public String getCommand() {
    return command;
  }

  public void setCommand(String command) {
    this.command = command;
  }

  public boolean isDisplayData() {
    return displayData != null;
  }

  public boolean isDisplayAll() {
    return displayAll != null;
  }

  public boolean isSaveDb() {
    return saveDb != null;
  }

  public boolean processAdditions() {
    return events == null || events.toLowerCase().indexOf('a') != -1;
  }

  public boolean processUpdates() {
    return events == null || events.toLowerCase().indexOf('u') != -1;
  }

  public boolean processDeletions() {
    return events == null || events.toLowerCase().indexOf('d') != -1;
  }

  public int getReportPeriodSec() {
    return reportPeriodSec;
  }

  public boolean isEnableDbTest() {
    return enableDbTest;
  }

  public List<Integer> getExaminationTypeSc() {
    ArrayList<Integer> output = new ArrayList<>();
    if (examinationTypeSc != null && !examinationTypeSc.isEmpty()) {
      String[] values = examinationTypeSc.split(",");
      for (String s : values) {
        try {
          output.add(Integer.parseInt(s));
        } catch (NumberFormatException ex) {
          //do nothing
        }
      }
    }
    return output;
  }

  @Override
  public String toString() {
    return "VandaHEventConsumerConfig [\nexaminationTypeSc=" + examinationTypeSc +
        ",\ncommand=" + getCommand() +
        ",\nisDisplayAll=" + isDisplayAll() +
        ",\nisDisplayData=" + isDisplayData() +
        ",\nisSaveDb=" + isSaveDb() +
        ",\ngetReportPeriodSec=" + getReportPeriodSec() +
        ",\nevents=" + events
        + "\n]";
  }


}
