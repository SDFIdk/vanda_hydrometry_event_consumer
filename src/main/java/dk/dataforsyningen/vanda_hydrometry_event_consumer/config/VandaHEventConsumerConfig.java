package dk.dataforsyningen.vanda_hydrometry_event_consumer.config;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import dk.dataforsyningen.vanda_hydrometry_event_consumer.VandaHEventConsumerApplication;

@Configuration
public class VandaHEventConsumerConfig {

	private String command;
	
	@Value("${verbose:#{null}}")
	private String verbose;  //boolean
	
	@Value("${displaydata:#{null}}")
	private String displayData;  //boolean
	
	@Value("${displayrawdata:#{null}}")
	private String displayRawData;  //boolean
	
	@Value("${savedb:#{null}}")
	private String saveDb;  //boolean
	
	@Value("${dk.dataforsyningen.vanda_hydrometry_event_consumer.reportPeriodSec:#{0}}")
	private int reportPeriodSec;
	
	@Value("${dk.dataforsyningen.vanda_hydrometry_event_consumer.examinationTypeSc:#{null}}")
	private String examinationTypeSc;
	
	/**
	 * parse the arguments list and retrieves the first command
	 * @param args
	 * @return the command
	 */
	public String parseCommands(String... args) {
		for(String arg : args) {
			if (arg != null && arg.length() > 0 && !arg.startsWith("--")) {
				//do not consider application's startup class as a command.
				if (!arg.toLowerCase().equals(VandaHEventConsumerApplication.class.getCanonicalName().toLowerCase())) {
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
	
	public boolean isVerbose() {
		return verbose != null;
	}
	
	public boolean isDisplayData() {
		return displayData != null;
	}
	
	public boolean isDisplayRawData() {
		return displayRawData != null;
	}
	
	public boolean isSaveDb() {
		return saveDb != null;
	}
	
	public int getReportPeriodSec() {
		return reportPeriodSec;
	}
	
	public List<Integer> getExaminationTypeSc() {
		ArrayList<Integer> output = new ArrayList<>();
		if (examinationTypeSc != null && examinationTypeSc.length() > 0) {
			String[] vals = examinationTypeSc.split(";");
			for(String s : vals) {
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
		return "VandaHEventConsumerConfig [examinationTypeSc=" + examinationTypeSc + ", command=" + getCommand()
				+ ", isVerbose=" + isVerbose() + ", isDisplayData=" + isDisplayData() + ", isDisplayRawData="
				+ isDisplayRawData() + ", isSaveDb=" + isSaveDb() + ", getReportPeriodSec=" + getReportPeriodSec()
				+ "]";
	}


	
	
}
