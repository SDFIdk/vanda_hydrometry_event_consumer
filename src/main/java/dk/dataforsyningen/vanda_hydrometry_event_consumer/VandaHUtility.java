package dk.dataforsyningen.vanda_hydrometry_event_consumer;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.event.Level;

import dk.dataforsyningen.vanda_hydrometry_event_consumer.model.EventModel;
import dk.dataforsyningen.vanda_hydrometry_event_consumer.model.Measurement;


/**
 * Utility class
 * 
 * @author Radu Dudici
 */
public class VandaHUtility {
	
	public static String BOLD_ON = "\033[1m";
	public static String ITALIC_ON = "\033[3m";
	public static String FORMAT_OFF = "\033[0m";
	
	/**
	 *  See {@link #logAndPrint(Logger log, Level level, boolean consolePrint, String message, Throwable exception)}
	 *  
	 * @param log
	 * @param level
	 * @param consolePrint
	 * @param message
	 */
	public static void logAndPrint(Logger log, Level level, boolean consolePrint, String message) {
		VandaHUtility.logAndPrint(log, level, consolePrint, message, null);
	}
	
	/**
	 * Function to add a message to the log file as well as print it to the console.
	 * 
	 * For logging to file use the log and level parameters.
	 * For printing to the console use consolePrint = true (unless level = ERROR) and level (optional). 
	 * 
	 * @param log the Logger object if the file logging is desired.
	 * @param level the log level used both by the file logger and as prefix for the printed message.
	 * @param consolePrint enables or disables the printing on the console unless it is an error (which is always printed).
	 * @param message (required) the message to be logged or printed.
	 * @param exception add an exception besides the message (both for logging and printing)
	 */
	public static void logAndPrint(Logger log, Level level, boolean consolePrint, String message, Throwable exception) {
		if (log != null) {
			if (Level.DEBUG.equals(level)) {
				log.debug(message);
			} else if (Level.INFO.equals(level)) {
				log.info(message);
			} else if (Level.WARN.equals(level)) {
				if (exception == null) {
					log.warn(message);
				} else {
					log.warn(message, exception);
				}
			} else if (Level.ERROR.equals(level)) {
				if (exception == null) {
					log.error(message);
				} else {
					log.error(message, exception);
				}
			} else if (Level.TRACE.equals(level)) {
				log.trace(message);
			}
		}
		if (level != null && !Level.INFO.equals(level)) { 
			message = level.name() + ": " + message;
		}
		if (exception != null) { 
			message += " [" + exception.getMessage() + "]";
		}
		if (consolePrint) {
			if (Level.ERROR.equals(level)) {
				System.err.println(message);
			} else {
				System.out.println(message);
			}
		}
	}
	
	/**
	 * Returns the value for the given key from a json given as string
	 * @param key
	 * @return value as string
	 */
	public static String valueFromJson(String json, String key) {
		String message = null;
		if (json == null || key == null) return null;
		try { 
			JSONObject bodyObj = new JSONObject(json);
			message = bodyObj.has(key) ? "" + bodyObj.get(key) : null;
		} catch (Exception ex) {}
		return message;
	}
	
	
	/**
	 * Takes a string representing a data with possible deviations from the standard form and converts into the form "yyyy-mm-ddThh:mm:ss.SSS[Z]"
	 * 
	 * Input format: yyyy-mm-ddThh:mm:ss.sssZ
	 * where there may be deviations like:
	 *   missing T or Z
	 *   month, day ,hours, minutes and seconds may have 1 or 2 digits or be missing
	 *   milliseconds may have 1 to 3 digit or missing 
	 *   
	 * @param date
	 * @param withoutSeconds if true the output will not contain the seconds
	 * @return date string yyyy-mm-ddThh:mm:ss.SSS[Z]
	 */
	private static String normalizeDate(String dateStr, boolean withoutSeconds) {
        String regex = "(\\d{4})-(\\d{1,2})-(\\d{1,2})(?:[T\\s](\\d{1,2}):(\\d{1,2})(?::(\\d{1,2}))?(?:\\.(\\d+))?)?(Z)?(?:\\+\\d{2}:\\d{2})?";
        Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(dateStr);

        if (matcher.matches()) {
        	String year = matcher.group(1);
            String month = String.format("%02d", Integer.parseInt(matcher.group(2)));
            String day = String.format("%02d", Integer.parseInt(matcher.group(3)));
            String hour = String.format("%02d", matcher.group(4) != null ? Integer.parseInt(matcher.group(4)) : 0);
            String minute = String.format("%02d", matcher.group(5) != null ? Integer.parseInt(matcher.group(5)) : 0);
            String seconds = String.format("%02d", matcher.group(6) != null ? Integer.parseInt(matcher.group(6)) : 0);
            String millis = String.format("%03d", matcher.group(7) != null ? Integer.parseInt(matcher.group(7)) : 0);
            boolean utc = matcher.group(8) != null;

            return withoutSeconds ? String.format("%s-%s-%sT%s:%s%s", year, month, day, hour, minute, utc ? "Z" : "")
            		: String.format("%s-%s-%sT%s:%s:%s.%s%s", year, month, day, hour, minute, seconds, millis, utc ? "Z" : "");
        } else {
            return null;
        }
	}
	
	
	/**
	 * Parses the date/time string given by the user into an OffsetDateTime
	 * without seconds and milliseconds in UTC time zone accepted by the API.
	 * 
	 * @param API date/time string
	 * @return OffsetDateTime in UTC
	 */
	public static OffsetDateTime parseForAPI(String dateStr) throws DateTimeParseException {
		if (dateStr == null) return null;
		OffsetDateTime odt = null;
		String normalized = normalizeDate(dateStr, true);
		if (normalized.endsWith("Z")) {
			odt = OffsetDateTime.parse(normalized);
		} else {
			LocalDateTime ldt = LocalDateTime.parse(normalized, DateTimeFormatter.ISO_DATE_TIME);
			odt = ldt.atZone(ZoneId.systemDefault()).toOffsetDateTime() //convert LDT to ODT using the system's default TZ rules
					.withOffsetSameInstant(ZoneOffset.UTC); //convert ODT to UTC timezone
		}
		return odt;
	}
	
	/**
	 * Same as {@link #parseForAPI} but it returns seconds and milliseconds
	 * 
	 * @param dateStr
	 * @return OffsetDateTime object in UTC
	 */
	public static OffsetDateTime parseToUtcOffsetDateTime(String dateStr) {
		if (dateStr == null) return null;
		OffsetDateTime odt = null;
		try {
			String normalized = normalizeDate(dateStr, false);
			if (normalized.endsWith("Z")) {
				odt = OffsetDateTime.parse(normalized);
			} else {
				LocalDateTime ldt = LocalDateTime.parse(normalized, DateTimeFormatter.ISO_DATE_TIME);
				odt = ldt.atZone(ZoneId.systemDefault()).toOffsetDateTime() //convert LDT to ODT using the system's default TZ rules
						.withOffsetSameInstant(ZoneOffset.UTC); //convert ODT to UTC timezone
			}
		} catch (Exception ex) {
			//Do nothing
		}
		return odt;
	}
	
	/**
	 * Convert a UTC date string into a Date.
	 * see {@link #parseToUtcOffsetDateTime(String dateStr)}
	 * 
	 * @param dateStr UTC date string
	 * @return Date
	 */
	public static Date parseUtcDate(String dateStr) {
		if (dateStr == null) return null;
		
		OffsetDateTime odt = parseToUtcOffsetDateTime(dateStr);
		
		return odt != null ? Date.from(odt.toInstant()) : null;
	}
	
	/**
	 * Converts a Java Date object representing a date to an OffsetDateTime using the UTC time zone
	 * 
	 *  @param date
	 * @return OffsetDateTime in UTC
	 */
	public static OffsetDateTime dateToOfssetDateTimeUtc(Date date) {
		return date != null ? date.toInstant().atOffset(ZoneOffset.UTC) : null;
	}
	
	/**
	 * Converts a Java Date object to an OffsetDateTime using the local time zone
	 * 
	 * @param date 
	 * @return OffsetDateTime in local time zone
	 */
	public static OffsetDateTime dateToOfssetDateTimeLocalZone(Date date) {
		return date != null ? date.toInstant().atOffset(ZoneOffset.UTC).atZoneSameInstant(ZoneId.systemDefault()).toOffsetDateTime() : null;
	}
	
	/**
	 * Converts an SQL Timestamp to an OffsetDateTime object
	 * 
	 * @param ts the Timestamp 
	 * @param utc if true the date object will be set to UTC time zone
	 * @return OffsetDateTime in local or UTC time zone
	 */
	public static OffsetDateTime toOffsetDate(Timestamp ts, boolean utc) {
		if (ts == null) return null;
		return utc ? dateToOfssetDateTimeUtc(new Date(ts.getTime())) 
				: dateToOfssetDateTimeLocalZone(new Date(ts.getTime()));
	}
	
	public static OffsetDateTime timestampToOffsetDateTimeUtc(long ts) {
		return OffsetDateTime.ofInstant(Instant.ofEpochMilli(ts), ZoneOffset.UTC);
	}
	
	
	public static int toInt(String s) {
		int v = 0;
		if (s != null) {
			try {
				v = Integer.parseInt(s);
			} catch (NumberFormatException e) {}
		}
		return v;
	}
	
	public static double toDouble(String s) {
		double v = 0;
		if (s != null) {
			try {
				v = Double.parseDouble(s);
			} catch (NumberFormatException e) {}
		}
		return v;
	}

}
