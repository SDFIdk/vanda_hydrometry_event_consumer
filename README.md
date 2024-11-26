# vanda_hydrometry_event_consumer

Reads and process events from DMP VanDa Hydro Event Hub.

Find the description and details at Danmarks Miljø Portalen VanDa Wiki [https://github.com/danmarksmiljoeportal/VanDa/wiki/Hydro-Event-Hub](https://github.com/danmarksmiljoeportal/VanDa/wiki/Hydro-Event-Hub).


## Description

The application is a command line application that upon execution connects to DMP Vanda Event Hub through a Kafka client, receives the events and process them by adding or updating measurements in the DB.

The events may be:
* Measurement Added
* Measurement Updated
* Measurement Deleted

Only examination types 25 and 27 are considered and the records do not get updated or deleted but marked as not active and new records are created in case of addition and update events.

DMP and DB connections are configured in the _application.properties_ file. The database DAO queries are based on Postgresql (with Postgis extension) database.

## How it works

This section describes the processing logic (the steps) behind each event type.

### MeasurementAdded

* The event's time stamp (TS) is checked against the DB records belonging to this measurement's history to see if it is delayed. Delayed means that there is already registered a record with TS later than this event's TS. This can happen because the order of events in the stream is not guaranteed to be in the sorted order after TS.
* If the event is delayed, it is dropped and a warning message is issued.
* If the event is not delayed then all the records from the history are deactivated (is_current = false) and ...
* the new event is added as the new active (is_current = true) record in the history.

Normally the addition event should be the 1st event receive concerning a certain measurement and it should be received and processed only once. However this is not always true since the event consumer can restart and reset the stream and so the event is being processed more than once. In this case a warning is issued but the event is still added to the measurement history as a new active record. 

### MeasurementUpdated

* The event's time stamp (TS) is checked against the DB records belonging to this measurement's history to see if it is delayed. Delayed means that there is already registered a record with TS later than this event's TS. This can happen because the order of events in the stream is not guaranteed to be in the sorted order after TS.
* If the event is delayed, it is dropped and a warning message is issued.
* If the event is not delayed then all the records from the history are deactivated (is_current = false) and ...
* the new event is added as the new active (is_current = true) record in the history.

Normally an update event should always happen after an addition event. So if an update is processed while there is no record in the history a warning is issued but the event is still added to the history as the new active record.

### MeasurementDeleted

* Try to deactivate all the history
* If no record was affected it means that there is no history so no registration of this event. In this case a warning is issued that a delete event is attempted on a non existing measurement.
* If the deactivation succeeded (at least one record was deactivated) then the deletion event is added as a new record in the history (in order to have a registration of its latest TS) but the record will not be active (is_current = false).

A delete event can never be delayed since it is always the last one. However it may be received before other delayed event (in which case the latest should be dropped).

### Delayed events

Because the events in the stream get received through several parallel partition it is not a guaranty that the receiving order of the events is the real order of the events as given by their TS. A delayed event is an event whose TS upon processing is before any of the registered events in the history (usually before the last one registered). Therefore in order to cope with this, the delayed events are disconsidered (dropped) so that the latest event in the history remains active (current).

Note that the API retrieved data, since they are not events, they do not contain a TS but they have a creation TS (when they were retrieved, which is also the latest true value at that moment). Therefore the event's TS is compared with record's creation date when an event has to be compared against an API data to determine if it is delayed.

### Logging

Activate debug level for logging to get received raw event into the log file. Activate trace level to get both raw events and executed queries into the log file.

The property "dk.dataforsyningen.vanda_hydrometry_event_consumer.loggingEvents" in application.properties can be set to "all" so that all events are logged or to "processed" so only the processed events are logged.

Running statistics are displayed and logged with level INFO when events are received but with a minimum period define by "dk.dataforsyningen.vanda_hydrometry_event_consumer.reportPeriodSec" in application.properties. 
If the time between events is greater the statistic reporting period can be longer.

Displayed statistics are of the form:

> Received  _eventCount / totalEventCount_  events (processed a,u,d:  _eventCount / totalEventCount_ , _eventCount / totalEventCount_ , _eventCount / totalEventCount_ ); min/max for partition  _P_ :  _minOffset / maxOffset_ ; event creation timestamp between  _minDateTime_  and  _maxDateTime_  within  _N_  sec

This will show the number of events within the last period of N seconds and the total number of events for the entire time the application was running. It also shows the number of events (as well as the total) divided by the event type: addition, updates or deletes ('a,u,d'). While the received counters shows all received events (both accepted/processed as well as ignored), the a,u,d will only count the accepted (i.e. processed) events.

It will also show the minimum and maximum offset value for each partition P, for which events have been received within the last period of N seconds.

It will also show the minium and maximum event timestamp within the period.

## Usage

This section shows the operations and parameters that can be used with the application. In order to run the application from the command line (console) use this command:

	java -jar vanda_hydrometry_event_consumer.jar

In order to tell the application what to do the following parameters should be used further (added to the command line). Parameters may be commands,or options that starts with "--" and may have or not a value after the "=" sign.

### Start

In order to start the message listener use the command:

	start
	
### Events filtering

In order to filter which event types to accept and process use the option --events and add the event type as the value. Use event type 'a' for measurement additions, 'u' for measurement updates and 'd' for measurement deletions. Use combinations or all 3 at once (as 'aud', which is the default too) as in the example. If the option is not used all events are processed by default

	start --events=aud
	
### Display data

In order to display the received (decoded into the internal model) data in the console (or redirect the output into a file) so the user can inspect it, use:

	start --displayData	
	
In order to display the raw json data received from the event hub use the following:

	start --displayRawData	
	
... however this will only display the raw data of the processed events. In order to display the raw data of all received events use:

	start --displayAll	
	
### Save to DB

In order to save the events into the DB (using the config from properties file) use the parameter "saveDb". The save operation wil ladd or update existing measurements based on the received event.

	start --saveDb
	
If the event is of type "measurement added" but the measurement already exists it will be updated. If the event is of type "measurement updated" but the measurement does not exist it will be ignored and a warning message logged. If the measurement type is "measurement updated" and the measurement exists, a new record is created and made "active" while the existing record will be made "not active".
