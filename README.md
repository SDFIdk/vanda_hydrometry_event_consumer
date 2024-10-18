# vanda_hydrometry_event_consumer

Reads and process events from DMP VanDa Hydro Event Hub.

Find the description and details at Danmarks Miljø Portalen VanDa Wiki [https://github.com/danmarksmiljoeportal/VanDa/wiki/Hydro-Event-Hub](https://github.com/danmarksmiljoeportal/VanDa/wiki/Hydro-Event-Hub).


## Description

The application is a command line application that upon execution connects to DMP Vanda Event Hub through a Kafka client, receives the events and process them by adding or updating measurements in the DB.

The events may be:
* Measurement Added
* Measurement Updated
* Measurement Deleted

Only examination types 25 and 27 are considered.

DMP and DB connections are configured in the _application.properties_ file. The database DAO queries are based on Postgresql (with Postgis extension) database.

## Usage

This section shows the operations and parameters that can be used with the application. In order to run the application from the command line (console) use this command:

	java -jar vanda_hydrometry_event_consumer.jar

In order to tell the application what to do the following parameters should be used further (added to the command line). Parameters may be commands,or options that starts with "--" and may have or not a value after the "=" sign.

### Start

In order to start the message listener use the command:

	start
	
### Display data

In order to display the received (decoded into the internal model) data in the console (or redirect the output into a file) so the user can inspect it, use:

	start --displayData	
	
In order to display the raw json data received from the event hub use:

	start --displayRawData	
	
### More output

In order to display more details about the execution of the program or warnings in the console use the parameter "verbose". 

	start --verbose	
	
### Save to DB

In order to save the events into the DB (using the config from properties file) use the parameter "saveDb". The save operation wil ladd or update existing measurements based on the received event.

	start --saveDb
	
If the event is of type "measurement added" but the measurement already exists it will be updated. If the event is of type "measurement updated" but the measurement does not exist it will be ignored and a warning message logged.
