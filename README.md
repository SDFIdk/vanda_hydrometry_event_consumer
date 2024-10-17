# vanda_hydrometry_event_consumer

Reads and process events from DMP VanDa Hydro Event Hub.

Find the description and details at Danmarks Miljø Portalen VanDa Wiki [https://github.com/danmarksmiljoeportal/VanDa/wiki/Hydro-Event-Hub](https://github.com/danmarksmiljoeportal/VanDa/wiki/Hydro-Event-Hub).


## Description

The application is a command line application that upon execution connects to DMP Vanda Event Hub through a Kafka client, receives the events and process them by adding or updating measurements in the DB.

DMP and DB connections are configured in the _application.properties_ file. The database DAO queries are based on Postgresql (with Postgis extension) database.

## Usage

This section shows the operations and parameters that can be used with the application. In order to run the application from the command line (console) use this command:

	java -jar vanda_hydrometry_event_consumer.jar

In order to tell the application what to do the following parameters should be used further (added to the command line). Parameters may be commands,or options that starts with "--" and may have or not a value after the "=" sign.