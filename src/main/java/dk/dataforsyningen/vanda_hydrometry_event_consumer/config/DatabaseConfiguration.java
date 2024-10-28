package dk.dataforsyningen.vanda_hydrometry_event_consumer.config;

import javax.sql.DataSource;

import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.statement.SqlStatements;
//import org.jdbi.v3.jackson2.Jackson2Plugin;
import org.jdbi.v3.postgis.PostgisPlugin;
import org.jdbi.v3.postgres.PostgresPlugin;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.TransactionAwareDataSourceProxy;

import dk.dataforsyningen.vanda_hydrometry_event_consumer.dao.MeasurementDao;
import dk.dataforsyningen.vanda_hydrometry_event_consumer.dao.MeasurementTypeDao;
import dk.dataforsyningen.vanda_hydrometry_event_consumer.dao.StationDao;

/**
 * Configure the JDBI object with values from properties file.
 */
@Configuration
public class DatabaseConfiguration {
	
	private static final Logger log = LoggerFactory.getLogger(DatabaseConfiguration.class);
			
    /**
     * The SQL data source that Jdbi will connect to. https://jdbi.org/#_spring_5
     *
     * @return DataSource
     */
    @Bean(name = "vandaHydroDataDS")
    @Primary
    @ConfigurationProperties(prefix = "spring.datasource")
    public DataSource dataSource() {
        return new DriverManagerDataSource();
    }

    /**
     * Creates the JDBI object
     * 
     * @param ds
     * @return jdbi bean
     */
	@Bean
    public Jdbi jdbi(@Qualifier("vandaHydroDataDS") DataSource ds) {  
				
        TransactionAwareDataSourceProxy proxy = new TransactionAwareDataSourceProxy(ds);        
        Jdbi jdbi = Jdbi.create(proxy)
        		.installPlugin(new PostgresPlugin())
                .installPlugin(new PostgisPlugin())
                .installPlugin(new SqlObjectPlugin());
        
        return jdbi;
    }
		
	@Bean
	public StationDao stationDao(Jdbi jdbi) {
		return jdbi.onDemand(StationDao.class);
	}
	
	@Bean
	public MeasurementDao measurementDao(Jdbi jdbi) {
		return jdbi.onDemand(MeasurementDao.class);
	}
	
	@Bean
	public MeasurementTypeDao measurementTypeDao(Jdbi jdbi) {
		return jdbi.onDemand(MeasurementTypeDao.class);
	}
}
