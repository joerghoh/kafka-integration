package de.joerghoh.aem.kafkasample.consumer.impl;

import java.util.Properties;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.joerghoh.aem.kafka.consumer.AbstractConsumer;
import de.joerghoh.aem.kafka.consumer.IncompleteKafkaConfigurationException;
import de.joerghoh.aem.kafkasample.dto.news.News;

public class NewsConsumer extends AbstractConsumer<String,News> {
	
	
	private static final Logger log = LoggerFactory.getLogger(NewsConsumer.class);

	@Override
	protected void prepareProperties(Properties props) {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected void handleRecords(ConsumerRecords<String, News> records) {
		// TODO Auto-generated method stub
		
	}
	
	
	@Activate
	protected void activate(ComponentContext context) throws IncompleteKafkaConfigurationException {
		start (context);
		log.info("NewsConsumer started");
	}
	
	@Deactivate 
	protected void deactivate () {
		stop ();
		log.info("NewsConsumer stopped");
	}

}
