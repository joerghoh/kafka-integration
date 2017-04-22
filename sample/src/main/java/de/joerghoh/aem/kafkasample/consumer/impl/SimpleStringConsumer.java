package de.joerghoh.aem.kafkasample.consumer.impl;

import java.util.Iterator;
import java.util.Properties;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Service;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.joerghoh.aem.kafka.consumer.AbstractConsumer;
import de.joerghoh.aem.kafka.consumer.IncompleteKafkaConfigurationException;

@Component(inherit=true,metatype=true)
@Service
public class SimpleStringConsumer extends AbstractConsumer<String,String> {
	
	
	private static final Logger log = LoggerFactory.getLogger(SimpleStringConsumer.class);

	@Override
	protected void prepareProperties(Properties props) {
	     props.put("key.deserializer", org.apache.kafka.common.serialization.StringDeserializer.class);
	     props.put("value.deserializer", org.apache.kafka.common.serialization.StringDeserializer.class);
	     //props.put("auto.offset.reset", "earliest");
		
	}

	@Override
	protected void handleRecords(ConsumerRecords<String, String> records) {
		Iterator<ConsumerRecord<String,String>> iter = records.iterator();
		while (iter.hasNext()) {
			ConsumerRecord<String,String> record = iter.next();
			log.info ("Received message with key= {} and value={}",record.key(),record.value());
		}
	}
	
	@Activate
	protected void activate(ComponentContext context) throws IncompleteKafkaConfigurationException {
		start (context);
		log.info("SimpleStringConsumer started");
	}
	
	@Deactivate 
	protected void deactivate () {
		stop ();
		log.info("SimpleStringConsumer stopped");
	}

}
