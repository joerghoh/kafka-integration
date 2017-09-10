package de.joerghoh.aem.kafka.impl.sample;

import java.util.Dictionary;
import java.util.Iterator;
import java.util.Properties;

import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Service;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.sling.commons.json.JSONException;
import org.apache.sling.commons.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.joerghoh.aem.kafka.consumer.AbstractConsumer;

@Component(label="News importer", metatype=true)
@Service()
public class Importer extends AbstractConsumer<String, String> {
	
	private static final Logger LOG = LoggerFactory.getLogger(Importer.class);

	@Override
	public Properties setupCustomProperties () {
		Properties props = new Properties();
		props.put("key.deserializer", org.apache.kafka.common.serialization.StringDeserializer.class);
		props.put("value.deserializer", org.apache.kafka.common.serialization.StringDeserializer.class);
		return props;
		
	}

	@Override
	public void handleRecords(ConsumerRecords<String, String> records) {
		
		Iterator<ConsumerRecord<String, String>> iter = records.iterator();
		while (iter.hasNext()) {
			ConsumerRecord<String,String> record = iter.next();
			String dataJson = record.value();
			LOG.info("2a: {}", dataJson);
			JSONObject data;
			try {
				data = new JSONObject(dataJson);
				LOG.info("imported log with ID {}", data.get("newsId"));
			} catch (JSONException e) {
				LOG.warn("Incomplete JSON record ",e);
			}
			
			
			
		}
		
	}
	


}
