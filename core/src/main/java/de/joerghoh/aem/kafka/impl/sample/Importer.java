package de.joerghoh.aem.kafka.impl.sample;

import java.util.Iterator;
import java.util.Properties;

import javax.jcr.Session;

import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.Service;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.commons.json.JSONException;
import org.apache.sling.commons.json.JSONObject;
import org.apache.sling.jcr.api.SlingRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.day.cq.wcm.api.PageManager;
import com.day.cq.wcm.api.WCMException;

import de.joerghoh.aem.kafka.consumer.AbstractConsumer;

	@Component(label="News importer", metatype=true, immediate=true)
	@Service()
	public class Importer extends AbstractConsumer<String, String> {
	
	private static final Logger LOG = LoggerFactory.getLogger(Importer.class);
	
	private static final String AUTHOR = "author";
	private static final String HEADLINE ="headline";
	private static final String ID = "newsId";
	private static final String TEXT = "text";
	
	private static final String CONTENT_PATH = "/content/kafka/news/";
	private static final String NEWS_TEMPLATE = "/apps/kafka/templates/newstemplate";
	
	
	@Reference
	ResourceResolverFactory rrf;

	@Override
	public Properties setupCustomProperties () {
		Properties props = new Properties();
		props.put("key.deserializer", org.apache.kafka.common.serialization.StringDeserializer.class);
		props.put("value.deserializer", org.apache.kafka.common.serialization.StringDeserializer.class);
		return props;
		
	}

	@SuppressWarnings("deprecation")
	@Override
	public void handleRecords(ConsumerRecords<String, String> records)   {
		
		Iterator<ConsumerRecord<String, String>> iter = records.iterator();
		while (iter.hasNext()) {
			ConsumerRecord<String,String> record = iter.next();
			String dataJson = record.value();
			LOG.info("2a: {}", dataJson);
			JSONObject data;
			try {
				ResourceResolver resolver = rrf.getAdministrativeResourceResolver(null);
				
				data = new JSONObject(dataJson);
				String author = data.getString(AUTHOR);
				String headline = data.getString(HEADLINE);
				String id = data.getString(ID);
				String text = data.getString(TEXT);
				
				try {
					PageManager pageManager = resolver.adaptTo(PageManager.class);
					pageManager.create(CONTENT_PATH,id,NEWS_TEMPLATE,headline);
					
				} catch (WCMException e) {
					LOG.error("Cannot create new page with newsID = {}", id, e);
				}
				
			} catch (JSONException e) {
				LOG.warn("Incomplete JSON record ",e);
			} catch (LoginException e1) {
				LOG.warn("Cannot open resourceResolver",e1);
			}
			
			
			
		}
		
	}
	


}
