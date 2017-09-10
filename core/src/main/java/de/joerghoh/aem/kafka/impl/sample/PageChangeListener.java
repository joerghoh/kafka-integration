package de.joerghoh.aem.kafka.impl.sample;

import java.util.Iterator;
import java.util.Properties;

import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Service;
import org.apache.sling.commons.json.JSONException;
import org.apache.sling.commons.json.JSONObject;
import org.osgi.service.event.Event;
import org.osgi.service.event.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.day.cq.wcm.api.PageEvent;
import com.day.cq.wcm.api.PageModification;

import de.joerghoh.aem.kafka.impl.producer.AbstractProducer;

@Service()
@Component(immediate=true,metatype=true,label="Kafka Audit Bridge")
@Property(name="event.topics", value="com/day/cq/wcm/core/page")
public class PageChangeListener extends AbstractProducer<String,String> implements EventHandler {
	
	final Logger LOG = LoggerFactory.getLogger(PageChangeListener.class);

	@Override
	protected Properties setupCustomProperties() {
		Properties props = new Properties();
		props.put("key.serializer", org.apache.kafka.common.serialization.StringSerializer.class);
		props.put("value.serializer", org.apache.kafka.common.serialization.StringSerializer.class);
		return props;
	}

	@Override
	public void handleEvent(Event event) {
		PageEvent pageEvent = PageEvent.fromEvent(event);
        if (pageEvent != null) {
            final Iterator<PageModification> i = pageEvent.getModifications();
            while (i.hasNext()) {
                final PageModification pm = i.next();
                LOG.info("Page event occurred: {} on {}, user={}", new Object[]{pm.getType(), pm.getPath(),pm.getUserId()});
                JSONObject json = new JSONObject();
                try {
					json.append("type", pm.getType());
	                json.append("page", pm.getPath());
	                json.append("user", pm.getUserId());
	                
	                String payload = json.toString();
	                
	                send("audit-data", payload);
				} catch (JSONException e) {
					LOG.warn("problems when creating payload JSON",e);
				}

            }
        }
		
	}
	
	
	

}
