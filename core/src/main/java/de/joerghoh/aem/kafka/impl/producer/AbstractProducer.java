package de.joerghoh.aem.kafka.impl.producer;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Service;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.sling.commons.osgi.PropertiesUtil;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.joerghoh.aem.kafka.consumer.AbstractConsumer;
import de.joerghoh.aem.kafka.consumer.IncompleteKafkaConfigurationException;
import de.joerghoh.aem.kafka.consumer.AbstractConsumer.KafkaConsumerRunner;

@Component(componentAbstract=true,metatype=true)
@Service
public abstract class AbstractProducer<K,V> {

	
	private static Logger log = LoggerFactory.getLogger(AbstractConsumer.class);

	@Property(label="Topic", description="The kafka topic to post to")
	private static final String TOPICNAME = "kafka.topicName";
	private String topicName;

	@Property(cardinality=Integer.MAX_VALUE,label="bootstrap servers", description="the Kafka bootstrap servers")
	private static final String BOOTSTRAP_SERVERS = "kafka.bootstrap.servers";
	private String[] bootstrapServers;
	
	@Property (label="Timeout on shutdown", description="timeout in seconds on shutdown", intValue=5)
	private static final String TIMEOUT_ON_SHUTDOWN = "TimeoutOnShutdown";
	private int timeoutOnShutdown;
	
	
	
	KafkaProducer<K,V> producer;
	
	protected abstract Properties setupCustomProperties();
	
	@Activate
	protected void activate (ComponentContext context) throws IncompleteKafkaConfigurationException {
		
		Properties props = new Properties();
		
		topicName = PropertiesUtil.toString(context.getProperties().get(TOPICNAME), "");
		if (StringUtils.isEmpty(topicName)) {
			throw new IncompleteKafkaConfigurationException ("No topic configured");
		}
		bootstrapServers = PropertiesUtil.toStringArray(context.getProperties().get(BOOTSTRAP_SERVERS), null);
		if (bootstrapServers == null || bootstrapServers.length == 0) {
			throw new IncompleteKafkaConfigurationException ("No bootstrapServers configured");
		}
		props.put("bootstrap.servers", StringUtils.join(bootstrapServers,","));
	
		props.putAll(setupCustomProperties()); 
		producer = new KafkaProducer<>(props);
		

		log.info ("Started {}",toString());
	}
	
	
	@Deactivate
	protected void deactivate() {
		producer.close(timeoutOnShutdown, TimeUnit.SECONDS);
	}
	
	
	public void send (K key,V value) {
		ProducerRecord<K,V> r = new ProducerRecord<K,V> (topicName, key ,value);
	    producer.send(r);	
	}
	
	public void send (Integer partition, K key,V value) {
		ProducerRecord<K,V> r = new ProducerRecord<K,V> (topicName, partition, key, value);
	    producer.send(r);	
	}
	
	public String toString () {
		return String.format("KafkaProducer (topic=%s, bootstrap servers = [%s])", new Object[]{
				topicName, StringUtils.join(bootstrapServers,",")});
	}
	
	
}
