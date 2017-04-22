package de.joerghoh.aem.kafka.consumer;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.StringUtils;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.Service;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.sling.commons.osgi.PropertiesUtil;
import org.apache.sling.settings.SlingSettingsService;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component(componentAbstract=true,metatype=true)
@Service
public abstract class AbstractConsumer<K,V> implements Consumer {
	
	
	private static Logger log = LoggerFactory.getLogger(AbstractConsumer.class);

	@Property(label="Topic", description="The kafka topic to listen for")
	private static final String TOPICNAME = "kafka.topicName";
	private String topicName;

	@Property(cardinality=Integer.MAX_VALUE,label="Bootstrap servers", description="The Kafka bootstrap servers")
	private static final String BOOTSTRAP_SERVERS = "kafka.bootstrap.servers";
	private String[] bootstrapServers;
	
	@Property(label="Group ID", description="The Kafka group ID")
	private static final String GROUP_ID = "kafka.group.id";
	private String groupId;
	
	private static final boolean DEFAULT_RANDOM_GROUP_ID = false;
	@Property(boolValue=DEFAULT_RANDOM_GROUP_ID, label="Random Group ID", description="Use Sling ID as Group ID. Do not provide a Group ID then")
	private static final String RANDOM_GROUP_ID = "kafka.random.group.id";
	private boolean randomGroupId;

	@Reference
	SlingSettingsService settings;

	Properties props = new Properties();
	KafkaConsumer<K,V> consumer;

	KafkaConsumerRunner runnable;


	// Abstract methods
	protected abstract void prepareProperties(Properties props);


	protected abstract void handleRecords (ConsumerRecords<K,V> records);



	// Lifecycle

	protected void start (ComponentContext context) throws IncompleteKafkaConfigurationException {
		topicName = PropertiesUtil.toString(context.getProperties().get(TOPICNAME), "");
		if (StringUtils.isEmpty(topicName)) {
			throw new IncompleteKafkaConfigurationException ("No topic configured");
		}
		bootstrapServers = PropertiesUtil.toStringArray(context.getProperties().get(BOOTSTRAP_SERVERS), null);
		if (bootstrapServers == null || bootstrapServers.length == 0) {
			throw new IncompleteKafkaConfigurationException ("No bootstrapServers configured");
		}
		props.put("bootstrap.servers", StringUtils.join(bootstrapServers,","));
		
		
		
		groupId = PropertiesUtil.toString(context.getProperties().get(GROUP_ID), "");
		randomGroupId = PropertiesUtil.toBoolean(context.getProperties().get(RANDOM_GROUP_ID), DEFAULT_RANDOM_GROUP_ID);
		if (randomGroupId) {
			groupId = settings.getSlingId();
		} else {
			if (StringUtils.isEmpty(groupId)) {
				throw new IncompleteKafkaConfigurationException ("No groupId configured and groupId not random");
			}
		}
		props.put("group.id", groupId);
		

		prepareProperties(props);
		consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList(new String[]{topicName}));
		runnable = new KafkaConsumerRunner (this);
		log.info ("Started consumer {}",toString());
	}
	
	public String toString() {
		return String.format("[Consumer(class=%s,topic=%s,bootstrapServers=%s,groupId=%s)]", 
				new Object[]{this.getClass().getName(),topicName,StringUtils.join(bootstrapServers,","),groupId});
		
	}



	protected void stop () {
		runnable.shutdown();
	}

	public Runnable getRunnable() {
		return runnable;
	}


	public class KafkaConsumerRunner implements Runnable {
		private final AtomicBoolean closed = new AtomicBoolean(false);
		private final AbstractConsumer<K,V> consumer;

		protected KafkaConsumerRunner (AbstractConsumer<K,V> consumer) {
			this.consumer = consumer;
		}

		public void run() {
			try {
				while (!closed.get()) {
					ConsumerRecords<K, V> records = consumer.consumer.poll(10000);
					handleRecords (records);
				}
			} catch (WakeupException e) {
				// Ignore exception if closing
				if (!closed.get()) throw e;
			} finally {
				consumer.consumer.close();
				log.info("consumer {} shutdown",consumer);
			}
		}

		// Shutdown hook which can be called from a separate thread
		public void shutdown() {
			closed.set(true);
			consumer.consumer.wakeup();
		}
	}

}
