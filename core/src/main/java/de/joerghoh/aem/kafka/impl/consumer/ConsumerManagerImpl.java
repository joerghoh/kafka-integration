package de.joerghoh.aem.kafka.impl.consumer;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.ReferencePolicy;
import org.apache.felix.scr.annotations.Service;
import org.apache.sling.commons.osgi.PropertiesUtil;
import org.apache.sling.commons.threads.ModifiableThreadPoolConfig;
import org.apache.sling.commons.threads.ThreadPool;
import org.apache.sling.commons.threads.ThreadPoolManager;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.joerghoh.aem.kafka.consumer.Consumer;

@Reference(referenceInterface=Consumer.class,
	cardinality=ReferenceCardinality.OPTIONAL_MULTIPLE,policy=ReferencePolicy.DYNAMIC,
	bind="bindConsumer", unbind="unbindConsumer")
@Service(value= ConsumerManagerImpl.class)
@Component(immediate=true)
public class ConsumerManagerImpl {
	
	private static final Logger log = LoggerFactory.getLogger(ConsumerManagerImpl.class);
	

	private static final String THREADPOOL_NAME = "kafkaPool";
	
	
	private static final int DEFAULT_THREADPOOL_SIZE = 10;
	@Property(intValue=DEFAULT_THREADPOOL_SIZE)
	private static final String PROP_THREADPOOL_SIZE = "threadpool.size";
	int threadpoolSize;
	
	
	@Reference
	ThreadPoolManager tpm;
	
	ThreadPool kafkaPool;
	AtomicInteger activeThreads;
	
	Set<Consumer> registeredConsumers = new HashSet<>();
	
	
	@Activate
	protected void activate (ComponentContext context) {
		threadpoolSize = PropertiesUtil.toInteger(context.getProperties().get(PROP_THREADPOOL_SIZE), DEFAULT_THREADPOOL_SIZE);
		ModifiableThreadPoolConfig tpConfig = new ModifiableThreadPoolConfig();
		tpConfig.setMaxPoolSize(threadpoolSize);
		activeThreads = new AtomicInteger(0);
		
		kafkaPool = tpm.create(tpConfig, THREADPOOL_NAME);
		log.info("threadpool '{}' created with {} threads", THREADPOOL_NAME,threadpoolSize);
	}
	
	@Deactivate
	protected void deactivate () {
		
		// deactivate consumers
		synchronized(registeredConsumers) {
			Iterator<Consumer> iter = registeredConsumers.iterator();
			while (iter.hasNext()) {
				Consumer c = iter.next();
				log.info("Shutting down consumer {}", c);
				c.stop();
			}
		}
		
		tpm.release(kafkaPool);
		log.info("KafkaPool deactivated");
		
	}
	
	
	// SCR methods
	
	protected void bindConsumer (Consumer c) {
		
		synchronized (registeredConsumers) {
			registeredConsumers.add(c);
		}

		log.info("Bound consumer {}", c.toString());
		kafkaPool.execute(c.getRunnable());
		if (registeredConsumers.size() > threadpoolSize) {
			log.warn("Binding Consumer although no free thread available in pool (threadpoolsize = {}, currentSize = {}).",
					new Object[]{threadpoolSize, registeredConsumers.size()});
		}
		
	}
	
	protected void unbindConsumer (Consumer c) {
		synchronized(registeredConsumers) {
			registeredConsumers.remove(c);
		}
		log.info("unbound consumer {}", c.toString());
		
	}

}
