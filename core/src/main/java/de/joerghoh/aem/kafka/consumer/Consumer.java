package de.joerghoh.aem.kafka.consumer;

public interface Consumer {

	
	Runnable getRunnable();
	
	void stop ();
	
}
