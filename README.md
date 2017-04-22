# Kafka example integration

This project demonstrates how Apache Kafka can be integrated into a Sling/AEM based application.

It consists of 3 maven projects
* core: the Integration itself
* samples: a sample application based on the integration
* ui.apps: an easy vehicle to deploy all required artifacts into a running AEM application

(for the record: This integration is based on the AEM archetypes, but besides the core module itself does not have any dependency to AEM.

## How the integration works

There is no "ready to use" implementation available, you always need to implement code to use the Kafka Integration. 

### Implementing your own Kafka consumer in Sling

* Create a service which extends from AbstractConsumer
* in the SCR livecycle methods (activate and deactivate) call the methods "start"/"stop" from AbstractConsumer
* implement the prepareProperties method and add at least these properties (check the Kafka documentation)
** "key.deserializer"
** "key.serializer"


## How to build

If you have a running AEM instance you can build and package the whole project and deploy into AEM with  

    mvn clean install -PautoInstallPackage
    



