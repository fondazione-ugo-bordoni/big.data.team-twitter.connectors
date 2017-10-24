# FUB Big Data Platform: Twitter connectors as DAF IoT events datasource.  

In order to enable the Twitter connectors for their usage in the context of the [Data & Analytics Framework](https://daf-docs.readthedocs.io/en/latest/) project (DAF), a specific emitter has been developed.

The emitter [IotDafKafkaEmitter](https://github.com/fondazione-ugo-bordoni/bigdataplatform-connectors/blob/master/src/main/it/fub/bigdataplatform/connectors/emitters/IotDafKafkaEmitter.java) wraps collected tweets into DAF IoT objects.

To configure and enable the emitter, set properly the 
`$CONNECTOR_HOME/etc/iot-daf-kafka.properties` and the  `$CONNECTOR_HOME/etc/twitter-connector.properties` files, respectively.

###Notes
The IotDafKafkaEmitter uses the class  `it.gov.daf.iotingestion.event.Event`. This class is generated starting from the [Event.avsc](https://github.com/italia/daf/tree/master/iot_ingestion_manager/common/src/main/avro) Avro schema by using the avro-tools.1.7.5.jar. See [http://avro.apache.org/docs/1.7.5/gettingstartedjava.html](http://avro.apache.org/docs/1.7.5/gettingstartedjava.html) for more info.
