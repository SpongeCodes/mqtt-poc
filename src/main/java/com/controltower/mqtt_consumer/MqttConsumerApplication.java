package com.controltower.mqtt_consumer;

import com.controltower.mqtt_consumer.constants.GoogleCloudDetails;
import com.controltower.mqtt_consumer.mqtt.MqttCallbackImpl;
import com.google.api.gax.core.GoogleCredentialsProvider;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.pubsub.v1.TopicName;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;

@SpringBootApplication
@Slf4j
public class MqttConsumerApplication {
	public static void main(String[] args) throws IOException {
		SpringApplication.run(MqttConsumerApplication.class, args);

		TopicName topicName = TopicName.of(
				GoogleCloudDetails.GOOGLE_CLOUD_PROJECT,
				"iot");

		//GoogleCredentialsProvider

		Publisher publisher = Publisher.newBuilder(topicName).build();

		String brokerUrl = "tcp://localhost:1883";
		String clientId = "control-tower-consumer";

		try(MqttClient mqttClient = new MqttClient(brokerUrl,clientId) ) {
			MqttConnectOptions connectOptions = new MqttConnectOptions();
			connectOptions.setConnectionTimeout(10000);
			connectOptions.setCleanSession(false);
			connectOptions.setUserName("controltower");
			connectOptions.setPassword("Admin@1234".toCharArray());

			mqttClient.setCallback(new MqttCallbackImpl(publisher));

			mqttClient.connect(connectOptions);
			log.info("Connection successful");
			mqttClient.subscribe("iot");

		}
		catch(MqttException e) {
			log.debug("Some error occurred: \n");
			log.error(e.getMessage());
		}


	}




}
