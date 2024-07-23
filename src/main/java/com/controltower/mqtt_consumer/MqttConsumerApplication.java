package com.controltower.mqtt_consumer;

import com.controltower.mqtt_consumer.mqtt.MqttCallbackImpl;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
@Slf4j
public class MqttConsumerApplication {
	public static void main(String[] args) {
		SpringApplication.run(MqttConsumerApplication.class, args);
	}

	@Bean
	public CommandLineRunner commandLineRunner() {
		return (runner) -> {
			// start consumer here - could just be java application
			String brokerUrl = "tcp://";
			String clientId = "control-tower-consumer";

			try(MqttClient mqttClient = new MqttClient(brokerUrl,clientId) ) {
				MqttConnectOptions connectOptions = new MqttConnectOptions();
				connectOptions.setConnectionTimeout(10000);
				connectOptions.setCleanSession(false);
				connectOptions.setUserName("control-tower");
				connectOptions.setPassword("Admin@1234".toCharArray());

				mqttClient.setCallback(new MqttCallbackImpl());

				mqttClient.connect(connectOptions);

			}
			catch(MqttException e) {
				log.debug("Some error occurred: \n");
				log.error(e.getMessage());
			}
		};
	}

}
