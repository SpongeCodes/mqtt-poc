package com.controltower.mqtt_consumer.constants;

import org.springframework.beans.factory.annotation.Value;

public class MqttBrokerConstants {

        @Value("${mqtt.broker.url}")
        public static String brokerURL;

        @Value("${mqtt.broker.username}")
        public static String username;

        @Value("${mqtt.broker.password}")
        public static String password;

}
