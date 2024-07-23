package com.controltower.mqtt_consumer.mqtt;

import com.controltower.mqtt_consumer.constants.GoogleCloudDetails;
import com.google.api.core.ApiFuture;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Slf4j
public class MqttCallbackImpl implements MqttCallback {

    private Publisher publisher;

    public MqttCallbackImpl() throws IOException {
        TopicName topicName = TopicName.of(
                GoogleCloudDetails.GOOGLE_CLOUD_PROJECT,
                "iot");
        publisher = Publisher.newBuilder(topicName).build();
    }
    @Override
    public void connectionLost(Throwable throwable) {
      log.error("Connection lost: "+throwable.getMessage());
      publisher.shutdown();
      try {
          publisher.awaitTermination(30, TimeUnit.SECONDS);
      }
      catch(InterruptedException e) {
          log.error(e.getMessage());
      }
      finally {
          log.debug("Connection lost so shutting producer down..");
      }
        System.exit(1);
    }

    @Override
    public void messageArrived(String s, MqttMessage mqttMessage) throws Exception {

        try {
            ByteString data = ByteString.copyFromUtf8(s);
            PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
            .setData(data).build();
            ApiFuture<String> messageIdFuture = publisher.publish(pubsubMessage);
            String messageId = messageIdFuture.get();
            log.info("Published message id: "+messageId);

        }
        catch(InterruptedException| ExecutionException e) {
            log.debug("Error in publishing message...");

        }
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {

    }
}
