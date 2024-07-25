package com.controltower.mqtt_consumer.mqtt;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.ApiException;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Slf4j
public class MqttCallbackImpl implements MqttCallback {

    private Publisher publisher;

    public MqttCallbackImpl(Publisher publisher) {
        this.publisher = publisher;
    }

    @Override
    public void connectionLost(Throwable throwable) {
      log.error("Connection lost so shutting publisher down: "+throwable.getMessage());
      publisher.shutdown();
      try {
          publisher.awaitTermination(30, TimeUnit.SECONDS);
      }
      catch(InterruptedException e) {
          log.error(e.getMessage());
      }
      finally {
          log.debug("publisher shut down..");
      }
        System.exit(1);
    }

    @Override
    public void messageArrived(String topic, MqttMessage mqttMessage) throws Exception {

        String trackingMessage = new String(mqttMessage.getPayload(), StandardCharsets.UTF_8);
        log.info("Message received from subscription: "+trackingMessage);

        try {
            ByteString data = ByteString.copyFromUtf8(trackingMessage);
            PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
            .setData(data).build();
            log.debug("Sending message to cloud pub/sub...");
            ApiFuture<String> messageIdFuture = publisher.publish(pubsubMessage);
            String messageId = messageIdFuture.get();
            ApiFutures.addCallback(
                    messageIdFuture,
                    new ApiFutureCallback<>() {

                        @Override
                        public void onFailure(Throwable throwable) {
                            if (throwable instanceof ApiException) {
                                ApiException apiException = ((ApiException) throwable);
                                // details on the API exception
                                log.error("gcp api error code is: "+apiException.getStatusCode().getCode());
                                System.out.println(apiException.isRetryable());
                            }
                            System.out.println("Error publishing message : ");
                        }

                        @Override
                        public void onSuccess(String messageId) {
                            // Once published, returns server-assigned message ids (unique within the topic)
                            log.info("Message successfully sent");
                            log.info("Published message ID: " + messageId);
                        }
                    },
                    MoreExecutors.directExecutor());
            log.info("Published message id: "+messageId);

        }
        catch(InterruptedException| ExecutionException e) {
            e.printStackTrace();
            log.error("Error in publishing message... {}",e.getMessage());

        }
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {

    }
}
