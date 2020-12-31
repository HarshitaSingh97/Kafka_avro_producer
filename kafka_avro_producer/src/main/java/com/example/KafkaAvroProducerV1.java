package com.example;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaAvroProducerV1 {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("acks", "1");
        properties.setProperty("retries", "10");

        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
        properties.setProperty("schema.registry.url", "http://127.0.0.1:8081");

        KafkaProducer<String, avro_Schema> kafkaProducer = new KafkaProducer<String, avro_Schema>(properties);
        String topic = "hsingh_avro";

        avro_Schema avro_record = avro_Schema.newBuilder()
                .setFirstName("Harshita")
                .setLastName("Singh")
                .setAge(123)
                .setHeight(270.0f)
                .setWeight(200)
                .setAutomatedEmail(false)
                .build();
        ProducerRecord<String, avro_Schema> producerRecord = new ProducerRecord<String, avro_Schema>(
                topic, avro_record
        );

        kafkaProducer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception == null) {
                    System.out.println("Success!");
                    System.out.println(metadata.toString());
                } else {
                    exception.printStackTrace();
                }
            }
        });

        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
