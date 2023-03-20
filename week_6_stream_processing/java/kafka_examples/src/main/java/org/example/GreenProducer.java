package org.example;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.StreamsConfig;
import org.example.data.GreenRide;

import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;


public class GreenProducer {
    private final Properties props = new Properties();

    public GreenProducer() {
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "pkc-xmzwx.europe-central2.gcp.confluent.cloud:9092");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='" + Secrets.KAFKA_CLUSTER_KEY + "' password='" + Secrets.KAFKA_CLUSTER_SECRET + "';");
        props.put("sasl.mechanism", "PLAIN");
        props.put("client.dns.lookup", "use_all_dns_ips");
        props.put("session.timeout.ms", "45000");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaJsonSerializer");
    }

    public static void main(String[] args) throws IOException, CsvException, ExecutionException, InterruptedException {
        var producer = new GreenProducer();
        var rides = producer.getRides();
        producer.publishRides(rides);
    }

    public List<GreenRide> getRides() throws IOException, CsvException {
        var ridesStream = this.getClass().getResource("/green_tripdata_2019-01.csv");
        var reader = new CSVReader(new FileReader(ridesStream.getFile()));
        reader.skip(1);
        return reader.readAll().stream().map(arr -> new GreenRide(arr))
                .collect(Collectors.toList());

    }

    public void publishRides(List<GreenRide> rides) throws ExecutionException, InterruptedException {
        KafkaProducer<String, GreenRide> kafkaProducer = new KafkaProducer<String, GreenRide>(props);
        for (GreenRide ride : rides) {
            var record = kafkaProducer.send(new ProducerRecord<>(Topics.GREEN_RIDE_TOPIC, String.valueOf(ride.PULocationID), ride), (metadata, exception) -> {
                if (exception != null) {
                    System.out.println(exception.getMessage());
                }
            });
            System.out.println(record.get().offset());
            System.out.println(ride.PULocationID);
            Thread.sleep(500);
        }
    }
}