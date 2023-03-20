package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.example.customserdes.CustomSerdes;
import org.example.data.FhvRide;
import org.example.data.GreenRide;

import java.util.Properties;

public class MyKStreamJoins {
    private final Properties props = new Properties();

    public MyKStreamJoins() {
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "pkc-xmzwx.europe-central2.gcp.confluent.cloud:9092");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='" + Secrets.KAFKA_CLUSTER_KEY + "' password='" + Secrets.KAFKA_CLUSTER_SECRET + "';");
        props.put("sasl.mechanism", "PLAIN");
        props.put("client.dns.lookup", "use_all_dns_ips");
        props.put("session.timeout.ms", "45000");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka_tutorial.kstream.joined.rides.pickuplocation.v2");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
    }

    public static void main(String[] args) throws InterruptedException {
        var object = new MyKStreamJoins();
        object.joinRidesPickupLocation();
    }

    public Topology createTopology() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, FhvRide> rides = streamsBuilder.stream(Topics.FHV_RIDE_TOPIC, Consumed.with(Serdes.String(), CustomSerdes.getSerde(FhvRide.class)));
        KStream<String, GreenRide> greenrides = streamsBuilder.stream(Topics.GREEN_RIDE_TOPIC, Consumed.with(Serdes.String(), CustomSerdes.getSerde(GreenRide.class)));

        var pickupLocationsKeyedOnPUId = greenrides.selectKey((key, value) -> String.valueOf(value.PULocationID));
        var fhvRides = rides.selectKey((key, value) -> String.valueOf(value.PULocationID));

        var merged = fhvRides
                .mapValues(value -> value.PULocationID)
                .merge(pickupLocationsKeyedOnPUId
                        .mapValues(value -> value.PULocationID));
        merged.peek((key, value) -> System.out.println("Incoming record - key " + key + " value " + value));
        var myStream = merged.groupByKey(Grouped.with(
                        Serdes.String(), /* key */
                        Serdes.Long()))
                .count()
                .toStream()
                .mapValues(value -> value.toString() + " count");
        myStream.peek((key, value) -> System.out.println("Outgoing record - key " + key + " value " + value)).to(Topics.OUTPUT_TOPIC_HOME, Produced.with(Serdes.String(), Serdes.String()));
        return streamsBuilder.build();
    }

    public void joinRidesPickupLocation() throws InterruptedException {
        var topology = createTopology();
        var kStreams = new KafkaStreams(topology, props);

        kStreams.setUncaughtExceptionHandler(exception -> {
            System.out.println(exception.getMessage());
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
        });
        kStreams.start();
        while (kStreams.state() != KafkaStreams.State.RUNNING) {
            System.out.println(kStreams.state());
            Thread.sleep(1000);
        }
        System.out.println(kStreams.state());
        Runtime.getRuntime().addShutdownHook(new Thread(kStreams::close));

    }
}