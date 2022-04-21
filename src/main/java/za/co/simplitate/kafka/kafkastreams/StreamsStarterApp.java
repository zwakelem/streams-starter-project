package za.co.simplitate.kafka.kafkastreams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

public class StreamsStarterApp {

    public static void main(String[] args) {

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-starter-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG , "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG , Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG , Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        // stream from Kafka
        KStream<String, String> wordCountInput = builder.stream("word-count-input");

        // map to lowercase
        KTable<String, Long> wordCounts = wordCountInput.mapValues(textLine -> textLine.toLowerCase())
                // split by space
                .flatMapValues(lowerCasedTextlive -> Arrays.asList(lowerCasedTextlive.split(" ")))
                .selectKey((ignoredKey, word) -> word) // select key to apply a key
                .groupByKey() // group by key before aggregation
                .count(); // count occurences

        wordCounts.toStream().to("word-count-output", Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(), config);

        streams.start();

        System.out.println(streams.toString()); // printed the topology

        // shutdown hook to gracefully close our streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}
