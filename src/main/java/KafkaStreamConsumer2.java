import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.util.Arrays;
import java.util.Properties;

public class KafkaStreamConsumer2 {
    public static void main(String[] args) {
        new KafkaStreamConsumer2().start();
    }

    private void start() {

        Properties properties=new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.Long().getClass().getName());
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-consumer-2");
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "1000");
        StreamsBuilder builder= new StreamsBuilder();
        KStream<String,Long> kStream = builder.stream("resTopic", Consumed.with(Serdes.String(),Serdes.Long()));

        kStream
                .foreach((k,v)-> System.out.println(k+"->"+ v));

        //resultStream.to("resTopic", Produced.with(Serdes.String(),Serdes.Long()));

        Topology topology= builder.build();
        KafkaStreams kafkaStreams = new KafkaStreams(topology,properties);
        kafkaStreams.start();

    }
}
