import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class KafkaStreamConsumer {
    public static void main(String[] args) {
        new KafkaStreamConsumer().start();
    }

    private void start() {

        Properties properties=new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass().getName());
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-consumer-1");
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "1000");
        StreamsBuilder builder= new StreamsBuilder();
        KStream<String,String> kStream = builder.stream("bdccTopic", Consumed.with(Serdes.String(),Serdes.String()));

        KTable<Windowed<String>, Long> resultStream=kStream.flatMapValues(textLine-> Arrays.asList(textLine.split(" ")))
                .map((k,v)->new KeyValue<>(k,v.toLowerCase()))
                .filter((k,v)->v.equals("a")||v.equals("b"))
                .groupBy((k,v)->v)
                .windowedBy(TimeWindows.of(Duration.ofSeconds(5)))
                // permet de voir seulement par tranche de 5 secondes(sinon il cummule depuis le debut)
                .count(Materialized.as("count-analytics"))
                ;
        //.foreach((k,v)-> System.out.println(k+"->"+ v));

        resultStream.toStream().map((k,v)->new KeyValue<>(k.key(),v)).to("resTopic", Produced.with(Serdes.String(),Serdes.Long()));

        Topology topology= builder.build();
        KafkaStreams kafkaStreams = new KafkaStreams(topology,properties);
        kafkaStreams.start();

    }
}
