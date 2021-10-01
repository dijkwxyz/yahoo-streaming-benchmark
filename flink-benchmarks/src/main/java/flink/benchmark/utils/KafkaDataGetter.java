package flink.benchmark.utils;

import flink.benchmark.BenchmarkConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import redis.clients.jedis.Jedis;

import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

public class KafkaDataGetter {

    private final BenchmarkConfig config;
    private KafkaConsumer consumer;

    public KafkaDataGetter(String yamlFile) throws FileNotFoundException {
        this.config = new BenchmarkConfig(yamlFile);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", config.bootstrapServers);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); //key 序列化
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer"); //value 序列化
        properties.put("group.id", "result");
        properties.put("isolation.level", config.kafkaConsumerIsolationLevel);
        consumer = new KafkaConsumer(properties);
        consumer.subscribe(Collections.singletonList(config.kafkaSinkTopic));
    }

    public void execute(String outputFile) throws IOException, InterruptedException {
        FileWriter fw = new FileWriter(outputFile);
        long start = System.currentTimeMillis();
        while (true) {
            if (System.currentTimeMillis() - start > config.testTimeSeconds * 1000) {
                break;
            }
            final ConsumerRecords<String, String> consumerRecords =
                    consumer.poll(100);

            if (consumerRecords.count() == 0) {
                Thread.sleep(500);
            }

            for (ConsumerRecord record : consumerRecords) {
                try {
                    fw.write(String.format("%s, %d\n",
                            record.key(), System.currentTimeMillis()));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            consumer.commitAsync();
        }

        consumer.close();
        fw.close();

        System.out.println("Kafka Sink Consumer DONE");
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        KafkaDataGetter kafkaDataGetter = new KafkaDataGetter(args[0]);
//        KafkaDataGetter kafkaDataGetter = new KafkaDataGetter("conf/benchmarkConf.yaml");
        kafkaDataGetter.execute(args[1]);
    }
}
