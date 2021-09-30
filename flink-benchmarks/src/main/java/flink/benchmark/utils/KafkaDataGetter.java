package flink.benchmark.utils;

import flink.benchmark.BenchmarkConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import redis.clients.jedis.Jedis;

import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

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
        consumer = new KafkaConsumer(properties);
        consumer.subscribe(Collections.singletonList(config.kafkaSinkTopic));
    }

    public void execute() throws IOException {
        FileWriter fileWriter = new FileWriter("count-latency.txt");
        final int giveUp = 10000;   int noRecordsCount = 0;

        while (true) {
            final ConsumerRecords<Long, String> consumerRecords =
                    consumer.poll(1000);

            if (consumerRecords.count()==0) {
                noRecordsCount++;
                if (noRecordsCount > giveUp) { break; }
                else continue;
            }

            consumerRecords.forEach(record -> {
                System.out.printf("Consumer Record:(%d, %s, %d, %d)\n",
                        record.key(), record.value(),
                        record.partition(), record.offset());
            });

            consumer.commitAsync();
        }
        consumer.close();
        System.out.println("DONE");

        fileWriter.close();
    }

    public static void main(String[] args) throws IOException {
        RedisDataGetter redisDataGetter = new RedisDataGetter(args[0]);
//        RedisDataGetter redisDataGetter = new RedisDataGetter("conf/benchmarkConf.yaml");
        redisDataGetter.execute();
    }
}
