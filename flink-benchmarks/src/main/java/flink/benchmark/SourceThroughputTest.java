package flink.benchmark;

import flink.benchmark.generator.EventGeneratorSource;
import flink.benchmark.utils.ThroughputLogger;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.io.FileNotFoundException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class SourceThroughputTest {
    public static void main(String[] args) throws Exception {
        BenchmarkConfig config = BenchmarkConfig.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(4);

        EventGeneratorSource eventGenerator = new EventGeneratorSource(config);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "kafka1:9092, kafka2:9092");
        properties.setProperty("group.id", "flink_yahoo_benchmark");
        FlinkKafkaConsumer011<String> stringFlinkKafkaConsumer011 = new FlinkKafkaConsumer011<>(
                "ad-events",
                new SimpleStringSchema(),
                properties);

        DataStreamSource<String> source = env.addSource(stringFlinkKafkaConsumer011);
//        DataStreamSource<String> source = env.addSource(eventGenerator)
        SingleOutputStreamOperator<Integer> throughput = source
                .flatMap(new ThroughputLogger<>(240, 1_000_000));

        source.print("source");
        env.execute("throughput test");

    }
}
