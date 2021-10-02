/**
 * Copyright 2015, Yahoo Inc.
 * Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.
 */
package flink.benchmark;

import benchmark.common.advertising.RedisAdCampaignCache;
import com.dijk.multilevel.PatternBasedMultilevelStateBackend;
import flink.benchmark.generator.EventGeneratorSource;
import flink.benchmark.generator.KafkaDataGenerator;
import flink.benchmark.utils.FailureInjectorMap;
import flink.benchmark.utils.StateBackendFactory;
import flink.benchmark.utils.ThroughputLoggerProcessor;
import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;

/**
 * To Run:  flink run -c flink.benchmark.AdvertisingTopologyFlinkWindows flink-benchmarks-0.1.0.jar "benchmarkConf.yaml"
 * <p>
 * This job variant uses Flinks built-in windowing and triggering support to compute the windows
 * and trigger when each window is complete as well as once per second.
 */
public class AdvertisingTopologyFlinkWindowsKafkaSink {

    private static final Logger LOG = LoggerFactory.getLogger(AdvertisingTopologyFlinkWindowsKafkaSink.class);

    public static void main(final String[] args) throws Exception {

        BenchmarkConfig config = BenchmarkConfig.fromArgs(args);

        StreamExecutionEnvironment env = setupEnvironment(config);

        DataStream<String> rawMessageStream = streamSource(config, env);
//        rawMessageStream.print("raw");
        // log performance
        rawMessageStream.process(new ThroughputLoggerProcessor<String>(
                240, config.throughputLogFreq));

        //out (ad_id, event_time)
        SingleOutputStreamOperator<Tuple2<String, String>> adIdEventTime = rawMessageStream
                .flatMap(new DeserializeBolt())
                .assignTimestampsAndWatermarks(WatermarkStrategy.
                        <Tuple7<String, String, String, String, String, String, String>>forMonotonousTimestamps().
                        withTimestampAssigner(
                                (event, timestamp) -> Long.parseLong(event.f5))) // extract timestamps and generate watermarks from event_time
                .filter(new EventFilterBolt())
                .map(new FailureInjectorMap<>(config.mttiMs, config.injectWithProbability, env.getParallelism(), config.failureStartTimeDelayMs))
                .<Tuple2<String, String>>project(2, 5);

        //=======================advertisement count=========================================
//        //out (ad_id, count)
//        SingleOutputStreamOperator<Tuple3<String, String, Long>> adCount = adIdEventTime
//                .map(new MapToImpressionCount())
//                .keyBy(a -> a.f1)
//                .timeWindow(Time.seconds(config.windowSize), Time.seconds(config.windowSlide))
//                .aggregate(new AdAggregator());
//
//        adCount.addSink(new RedisAdCount(config));

        //=======================campaign count=========================================
        //out: (campaign id, ad_id, event_time)
        DataStream<Tuple3<String, String, String>> joinedAdImpressions = adIdEventTime
                .flatMap(new RedisJoinBolt(config));

        //out: (campaign id, ad_id, 1)
        WindowedStream<Tuple4<String, String, Long, String>, String, TimeWindow> windowStream = joinedAdImpressions
                .map(new MapToImpressionCount())
                .keyBy((a) -> a.f0)
                .timeWindow(Time.seconds(config.windowSize), Time.seconds(config.windowSlide));

        // set a custom trigger
        windowStream.trigger(EventTimeTrigger.create());
//        windowStream.trigger(new EventAndProcessingTimeTrigger());

        // campaign_id, window-end, count, trigger-time
        DataStream<Tuple5<String, String, Long, String, String>> result =
                windowStream.process(sumProcessFunction(config));
//        DataStream<Tuple4<String, String, Long, String>> result =
//                windowStream.reduce(sumReduceFunction(), sumWindowFunction());

//        result.print("process");
//        result2.print("reduce");
        // write result to redis
//        if (config.getParameters().has("add.result.sink.optimized")) {
        result.addSink(new RedisResultSinkOptimized(config));
        result.addSink(kafkaSink(config));
//        } else {
//            result.addSink(new RedisResultSink(config));
//        }

        env.execute("AdvertisingTopologyFlinkWindowsKafkaSink");
//        env.execute("AdvertisingTopologyFlinkWindows " + config.parameters.toMap().toString());
    }

    /**
     * Choose source - either Kafka or data generator
     */
    private static DataStream<String> streamSource(BenchmarkConfig config, StreamExecutionEnvironment env) {
        // Choose a source -- Either local generator or Kafka
        RichParallelSourceFunction<String> source;
        String sourceName;
        if (config.useLocalEventGenerator) {
            EventGeneratorSource eventGenerator = new EventGeneratorSource(config);
            source = eventGenerator;
            sourceName = "EventGenerator";

//            Map<String, List<String>> campaigns = eventGenerator.getCampaigns();
//            RedisHelper redisHelper = new RedisHelper(config);
//            redisHelper.prepareRedis(campaigns);
//            redisHelper.writeCampaignFile(campaigns);
        } else {
            source = kafkaSource(config);
            sourceName = "Kafka";
        }

        return env.addSource(source, sourceName);
    }

    /**
     * Setup Flink environment
     */
    private static StreamExecutionEnvironment setupEnvironment(BenchmarkConfig config) throws IOException {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(config.getParameters());
        env.setParallelism(config.parallelism);
        env.setMaxParallelism(config.parallelism);

        if (config.checkpointsEnabled) {
            env.enableCheckpointing(config.checkpointInterval);
            env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
            env.getCheckpointConfig().setMinPauseBetweenCheckpoints(config.checkpointMinPause);

            String[] patternString = config.multilevelPattern.split(",");
            int[] pattern = new int[patternString.length];
            for (int i = 0; i < pattern.length; i++) {
                pattern[i] = Integer.parseInt(patternString[i]);
            }

            if (config.multilevelEnable) {
                PatternBasedMultilevelStateBackend
                        patternBasedMultilevelBackend = new PatternBasedMultilevelStateBackend(
                        StateBackendFactory.create(config.multilevelLevel0Type, config.multilevelLevel0Path, config),
                        StateBackendFactory.create(config.multilevelLevel1Type, config.multilevelLevel1Path, config),
                        StateBackendFactory.create(config.multilevelLevel2Type, config.multilevelLevel2Path, config),
                        //new FsStateBackend("hdfs://192.168.154.100:9000/flink/checkpoints"),
                        //new RocksDBStateBackend("file:///home/ec2-user/yahoo-streaming-benchmark/flink-1.11.2/data/checkpoints/RDB"),
                        pattern
                );

                //env.setStateBackend(new FsStateBackend("hdfs://115.146.92.102:9000/flink/checkpoints"))
                //env.setStateBackend(new FsStateBackend("file:///home/ec2-user/yahoo-streaming-benchmark/flink-1.11.2/data/checkpoints/fs"))
                env.setStateBackend(patternBasedMultilevelBackend);
            } else {
                env.setStateBackend(StateBackendFactory.create(
                        config.singlelevelStateBackend, config.singlelevelPath, config));
            }

        }

        // use event time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

//        env.setParallelism(1);
        //env.disableOperatorChaining()
        //env.getCheckpointConfig.setCheckpointTimeout(10000);
        //multilevel backend
        //    val patternBasedMultilevelBackend = new PatternBasedMultilevelStateBackend(
        //      new MemoryStateBackend(false),
        //      new FsStateBackend("ftp://worker@hadoop101:21/opt/software/flink-1.11.2/data/flink/checkpoints"),
        //      new FsStateBackend("file:///data/flink/checkpoints/fakeRDB"),
        //      Array[Int](0, 1, 2)
        //    )

        return env;
    }

    private static ProcessWindowFunction<Tuple4<String, String, Long, String>, Tuple5<String, String, Long, String, String>, String, TimeWindow> sumProcessFunction(BenchmarkConfig config) {
        return new ProcessWindowFunction<Tuple4<String, String, Long, String>, Tuple5<String, String, Long, String, String>, String, TimeWindow>() {
            @Override
            public void process(String s, Context context, Iterable<Tuple4<String, String, Long, String>> elements, Collector<Tuple5<String, String, Long, String, String>> out) throws Exception {
                long sum = 0;
                Long max = Long.MAX_VALUE / 2;
                // campaign_id, window-end, count, trigger-time, ad-rank-info
                Tuple5<String, String, Long, String, String> res = new Tuple5<>();
                res.f0 = "0";
                ArrayList<Tuple4<String, String, Long, String>> arr = new ArrayList<>();
                elements.forEach(arr::add);

                for (int i = 0; i < config.cpuLoadAdjuster; i++) {
                    for (Tuple4<String, String, Long, String> e : arr) {
                        if (sum == 0) {
                            res.f0 = e.f0;
                        }
                        if (sum < max) {
                            sum += i + 1;
                        } else {
                            sum -= i;
                        }
                    }
                }

                res.f1 = String.valueOf(context.window().getEnd());
                res.f2 = sum;
                res.f3 = String.valueOf(System.currentTimeMillis());
                res.f4 = "-";
                out.collect(res);
            }
        };
    }

    /**
     * in: (ad_id(key), event_time, 1L)
     * out: (ad_id, largest event_time, count)
     */
    private static class AdAggregator implements AggregateFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, Tuple3<String, String, Long>> {

        @Override
        public Tuple3<String, String, Long> createAccumulator() {
            return Tuple3.of("", "0", 0L);
        }

        @Override
        public Tuple3<String, String, Long> add(Tuple3<String, String, Long> value, Tuple3<String, String, Long> accumulator) {
            return merge(value, accumulator);
        }

        @Override
        public Tuple3<String, String, Long> getResult(Tuple3<String, String, Long> accumulator) {
            return accumulator;
        }

        @Override
        public Tuple3<String, String, Long> merge(Tuple3<String, String, Long> a, Tuple3<String, String, Long> b) {
            return Tuple3.of(
                    a.f0,
                    String.valueOf(Math.max(Long.parseLong(a.f1), Long.parseLong(b.f1))),
                    a.f2 + b.f2);
        }
    }

    /**
     * Sum - window reduce function
     */
    private static ReduceFunction<Tuple3<String, String, Long>> sumReduceFunction() {
        return new ReduceFunction<Tuple3<String, String, Long>>() {
            @Override
            public Tuple3<String, String, Long> reduce(Tuple3<String, String, Long> t0, Tuple3<String, String, Long> t1) throws Exception {
                t0.f2 += t1.f2;
                return t0;
            }
        };
    }

    /**
     * Sum - Window function, summing already happened in reduce function
     */
    private static WindowFunction<Tuple3<String, String, Long>, Tuple4<String, String, Long, String>, String, TimeWindow> sumWindowFunction() {
        return new WindowFunction<Tuple3<String, String, Long>, Tuple4<String, String, Long, String>, String, TimeWindow>() {
            @Override
            public void apply(String keyTuple, TimeWindow window, Iterable<Tuple3<String, String, Long>> values, Collector<Tuple4<String, String, Long, String>> out) throws Exception {
                Iterator<Tuple3<String, String, Long>> valIter = values.iterator();
                Tuple3<String, String, Long> tuple = valIter.next();
                if (valIter.hasNext()) {
                    throw new IllegalStateException("Unexpected");
                }

                Tuple4<String, String, Long, String> res = new Tuple4<>();
                res.f0 = tuple.f0;
                res.f1 = String.valueOf(window.getEnd());
                res.f2 = tuple.f2;
                res.f3 = String.valueOf(System.currentTimeMillis());
                out.collect(res); // collect end time here
            }
        };
    }

    /**
     * Configure Kafka source
     */
    private static FlinkKafkaConsumer011<String> kafkaSource(BenchmarkConfig config) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", config.bootstrapServers);
        properties.setProperty("group.id", config.groupId);

        DeserializationSchema<String> schema = config.isStreamEndless
                ? new SimpleStringSchema()
                : new EndableStingSchema();
        return new FlinkKafkaConsumer011<>(
                config.kafkaTopic,
                schema,
                properties);
    }

    private static class EndableStingSchema extends SimpleStringSchema {
        private static final long serialVersionUID = 1L;

        @Override
        public String deserialize(byte[] message) {
            return super.deserialize(message);
        }

        @Override
        public boolean isEndOfStream(String nextElement) {
            if (KafkaDataGenerator.END_OF_STREAM_ELEMENT.equals(nextElement)) {
                return true;
            }
            return super.isEndOfStream(nextElement);
        }
    }

    /**
     * Custom trigger - Fire and purge window when window closes, also fire every 1000 ms.
     */
    private static class EventAndProcessingTimeTrigger extends Trigger<Object, TimeWindow> {

        @Override
        public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            ctx.registerEventTimeTimer(window.maxTimestamp());
            // register system timer only for the first time
            ValueState<Boolean> firstTimerSet = ctx.getPartitionedState(new ValueStateDescriptor<>("firstTimerSet", Boolean.class));
            if (firstTimerSet.value() == null || !firstTimerSet.value()) {
                ctx.registerProcessingTimeTimer(System.currentTimeMillis() + 1000L);
                firstTimerSet.update(true);
            }
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) {
            return TriggerResult.FIRE_AND_PURGE;
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {

        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            // schedule next timer
            ctx.registerProcessingTimeTimer(System.currentTimeMillis() + 1000L);
            return TriggerResult.FIRE;
        }
    }

    /**
     * Parse JSON
     */
    private static class DeserializeBolt implements
            FlatMapFunction<String, Tuple7<String, String, String, String, String, String, String>> {

        transient JSONParser parser = null;

        @Override
        public void flatMap(String input, Collector<Tuple7<String, String, String, String, String, String, String>> out)
                throws Exception {
            if (parser == null) {
                parser = new JSONParser();
            }
            JSONObject obj = (JSONObject) parser.parse(input);

            Tuple7<String, String, String, String, String, String, String> tuple =
                    new Tuple7<>(
                            obj.getAsString("user_id"),
                            obj.getAsString("page_id"),
                            obj.getAsString("ad_id"),
                            obj.getAsString("ad_type"),
                            obj.getAsString("event_type"),
                            obj.getAsString("event_time"),
                            obj.getAsString("ip_address"));
            out.collect(tuple);
        }
    }

    /**
     * Filter out all but "view" events
     */
    public static class EventFilterBolt implements
            FilterFunction<Tuple7<String, String, String, String, String, String, String>> {
        @Override
        public boolean filter(Tuple7<String, String, String, String, String, String, String> tuple) throws Exception {
            return tuple.getField(4).equals("view");
        }
    }

    /**
     * Map ad ids to campaigns using cached data from Redis
     */
    private static final class RedisJoinBolt extends RichFlatMapFunction<Tuple2<String, String>, Tuple3<String, String, String>> {

        private RedisAdCampaignCache redisAdCampaignCache;
        private BenchmarkConfig config;

        public RedisJoinBolt(BenchmarkConfig config) {
            this.config = config;
        }

        @Override
        public void open(Configuration parameters) {
            //initialize jedis
            String redis_host = config.redisHost;
            LOG.info("Opening connection with Jedis to {}", redis_host);
            this.redisAdCampaignCache = new RedisAdCampaignCache(redis_host);
            this.redisAdCampaignCache.prepare();
        }

        @Override
        public void flatMap(Tuple2<String, String> input, Collector<Tuple3<String, String, String>> out) throws Exception {
            String ad_id = input.f0;
            String campaign_id = this.redisAdCampaignCache.execute(ad_id);
            if (campaign_id == null) {
                return;
            }

            // campaign_id ad_id event_time
            out.collect(new Tuple3<>(campaign_id, input.f0, input.f1));
        }
    }

    /**
     * (campaign id, event time, 1)
     */
    private static class MapToImpressionCount implements MapFunction<Tuple3<String, String, String>, Tuple4<String, String, Long, String>> {
        @Override
        public Tuple4<String, String, Long, String> map(Tuple3<String, String, String> t) {
            return new Tuple4<>(t.f0, t.f1, 1L, t.f2);
        }
    }

    /**
     * Simplified version of Redis data structure
     */
    private static class RedisResultSinkOptimized extends RichSinkFunction<Tuple5<String, String, Long, String, String>> {
        private final BenchmarkConfig config;
        private Jedis flushJedis;

        public RedisResultSinkOptimized(BenchmarkConfig config) {
            this.config = config;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            flushJedis = new Jedis(config.redisHost);
            flushJedis.select(1); // select db 1
        }

        @Override
        public void invoke(Tuple5<String, String, Long, String, String> result) throws Exception {
            long currTime = System.currentTimeMillis();
            //currTime - the timestamp that generates the watermark which triggeres this window
            long eventTimeLatency = currTime - Long.parseLong(result.f1);
            String out = String.format("%d %d %d %d -",
                    result.f2, eventTimeLatency, currTime, getRuntimeContext().getIndexOfThisSubtask() + 1,
                    result.f4);

            flushJedis.hset(result.f0, result.f1, out);
            System.out.println("LLL" + out + "LLL");

//            StringBuilder sb = new StringBuilder();
//            sb.append(result.f2); //count
//            sb.append(' ');
//            sb.append(eventTimeLatency);
//            sb.append(' ');
//            sb.append(currTime);
//            sb.append(' ');
//            sb.append(getRuntimeContext().getIndexOfThisSubtask() + 1);
//            sb.append(' ');
//            sb.append(result.f4);
//            flushJedis.hset(result.f0, result.f1, sb.toString());

        }

        @Override
        public void close() throws Exception {
            super.close();
            flushJedis.close();
        }
    }

    /**
     * Simplified version of Redis data structure
     */
    private static FlinkKafkaProducer011<Tuple5<String, String, Long, String, String>> kafkaSink(BenchmarkConfig config) {
            Properties properties = new Properties();
            properties.setProperty("bootstrap.servers", config.bootstrapServers);
            properties.setProperty("group.id", config.groupId);
            properties.put("transaction.timeout.ms", "900000");
        return new FlinkKafkaProducer011<Tuple5<String, String, Long, String, String>>(
                    config.kafkaSinkTopic,
                    new KeyedSerializationSchema<Tuple5<String, String, Long, String, String>>() {

                        @Override
                        public byte[] serializeKey(Tuple5<String, String, Long, String, String> t) {
                            //campaignID + window end
                            return (t.f0 + " " + t.f1).getBytes();
                        }

                        @Override
                        public byte[] serializeValue(Tuple5<String, String, Long, String, String> t) {
                            return strForSink(t,-1).getBytes();
                        }

                        @Override
                        public String getTargetTopic(Tuple5<String, String, Long, String, String> t) {
                            return config.kafkaSinkTopic;
                        }
                    },
                    properties,
                    FlinkKafkaProducer011.Semantic.EXACTLY_ONCE
            );
    }

    public static String strForSink(Tuple5<String, String, Long, String, String> result, int subtask) {
        long currTime = System.currentTimeMillis();
        //currTime - the timestamp that generates the watermark which triggeres this window
        long eventTimeLatency = currTime - Long.parseLong(result.f1);
        return String.format("%d %d %d %d -",
                result.f2, eventTimeLatency, currTime, subtask, result.f4);
    }

    /**
     * Simplified version of Redis data structure
     */
    private static class RedisAdCount extends RichSinkFunction<Tuple3<String, String, Long>> {
        private final BenchmarkConfig config;
        private Jedis flushJedis;

        public RedisAdCount(BenchmarkConfig config) {
            this.config = config;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            flushJedis = new Jedis(config.redisHost);
            flushJedis.select(2); // select db 1
        }

        @Override
        public void invoke(Tuple3<String, String, Long> result) throws Exception {
            long currTime = System.currentTimeMillis();
            long eventTimeLatency = currTime - Long.parseLong(result.f1);
            StringBuilder sb = new StringBuilder();
            sb.append(result.f2); //count
            sb.append(' ');
            sb.append(eventTimeLatency);
            sb.append(' ');
            sb.append(currTime);
            sb.append(' ');
            sb.append(getRuntimeContext().getIndexOfThisSubtask() + 1);
            flushJedis.hset(result.f0, result.f1,
                    sb.toString()
            );

            System.out.println("LLL" + sb.toString() + "LLL");
        }

        @Override
        public void close() throws Exception {
            super.close();
            flushJedis.close();
        }
    }
}
