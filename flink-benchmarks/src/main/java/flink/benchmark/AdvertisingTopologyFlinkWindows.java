/**
 * Copyright 2015, Yahoo Inc.
 * Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.
 */
package flink.benchmark;

import benchmark.common.advertising.RedisAdCampaignCache;
import com.dijk.multilevel.PatternBasedMultilevelStateBackend;
import flink.benchmark.generator.EventGeneratorSource;
import flink.benchmark.generator.RedisHelper;
import flink.benchmark.utils.FailureInjectorMap;
import flink.benchmark.utils.StateBackendFactory;
import flink.benchmark.utils.ThroughputLogger;
import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.*;

/**
 * To Run:  flink run -c flink.benchmark.AdvertisingTopologyFlinkWindows flink-benchmarks-0.1.0.jar "benchmarkConf.yaml"
 * <p>
 * This job variant uses Flinks built-in windowing and triggering support to compute the windows
 * and trigger when each window is complete as well as once per second.
 */
public class AdvertisingTopologyFlinkWindows {

    private static final Logger LOG = LoggerFactory.getLogger(AdvertisingTopologyFlinkWindows.class);

    public static void main(final String[] args) throws Exception {

        BenchmarkConfig config = BenchmarkConfig.fromArgs(args);

        StreamExecutionEnvironment env = setupEnvironment(config);

        DataStream<String>  rawMessageStream = streamSource(config, env);
//        rawMessageStream.print("raw");
        // log performance
        rawMessageStream.flatMap(new ThroughputLogger<String>(240, config.throughputLogFreq));


        //out: (campaign id, event time)
        DataStream<Tuple2<String, String>> joinedAdImpressions = rawMessageStream
                .flatMap(new DeserializeBolt())
                .filter(new EventFilterBolt())
                .<Tuple2<String, String>>project(2, 5) //ad_id, event_time
                .map(new FailureInjectorMap<>(config.mttiMs, env.getParallelism()))
                .flatMap(new RedisJoinBolt(config)) // campaign_id, event_time
                .assignTimestampsAndWatermarks(WatermarkStrategy.
                        <Tuple2<String, String>>forMonotonousTimestamps().
                        withTimestampAssigner(
                                (Tuple2<String, String> event, long timestamp) ->
                                        Long.parseLong(event.f1))); // extract timestamps and generate watermarks from event_time

        //out: (campaign id, event time, 1)
        WindowedStream<Tuple3<String, String, Long>, String, TimeWindow> windowStream = joinedAdImpressions
                .map(new MapToImpressionCount())
//                .keyBy((a) -> a.f0) // key by campaign_id
//                .process(new FailureInjector(config.mttiMs, config.numCampaigns))
                .keyBy((a) -> a.f0)
                .timeWindow(Time.seconds(config.windowSize), Time.seconds(config.windowSlide));

        // set a custom trigger
        windowStream.trigger(new EventAndProcessingTimeTrigger());

        // campaign_id, window-end, count, trigger-time
        DataStream<Tuple4<String, String, Long, String>> result =
                windowStream.process(sumProcessFunction());
//        DataStream<Tuple4<String, String, Long, String>> result =
//                windowStream.reduce(sumReduceFunction(), sumWindowFunction());

//        result.print("process");
//        result2.print("reduce");
        // write result to redis
//        if (config.getParameters().has("add.result.sink.optimized")) {
        result.addSink(new RedisResultSinkOptimized(config));
//        } else {
//            result.addSink(new RedisResultSink(config));
//        }

        env.execute("AdvertisingTopologyFlinkWindows");
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

            Map<String, List<String>> campaigns = eventGenerator.getCampaigns();
            RedisHelper redisHelper = new RedisHelper(config);
            redisHelper.prepareRedis(campaigns);
            redisHelper.writeCampaignFile(campaigns);
        } else {
            source = kafkaSource(config);
            sourceName = "Kafka";
        }

        return env.addSource(source, sourceName);
    }

    /**
     * Setup Flink environment
     */
    private static StreamExecutionEnvironment setupEnvironment(BenchmarkConfig config) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(config.getParameters());

        if (config.checkpointsEnabled) {
            env.enableCheckpointing(config.checkpointInterval);

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

    private static ProcessWindowFunction<Tuple3<String, String, Long>, Tuple4<String, String, Long, String>, String, TimeWindow> sumProcessFunction() {
        return new ProcessWindowFunction<Tuple3<String, String, Long>, Tuple4<String, String, Long, String>, String, TimeWindow>() {
            @Override
            public void process(String s, Context context, Iterable<Tuple3<String, String, Long>> elements, Collector<Tuple4<String, String, Long, String>> out) throws Exception {
                long sum = 0;
                Long max = Long.MIN_VALUE;
                // campaign_id, window-end, count, trigger-time
                Tuple4<String, String, Long, String> res = new Tuple4<>();
                for (Tuple3<String, String, Long> e : elements) {
                    if (sum == 0) {
                        res.f0 = e.f0;
                    }
                    sum += e.f2;
                }
                res.f1 = String.valueOf(context.window().getEnd());
                res.f2 = sum;
                res.f3 = String.valueOf(System.currentTimeMillis());
                out.collect(res);
            }
        };
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
        FlinkKafkaConsumer011<String> consumer = new FlinkKafkaConsumer011<>(
                config.kafkaTopic,
                new SimpleStringSchema(),
                properties);
        return consumer;
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
    private static final class RedisJoinBolt extends RichFlatMapFunction<Tuple2<String, String>, Tuple2<String, String>> {

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
        public void flatMap(Tuple2<String, String> input, Collector<Tuple2<String, String>> out) throws Exception {
            String ad_id = input.f0;
            String campaign_id = this.redisAdCampaignCache.execute(ad_id);
            if (campaign_id == null) {
                return;
            }

            // campaign_id event_time
            Tuple2<String, String> tuple = new Tuple2<>(campaign_id, input.f1);
            out.collect(tuple);
        }
    }

    /**
     *
     */
    private static class MapToImpressionCount implements MapFunction<Tuple2<String, String>, Tuple3<String, String, Long>> {
        @Override
        public Tuple3<String, String, Long> map(Tuple2<String, String> t2) {
            return new Tuple3<>(t2.f0, t2.f1, 1L);
        }
    }

    /**
     * Simplified version of Redis data structure
     */
    private static class RedisResultSinkOptimized extends RichSinkFunction<Tuple4<String, String, Long, String>> {
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
        public void invoke(Tuple4<String, String, Long, String> result) throws Exception {
            // redis set: key: campaign id, field: window-timestamp,
            // data: count event-time-latency processing-time-latency subtask
            long currTime = System.currentTimeMillis();
            long eventTimeLatency = currTime - Long.parseLong(result.f1);
            // this value is not very meaningful for windows
//            long processingTimeLatency = currTime - Long.parseLong(result.f3);
            long windowEnd = Long.parseLong(result.f3);
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
            LOG.info("#$#$#$ {}", sb.toString());
        }

        @Override
        public void close() throws Exception {
            super.close();
            flushJedis.close();
        }
    }
}
