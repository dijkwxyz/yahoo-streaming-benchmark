package flink.benchmark;

import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

public class Test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<String> rawMessageStream = env.readTextFile("C:\\data\\flink-ec2-user-taskexecutor-0-wenzhongduan-multilevelcp-flink-worker-1.novalocal.out");
//        rawMessageStream.print("raw");

        //out: (campaign id, event time)
        DataStream<Tuple2<String, String>> joinedAdImpressions = rawMessageStream
                .flatMap(new DeserializeBolt())
//                .filter(new EventFilterBolt())
                .<Tuple2<String, String>>project(2, 5) //ad_id, event_time
                .flatMap(new RedisJoinBolt()) // campaign_id, event_time
                .assignTimestampsAndWatermarks(WatermarkStrategy.
                        <Tuple2<String, String>>forMonotonousTimestamps().
                        withTimestampAssigner(
                                (Tuple2<String, String> event, long timestamp) ->
                                        Long.parseLong(event.f1))); // extract timestamps and generate watermarks from event_time

        //out: (ad id, event time, 1)
        WindowedStream<Tuple3<String, String, Long>, Tuple, TimeWindow> windowStream = joinedAdImpressions
                .map(new MapToImpressionCount())
                .keyBy(0) // campaign_id
                .timeWindow(Time.milliseconds(100));

        // set a custom trigger
//        windowStream.trigger(new EventAndProcessingTimeTrigger());
/*
process> (0,1616556419860,278)
process> (0,1616556419870,452)
process> (0,1616556419880,754)
 */
        // campaign_id, window end time, count
        DataStream<Tuple3<String, String, Long>> result =
                windowStream.process(sumProcessFunction());
        DataStream<Tuple3<String, String, Long>> result2 =
                windowStream.reduce(sumReduceFunction(), sumWindowFunction());

        result.print("process");
        result2.print("reduce");
        // write result to redis

        env.execute("test");
    }

    private static ProcessWindowFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, Tuple, TimeWindow> sumProcessFunction() {
        return new ProcessWindowFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, Tuple, TimeWindow>() {
            @Override
            public void process(Tuple tuple, Context context, Iterable<Tuple3<String, String, Long>> elements, Collector<Tuple3<String, String, Long>> out) throws Exception {
                long sum = 0;
                Tuple3<String, String, Long> res = new Tuple3<>();
                for (Tuple3<String, String, Long> e : elements) {
                    if (sum == 0) {
                        res.f0 = e.f0;
                    }
                    sum += e.f2;
                }
                res.f1 = Long.toString(context.window().getEnd());
                res.f2 = sum;
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
                return new Tuple3<>(t0.f0, t0.f1, t0.f2 + t1.f2);
            }
        };
    }

    /**
     * Sum - Window function, summing already happened in reduce function
     */
    private static WindowFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, Tuple, TimeWindow> sumWindowFunction() {
        return new WindowFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, Tuple, TimeWindow>() {
            @Override
            public void apply(Tuple keyTuple, TimeWindow window, Iterable<Tuple3<String, String, Long>> values, Collector<Tuple3<String, String, Long>> out) throws Exception {
                Iterator<Tuple3<String, String, Long>> valIter = values.iterator();
                Tuple3<String, String, Long> tuple = valIter.next();
                if (valIter.hasNext()) {
                    throw new IllegalStateException("Unexpected");
                }
                tuple.f1 = Long.toString(window.getEnd());
                out.collect(tuple); // collect end time here
            }
        };
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

        public RedisJoinBolt() {
        }

        @Override
        public void open(Configuration parameters) {
        }

        @Override
        public void flatMap(Tuple2<String, String> input, Collector<Tuple2<String, String>> out) throws Exception {
            String ad_id = input.getField(0);
            String campaign_id = "0";
            if (campaign_id == null) {
                return;
            }

            Tuple2<String, String> tuple = new Tuple2<>(campaign_id, (String) input.getField(1)); // event_time
            out.collect(tuple);
        }
    }

    /**
     *
     */
    private static class MapToImpressionCount implements MapFunction<Tuple2<String, String>, Tuple3<String, String, Long>> {
        @Override
        public Tuple3<String, String, Long> map(Tuple2<String, String> t3) throws Exception {
            return new Tuple3<>(t3.f0, t3.f1, 1L);
        }
    }

}
