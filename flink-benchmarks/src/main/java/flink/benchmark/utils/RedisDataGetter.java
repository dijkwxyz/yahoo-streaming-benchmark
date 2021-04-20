package flink.benchmark.utils;

import flink.benchmark.BenchmarkConfig;
import redis.clients.jedis.Jedis;

import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.Set;


public class RedisDataGetter {
    private final BenchmarkConfig config;
    private Jedis jedis;

    public RedisDataGetter(String yamlFile) throws FileNotFoundException {
        this.config = new BenchmarkConfig(yamlFile);
        jedis = new Jedis(config.redisHost);
        jedis.select(1); // select db 1
    }

    public void execute() throws IOException {
        FileWriter fileWriter = new FileWriter("seen-updated-subtask.txt");
        Set<String> keys = jedis.keys("*");
        for (String campaignId : keys) {
            if ("campaign".equals(campaignId)) {
                continue;
            }
            // campaign id -> (window-timestamp, count + latency + subtask)
            Map<String, String> data = jedis.hgetAll(campaignId);
            for (String windowEnd : data.keySet()) {
                String line = data.get(windowEnd);
                fileWriter.write(line);
                fileWriter.write('\n');
            }
        }

        fileWriter.close();
    }

    public static void main(String[] args) throws IOException {
        RedisDataGetter redisDataGetter = new RedisDataGetter(args[0]);
//        RedisDataGetter redisDataGetter = new RedisDataGetter("conf/benchmarkConf.yaml");
        redisDataGetter.execute();
    }

}
