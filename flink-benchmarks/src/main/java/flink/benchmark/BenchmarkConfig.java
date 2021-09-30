package flink.benchmark;

import org.apache.flink.api.java.utils.ParameterTool;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Encapsulating configuration in once place
 */
public class BenchmarkConfig implements Serializable {

    // Kafka
    public final String kafkaTopic;
    public final String kafkaSinkTopic;
//    public final int kafkaPartition;
    public final String bootstrapServers;
    public final String groupId;

    // Load Generator
    public final int cpuLoadAdjuster;
    public final int loadTargetHz;
    public final int timeSliceLengthMs;
    public final boolean useLocalEventGenerator;
    public final int numCampaigns;
    public final int numAdPerCampaign;

    // Redis
    public final String redisHost;
    public final int redisDb;
    public final boolean redisFlush;
    public final int numRedisThreads;

    // Akka
//  public final String akkaZookeeperQuorum;
//  public final String akkaZookeeperPath;

    // Application
    public final long windowSize;
    public final long windowSlide;
    public final long failureStartTimeDelayMs;
    public final boolean isStreamEndless;
    public final long testTimeSeconds;
    public final long generateDataTimeSeconds;

    // Flink
    public final long checkpointInterval;
    public final long checkpointMinPause;
    public final boolean checkpointsEnabled;
//  public final String checkpointUri;
//  public boolean checkpointToUri;

    // The raw parameters
    public final ParameterTool parameters;

    public final boolean multilevelEnable;
    public final String multilevelLevel0Type;
    public final String multilevelLevel0Path;
    public final String multilevelLevel1Type;
    public final String multilevelLevel1Path;
    public final String multilevelLevel2Type;
    public final String multilevelLevel2Path;
    public final String multilevelPattern;
    public final String singlelevelPath;
    public final String singlelevelStateBackend;
    public final int maxMemStateSize;
    public final long mttiMs;
    public final boolean injectWithProbability;
    public final int throughputLogFreq;
    public final int parallelism;
    /**
     * Create a config starting with an instance of ParameterTool
     */
    public BenchmarkConfig(ParameterTool parameterTool) {
        this.parameters = parameterTool;

        // load generator
        this.loadTargetHz = parameterTool.getInt("load.target.hz", 400_000);
        this.cpuLoadAdjuster = parameterTool.getInt("cpu.load.adjuster", 1);
        this.timeSliceLengthMs = parameterTool.getInt("load.time.slice.length.ms", 100);
        this.useLocalEventGenerator = parameters.getBoolean("use.local.event.generator", false);
        this.numCampaigns = parameterTool.getInt("num.campaigns", 1_000_000);
        this.numAdPerCampaign = parameterTool.getInt("num.ad.per.campaigns", 10);

        // Kafka
        this.kafkaTopic = parameterTool.getRequired("kafka.topic");
        this.kafkaSinkTopic = parameterTool.get("kafka.sink.topic", "sink");
//        this.kafkaPartition = parameterTool.getInt("kafka.partitions", 1);
        this.bootstrapServers = parameterTool.getRequired("bootstrap.servers");
        this.groupId = parameterTool.getRequired("group.id");

        // Redis
        this.redisHost = parameterTool.get("redis.host", "localhost");
        this.redisDb = parameterTool.getInt("redis.db", 0);
        this.redisFlush = parameterTool.getBoolean("redis.flush", false);
        this.numRedisThreads = parameterTool.getInt("redis.threads", 20);

        // Akka
//    this.akkaZookeeperQuorum = parameterTool.get("akka.zookeeper.quorum", "localhost");
//    this.akkaZookeeperPath = parameterTool.get("akka.zookeeper.path", "/akkaQuery");

        // Application
        this.windowSize = parameterTool.getLong("window.size", 10);
        this.windowSlide = parameterTool.getLong("window.slide", 5);
        this.failureStartTimeDelayMs = parameterTool.getLong("failure.start.delay.ms", 0);
        this.isStreamEndless = parameterTool.getBoolean("stream.endless", true);
        this.testTimeSeconds = parameterTool.getLong("test.time.seconds", 0);
        this.generateDataTimeSeconds = parameterTool.getLong("generate.data.time.seconds", testTimeSeconds);

        // Flink
        this.checkpointInterval = parameterTool.getLong("flink.checkpoint.interval", 0);
        this.checkpointMinPause = parameterTool.getLong("flink.checkpoint.min-pause", 0);
        this.checkpointsEnabled = checkpointInterval > 0;
//        this.checkpointUri = parameterTool.get("flink.checkpoint.uri", "");
//    this.checkpointToUri = checkpointUri.length() > 0;

        /*
        multilevel.enable: true
        multilevel.level0.type: memory
        multilevel.level1.type: fs
        multilevel.level1.path: file:///home/ec2-user/yahoo-streaming-benchmark/flink-1.11.2/data/checkpoints/fs
        multilevel.level2.type: fs
        multilevel.level2.path: hdfs://115.146.92.102:9000/flink/checkpoints
        multilevel.pattern: 1,2,3
        */
        this.multilevelEnable = parameterTool.getBoolean("multilevel.enable", false);
        this.multilevelLevel0Type = parameterTool.get("multilevel.level0.statebackend", null);
        this.multilevelLevel0Path = parameterTool.get("multilevel.level0.path", null);
        this.multilevelLevel1Type = parameterTool.get("multilevel.level1.statebackend", null);
        this.multilevelLevel1Path = parameterTool.get("multilevel.level1.path", null);
        this.multilevelLevel2Type = parameterTool.get("multilevel.level2.statebackend", null);
        this.multilevelLevel2Path = parameterTool.get("multilevel.level2.path", null);
        this.multilevelPattern = parameterTool.get("multilevel.pattern", null);
        this.singlelevelPath = parameterTool.get("singlelevel.path", "/tmp/data");
        this.singlelevelStateBackend = parameterTool.get("singlelevel.statebackend", "fs");

        this.maxMemStateSize = parameterTool.getInt("max.memory.state.size", 5242880);

        this.mttiMs = parameterTool.getLong("mtti.ms", 20_000);
        this.injectWithProbability = parameterTool.getBoolean("failure.inject.useProbability");

        this.throughputLogFreq = parameterTool.getInt("throughput.log.freq", loadTargetHz * 5);
        this.parallelism = parameterTool.getInt("test.parallelism");
    }

    /**
     * Creates a config given a Yaml file
     */
    public BenchmarkConfig(String yamlFile) throws FileNotFoundException {
        this(yamlToParameters(yamlFile));
    }

    /**
     * Create a config directly from the command line arguments
     */
    public static BenchmarkConfig fromArgs(String[] args) throws FileNotFoundException {
        if (args.length < 1) {
            return new BenchmarkConfig("conf/benchmarkConf.yaml");
        } else {
            return new BenchmarkConfig(args[0]);
        }
    }

    /**
     * Get the parameters
     */
    public ParameterTool getParameters() {
        return this.parameters;
    }

    private static ParameterTool yamlToParameters(String yamlFile) throws FileNotFoundException {
        // load yaml file
        Yaml yml = new Yaml(new SafeConstructor());
        Map<String, String> ymlMap = (Map) yml.load(new FileInputStream(yamlFile));

        String kafkaZookeeperConnect = getZookeeperServers(ymlMap, String.valueOf(ymlMap.get("kafka.zookeeper.path")));
//    String akkaZookeeperQuorum = getZookeeperServers(ymlMap, "");

        // We need to add these values as "parameters"
        // -- This is a bit of a hack but the Kafka consumers and producers
        //    expect these values to be there
        ymlMap.put("zookeeper.connect", kafkaZookeeperConnect); // set ZK connect for Kafka
        ymlMap.put("bootstrap.servers", getKafkaBrokers(ymlMap));
//    ymlMap.put("akka.zookeeper.quorum", akkaZookeeperQuorum);
        ymlMap.put("auto.offset.reset", "latest");
        ymlMap.put("group.id", UUID.randomUUID().toString());

        // Convert everything to strings
        for (Map.Entry e : ymlMap.entrySet()) {
            {
                e.setValue(e.getValue().toString());
            }
        }
        return ParameterTool.fromMap(ymlMap);
    }

    private static String getZookeeperServers(Map conf, String zkPath) {
        if (!conf.containsKey("zookeeper.servers")) {
            throw new IllegalArgumentException("Not zookeeper servers found!");
        }
        return listOfStringToString((List<String>) conf.get("zookeeper.servers"), String.valueOf(conf.get("zookeeper.port")), zkPath);
    }

    private static String getKafkaBrokers(Map conf) {
        if (!conf.containsKey("kafka.brokers")) {
            throw new IllegalArgumentException("No kafka brokers found!");
        }
        if (!conf.containsKey("kafka.port")) {
            throw new IllegalArgumentException("No kafka port found!");
        }
        return listOfStringToString((List<String>) conf.get("kafka.brokers"), String.valueOf(conf.get("kafka.port")), "");
    }


    private static String listOfStringToString(List<String> list, String port, String path) {
        String val = "";
        for (int i = 0; i < list.size(); i++) {
            val += list.get(i) + ":" + port + path;
            if (i < list.size() - 1) {
                val += ",";
            }
        }
        return val;
    }

}
