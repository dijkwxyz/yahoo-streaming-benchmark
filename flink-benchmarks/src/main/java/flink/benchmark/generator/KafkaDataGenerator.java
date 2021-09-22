package flink.benchmark.generator;

import flink.benchmark.BenchmarkConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;

import java.io.FileNotFoundException;
import java.util.*;

public class KafkaDataGenerator {
    public static final String END_OF_STREAM_ELEMENT = "SHUTDOWN";
    private int adsIdx = 0;
    private int eventsIdx = 0;
    private StringBuilder sb = new StringBuilder();
    private String pageID = UUID.randomUUID().toString();
    private String userID = UUID.randomUUID().toString();
    private final String[] eventTypes = new String[]{"view", "click", "purchase"};

    private List<String> ads;
    private final Map<String, List<String>> campaigns;

    private final boolean isStreamEndless;
    private final long generateDataTimeSec;
    private final long numToGenerate;
    private final int loadTargetHz;
    private final int timeSliceLengthMs;
    private final String topic;
    private boolean running = true;
    private final List<Integer> partitions = new ArrayList<>();
    private int currPartition = 0;

    public Map<String, List<String>> getCampaigns() {
        return campaigns;
    }

    private final KafkaProducer<String, String> kafkaProducer;

    /**
     * @param config
     * @param dstHost      if not "", send to partitions with dstHost as leader
     * @param numCampaigns num ber campaign ids generated
     */
    public KafkaDataGenerator(BenchmarkConfig config, String dstHost, int numCampaigns, int loadTargetHz) {
        this.loadTargetHz = loadTargetHz;
        this.timeSliceLengthMs = config.timeSliceLengthMs;
        this.isStreamEndless = config.isStreamEndless;
        this.generateDataTimeSec = config.testTimeSeconds * 2 / 3;
        this.numToGenerate = generateDataTimeSec * loadTargetHz;
        this.campaigns = generateCampaigns(numCampaigns);
        this.ads = flattenCampaigns();

        this.topic = config.kafkaTopic;

        //kafka producer
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", config.bootstrapServers);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); //key 序列化
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer"); //value 序列化
        kafkaProducer = new KafkaProducer<>(properties);
        List<PartitionInfo> partitionInfos = kafkaProducer.partitionsFor(topic);
        if (dstHost.isEmpty()) {
            System.out.print("Host is not defined. Send to partition: ");
            for (PartitionInfo p : partitionInfos) {
                partitions.add(p.partition());
                System.out.print(p.partition());
            }
            System.out.print('\n');
        } else {
            System.out.print("Host: " + dstHost);
            System.out.print(". Send to partition: ");
            for (PartitionInfo p : partitionInfos) {
                if (dstHost.equals(p.leader().host())) {
                    partitions.add(p.partition());
                    System.out.print(p.partition() + " ");
                }
            }
            System.out.print('\n');
        }

//        // register campaigns to redis
        Map<String, List<String>> campaigns = getCampaigns();
        RedisHelper redisHelper = new RedisHelper(config);
        redisHelper.prepareRedis(campaigns);
        redisHelper.writeCampaignFile(campaigns);
    }

    /**
     * Generate a single element
     */
    public String generateElement() {
        if (adsIdx == ads.size()) {
            adsIdx = 0;
        }
        if (eventsIdx == eventTypes.length) {
            eventsIdx = 0;
        }
        sb.setLength(0);
        sb.append("{\"user_id\":\"");
        sb.append(pageID);
        sb.append("\",\"page_id\":\"");
        sb.append(userID);
        sb.append("\",\"ad_id\":\"");
        sb.append(ads.get(adsIdx++));
        sb.append("\",\"ad_type\":\"");
        sb.append("banner78"); // value is immediately discarded. The original generator would put a string with 38/5 = 7.6 chars. We put 8.
        sb.append("\",\"event_type\":\"");
        sb.append(eventTypes[new Random().nextInt(eventTypes.length)]);
        sb.append("\",\"event_time\":\"");
        sb.append(System.currentTimeMillis());
        sb.append("\",\"ip_address\":\"1.2.3.4\"}");

        return sb.toString();
    }

    public int adjustLoad(int load) {
        return load;
    }

    public void stop() {
        running = false;
    }

    /**
     * Generate a random list of ads and campaigns
     */
    private Map<String, List<String>> generateCampaigns(int numCampaigns) {
        int numAdsPerCampaign = 10;
        Map<String, List<String>> adsByCampaign = new LinkedHashMap<>();
        for (int i = 0; i < numCampaigns; i++) {
//            String campaign = UUID.randomUUID().toString();
            String campaign = String.valueOf(i);
            ArrayList<String> ads = new ArrayList<>();
            adsByCampaign.put(campaign, ads);
            for (int j = 0; j < numAdsPerCampaign; j++) {
                ads.add(UUID.randomUUID().toString());
            }
        }

        return adsByCampaign;
    }

    /**
     * Flatten into just ads
     */
    private List<String> flattenCampaigns() {
        // Flatten campaigns into simple list of ads
        List<String> ads = new ArrayList<>();
        for (Map.Entry<String, List<String>> entry : campaigns.entrySet()) {
            for (String ad : entry.getValue()) {
                ads.add(ad);
            }
        }
        return ads;
    }

    private long numSent = 0;
    private long lastnumSent = 0;
    private long logFreq = 1_000_000;
    private long lastLogTimeMs = -1;
    private long elementSize = 240;

    public void recordThroughput() {
        if (numSent % logFreq == 0) {
            // throughput over entire time
            long now = System.currentTimeMillis();

            // throughput for the last "logFreq" elements
            if (lastLogTimeMs == -1) {
                // init (the first)
                lastLogTimeMs = now;
                lastnumSent = numSent;
            } else {
                long timeDiff = now - lastLogTimeMs;
                long elementDiff = numSent - lastnumSent;
                double ex = (1000 / (double) timeDiff);
                System.out.println(String.format("From %d to %d (%d ms), we've sent %d elements. That's %f elements/second/core. %f MB/sec/core. GB received %d",
                        lastLogTimeMs, now, timeDiff, elementDiff, elementDiff * ex, elementDiff * ex * elementSize / 1024 / 1024, (elementDiff * elementSize) / 1024 / 1024 / 1024));
                // reinit
                lastLogTimeMs = now;
                lastnumSent = numSent;
            }
        }
    }

    /**
     * The main loop
     */
    public void run() throws InterruptedException {
        int numElements = adjustLoad(loadPerTimeslice());

        while (running) {
            long emitStartTime = System.currentTimeMillis();
            if (!isStreamEndless && numSent >= numToGenerate) {
                //time to end the stream
                for (int i = 0; i < partitions.size(); i++) {
                    collect(END_OF_STREAM_ELEMENT);
                }
                break;
            }
            for (int i = 0; i < numElements; i++) {
                collect(generateElement());
                // recordThroughput();
            }
            // Sleep for the rest of timeslice if needed
            long emitTime = System.currentTimeMillis() - emitStartTime;
            if (emitTime < timeSliceLengthMs) {
                Thread.sleep(timeSliceLengthMs - emitTime);
            }
        }
        kafkaProducer.close();
        System.out.println("Kafka Data Generator is closed. Elements generated: " + numSent);
    }

    public void collect(String element) {
        kafkaProducer.send(new ProducerRecord<>(topic, partitions.get(currPartition), String.valueOf(currPartition), element));
        currPartition = (currPartition + 1) % partitions.size();
        numSent++;
    }

    private int loadPerTimeslice() {
        return loadTargetHz / (1000 / timeSliceLengthMs);
    }

    /**
     * usage: main <yaml-path> <partition-leader-id> <num-producer>
     */
    public static void main(String[] args) throws FileNotFoundException, InterruptedException {
//        args = "conf/benchmarkConf.yaml \"\" 2".split(" ");

        //take args[0]
        BenchmarkConfig config = BenchmarkConfig.fromArgs(args);
        System.out.println("load-" + config.loadTargetHz);

        int numBrokers = Integer.parseInt(args[2]);

        KafkaDataGenerator k = new KafkaDataGenerator(config, args[1],
                config.numCampaigns / numBrokers,
                config.loadTargetHz / numBrokers);
        k.run();


//        new Thread(() -> {
//            KafkaDataGenerator k = new KafkaDataGenerator(config);
//            try {
//                k.run();
//                Thread.sleep(10_000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//            k.stop();
//        }).start();


    }
}
