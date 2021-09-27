package flink.benchmark.utils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.BaseJsonNode;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class CheckpointJsonParser {
    public static ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) throws IOException {
        String dir = "C:\\Users\\joinp\\Downloads\\results\\";
        String name = "subtask-cp-16";
        ArrayNode cpList = mapper.readValue(new File(dir, name + ".json"), ArrayNode.class);
        FileWriter fw = new FileWriter(new File(dir, name + ".txt"));
        fw.write("end_to_end_duration state_size\n");
        for (JsonNode cp : cpList) {
            ArrayNode subtasks = (ArrayNode) cp.get("subtasks");
            for (JsonNode node : subtasks) {
                if ("completed".equals(node.get("status").textValue())) {
                    fw.write(node.get("index").asText());
                    fw.write(' ');
                    fw.write(node.get("end_to_end_duration").asText());
                    fw.write(' ');
                    fw.write(node.get("state_size").asText());
                    fw.write('\n');
                }
            }
        }

        fw.close();
    }

    public static void getJsonValue(JsonNode objectNode) {
        Iterator<Map.Entry<String, JsonNode>> iter = objectNode.fields();
        Map<String, String > res = new HashMap<>();
        while (iter.hasNext()) {
            Map.Entry<String, JsonNode> entry = iter.next();
            if (entry.getValue().isValueNode()) {
                res.put(entry.getKey(), entry.getValue().textValue());
            }
        }
    }

}
