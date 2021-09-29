package flink.benchmark.utils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParseException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonMappingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.BaseJsonNode;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class CheckpointJsonParser {
    public static ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) throws IOException {
        String srcDir = "C:\\Users\\joinp\\Downloads\\results\\";
//        String srcDir = "C:\\Wenzhong\\我的坚果云\\实验\\results\\";
        Files.list(new File(srcDir).toPath()).forEach(dir -> {
            if (dir.toFile().isDirectory()) {
                System.out.println(dir.toString());
                try {
                    ArrayNode cpList = mapper.readValue(new File(dir.toFile(), "subtask-cp.json"), ArrayNode.class);
                    FileWriter fw = new FileWriter(new File(dir.toFile(), "subtask-cp.txt"));
                    fw.write("end_to_end_duration state_size\n");
                    for (JsonNode cp : cpList) {
                        if (cp.has("errors")) {
                            continue;
                        }

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
                } catch (JsonParseException e) {
                    e.printStackTrace();
                } catch (JsonMappingException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    public static void getJsonValue(JsonNode objectNode) {
        Iterator<Map.Entry<String, JsonNode>> iter = objectNode.fields();
        Map<String, String> res = new HashMap<>();
        while (iter.hasNext()) {
            Map.Entry<String, JsonNode> entry = iter.next();
            if (entry.getValue().isValueNode()) {
                res.put(entry.getKey(), entry.getValue().textValue());
            }
        }
    }

}
