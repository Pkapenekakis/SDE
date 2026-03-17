package infore.SDE.synopses.OnePassSampler;

import com.fasterxml.jackson.databind.JsonNode;

import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public class OnePassTuple implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String table;
    private final JsonNode rawJson;
    private final Map<String, JsonNode> fields;

    public OnePassTuple(String table, JsonNode rawJson) {
        this.table = table;
        this.rawJson = rawJson;
        this.fields = new LinkedHashMap<>();

        if (rawJson != null) {
            Iterator<Map.Entry<String, JsonNode>> it = rawJson.fields();
            while (it.hasNext()) {
                Map.Entry<String, JsonNode> entry = it.next();
                this.fields.put(entry.getKey(), entry.getValue());
            }
        }
    }

    public String getTable() {
        return table;
    }

    public JsonNode getRawJson() {
        return rawJson;
    }

    public Map<String, JsonNode> getFields() {
        return fields;
    }

    public JsonNode getField(String fieldName) {
        return fields.get(fieldName);
    }

    public String getFieldAsText(String fieldName) {
        JsonNode node = fields.get(fieldName);
        return node == null || node.isNull() ? null : node.asText();
    }

    public Integer getFieldAsInt(String fieldName) {
        JsonNode node = fields.get(fieldName);
        return node == null || node.isNull() ? null : node.asInt();
    }

    public Long getFieldAsLong(String fieldName) {
        JsonNode node = fields.get(fieldName);
        return node == null || node.isNull() ? null : node.asLong();
    }

    public Double getFieldAsDouble(String fieldName) {
        JsonNode node = fields.get(fieldName);
        return node == null || node.isNull() ? null : node.asDouble();
    }

    public boolean hasField(String fieldName) {
        return fields.containsKey(fieldName);
    }

    @Override
    public String toString() {
        return "OnePassTuple{" +
                "table='" + table + '\'' +
                ", fields=" + fields +
                '}';
    }
}