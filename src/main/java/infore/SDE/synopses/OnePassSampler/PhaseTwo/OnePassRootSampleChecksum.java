package infore.SDE.synopses.OnePassSampler.PhaseTwo;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * DEBUG / VALIDATION ONLY.
 * <p>
 * Deterministic checksum of a fully installed Phase-2 root sample.
 * <p>
 * This class is never called by the normal algorithm path.
 */
public final class OnePassRootSampleChecksum {

    public static final String VERSION = "ONEPASS_PHASE2_ROOT_SAMPLE_SHA256_V1";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private OnePassRootSampleChecksum() {
    }


    public static String sha256(OnePassRootSampleResult result) {

        if (result == null) {
            throw new IllegalArgumentException("Phase-2 root sample result must not be null");
        }


        MessageDigest digest = newSha256();
        updateString(digest, VERSION);
        updateString(digest, safe(result.getRootAlias()));
        updateInt(digest, result.getRequestedSampleSize());
        updateLong(digest, result.getRootTuplesSeen());
        updateLong(digest, result.getPositiveRootCandidatesSeen());
        updateLong(digest, Double.doubleToLongBits(result.getTotalRootGroupWeight()));
        List<OnePassRootSampleInstance> ordered = new ArrayList<OnePassRootSampleInstance>(result.getSampleInstances());
        Collections.sort(ordered, new Comparator<OnePassRootSampleInstance>() {

            @Override
            public int compare(OnePassRootSampleInstance left, OnePassRootSampleInstance right) {
                int byInstanceId = Long.compare(left.getSampleInstanceId(), right.getSampleInstanceId());
                if (byInstanceId != 0) {
                    return byInstanceId;
                }
                return Long.compare(left.getSourceCandidateId(), right.getSourceCandidateId());
            }
        });

        Set<Long> seenSampleInstanceIds = new HashSet<Long>();

        for (OnePassRootSampleInstance instance : ordered) {
            if (!seenSampleInstanceIds.add(instance.getSampleInstanceId())) {
                throw new IllegalStateException("Duplicate Phase-2 sampleInstanceId=" + instance.getSampleInstanceId());
            }
        }


        updateInt(digest, ordered.size());


        for (OnePassRootSampleInstance instance : ordered) {
            updateLong(digest, instance.getSampleInstanceId());
            updateLong(digest, instance.getSourceCandidateId());
            updateString(digest, safe(instance.getRootAlias()));
            updateLong(digest, Double.doubleToLongBits(instance.getRootGroupWeight()));
            JsonNode canonicalTuple = canonicalizeJson(instance.getRootTuple());
            byte[] tupleBytes;

            try {

                tupleBytes = MAPPER.writeValueAsBytes(canonicalTuple);

            } catch (Exception e) {
                throw new IllegalStateException("Could not serialize canonical Phase-2 root tuple", e);
            }
            updateBytes(digest, tupleBytes);
        }


        return toHex(digest.digest());
    }


    private static JsonNode canonicalizeJson(JsonNode node) {

        if (node == null || node.isNull()) {
            return MAPPER.getNodeFactory().nullNode();
        }


        if (node.isObject()) {

            TreeMap<String, JsonNode> sorted = new TreeMap<String, JsonNode>();
            Iterator<Map.Entry<String, JsonNode>> fields = node.fields();

            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                sorted.put(field.getKey(), canonicalizeJson(field.getValue()));
            }

            ObjectNode out = MAPPER.createObjectNode();

            for (Map.Entry<String, JsonNode> field : sorted.entrySet()) {
                out.set(field.getKey(), field.getValue());
            }

            return out;
        }


        if (node.isArray()) {
            ArrayNode out = MAPPER.createArrayNode();

            for (JsonNode element : node) {
                out.add(canonicalizeJson(element));
            }
            return out;
        }

        return node.deepCopy();
    }


    private static MessageDigest newSha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available", e);
        }
    }


    private static void updateString(MessageDigest digest, String value) {
        updateBytes(digest, value.getBytes(StandardCharsets.UTF_8));
    }


    private static void updateBytes(MessageDigest digest, byte[] bytes) {
        if (bytes == null) {
            updateInt(digest, -1);
            return;
        }

        updateInt(digest, bytes.length);
        digest.update(bytes);
    }


    private static void updateInt(MessageDigest digest, int value) {
        digest.update(ByteBuffer.allocate(4).putInt(value).array());
    }


    private static void updateLong(MessageDigest digest, long value) {
        digest.update(ByteBuffer.allocate(8).putLong(value).array());
    }


    private static String safe(String value) {
        return value == null ? "" : value;
    }


    private static String toHex(byte[] bytes) {
        StringBuilder out = new StringBuilder(bytes.length * 2);

        for (byte value : bytes) {
            out.append(String.format("%02x", value & 0xff));
        }

        return out.toString();
    }
}