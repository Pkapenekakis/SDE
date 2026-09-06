package infore.SDE.transformations.onepass.debug;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.File;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * DEBUG / VALIDATION ONLY.
 *
 * Completely independent offline merger for worker dump files.
 *
 * It knows nothing about:
 *   - OnePassShardOwnership
 *   - Phase-1 contribution calculation
 *   - Kafka
 *   - State Topic
 *   - SDE reducers
 *   - worker routing
 *
 * Its only operation is:
 *
 *      global edge index = union(worker edge indexes)
 *
 * A duplicate (edgeId, joinKey) on two workers is treated as an ERROR.
 */
public final class OnePassPhaseOneValidatorShardMerger {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private OnePassPhaseOneValidatorShardMerger() {}

    public static File merge(File shardDirectory, int expectedWorkers, File outputFile) throws Exception {

        if (shardDirectory == null || !shardDirectory.exists() || !shardDirectory.isDirectory()) {

            throw new IllegalArgumentException("Invalid shard directory: " + shardDirectory);
        }

        if (expectedWorkers <= 0) {
            throw new IllegalArgumentException("expectedWorkers must be > 0");
        }

        Map<String, Map<String, Double>> merged = new LinkedHashMap<String, Map<String, Double>>();

        Integer expectedUid = null;

        for (int workerId = 0; workerId < expectedWorkers; workerId++) {

            File shardFile = new File(shardDirectory, "worker-" + workerId + ".json");

            if (!shardFile.exists()) {
                throw new IllegalStateException("Missing worker shard file: " + shardFile.getAbsolutePath());
            }

            JsonNode root = MAPPER.readTree(shardFile);

            int fileWorker = root.path("workerId").asInt(-1);

            if (fileWorker != workerId) {
                throw new IllegalStateException("Worker id mismatch in " + shardFile + ": expected=" + workerId +
                        ", actual=" + fileWorker);
            }

            int fileExpectedWorkers = root.path("expectedWorkers").asInt(-1);

            if (fileExpectedWorkers != expectedWorkers) {
                throw new IllegalStateException("expectedWorkers mismatch in " + shardFile + ": expected=" +
                        expectedWorkers + ", actual=" + fileExpectedWorkers);
            }

            int uid = root.path("uid").asInt(-1);

            if (expectedUid == null) {
                expectedUid = uid;
            } else if (expectedUid.intValue() != uid) {
                throw new IllegalStateException("Shard files belong to different UIDs. " + "Expected uid=" +
                        expectedUid + ", found=" + uid + " in " + shardFile);
            }

            JsonNode edgeIndexes = root.get("edgeIndexes");

            if (edgeIndexes == null || !edgeIndexes.isObject()) {

                throw new IllegalStateException(
                        "Missing edgeIndexes object in " + shardFile.getAbsolutePath()
                );
            }

            Iterator<Map.Entry<String, JsonNode>> edges = edgeIndexes.fields();

            while (edges.hasNext()) {

                Map.Entry<String, JsonNode> edgeEntry = edges.next();
                String edgeId = edgeEntry.getKey();
                JsonNode entriesNode = edgeEntry.getValue();

                if (!entriesNode.isObject()) {
                    throw new IllegalStateException("edgeIndexes['" + edgeId + "'] is not an object in " + shardFile);
                }

                Map<String, Double> globalEdge = merged.get(edgeId);

                if (globalEdge == null) {
                    globalEdge = new LinkedHashMap<String, Double>();

                    merged.put(edgeId, globalEdge
                    );
                }

                Iterator<Map.Entry<String, JsonNode>> entries = entriesNode.fields();

                while (entries.hasNext()) {

                    Map.Entry<String, JsonNode> entry = entries.next();

                    String joinKey = entry.getKey();

                    double weight = entry.getValue().asDouble();

                    /*
                     * This is intentionally NOT a sum.
                     *
                     * In the sharded design one final edge/key belongs to one
                     * worker. Seeing the same edge/key on two worker dumps is
                     * itself a correctness failure.
                     */
                    if (globalEdge.containsKey(joinKey)) {

                        throw new IllegalStateException(
                                "Duplicate sharded Phase-1 key detected."
                                        + " edgeId=" + edgeId
                                        + ", joinKey=" + joinKey
                                        + ", secondWorker=" + workerId
                                        + ", previousWeight="
                                        + globalEdge.get(joinKey)
                                        + ", secondWeight="
                                        + weight
                        );
                    }

                    globalEdge.put(joinKey, weight);
                }
            }
        }

        ObjectNode validatorOutput = MAPPER.createObjectNode();

        /*
         * EXACT FIELD expected by validate_onepass_catalog_phase1.py.
         */
        validatorOutput.set("edgeIndexes", MAPPER.valueToTree(merged));

        File parent = outputFile.getParentFile();

        if (parent != null && !parent.exists() && !parent.mkdirs()) {

            throw new IllegalStateException("Could not create output directory: " + parent.getAbsolutePath());
        }

        MAPPER.writerWithDefaultPrettyPrinter().writeValue(outputFile, validatorOutput);

        System.out.println("[OnePass DEBUG MERGE] Validator file written: " + outputFile.getAbsolutePath());

        return outputFile;
    }


    /**
     * Optional standalone runner.
     *
     * args:
     *   0 = shard directory
     *   1 = expectedWorkers
     *   2 = final validator JSON
     */
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: OnePassPhaseOneValidatorShardMerger " + "<shard-directory> " + "<expected-workers> " + "<output-json>");
            System.exit(1);
        }

        merge(new File(args[0]), Integer.parseInt(args[1]), new File(args[2]));
    }
}