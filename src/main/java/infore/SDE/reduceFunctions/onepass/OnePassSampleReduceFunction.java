package infore.SDE.reduceFunctions.onepass;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import infore.SDE.messages.Estimation;
import infore.SDE.reduceFunctions.ReduceFunction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * Phase 2 parallel merge for OnePass*.
 *
 * Input:
 *   LOCAL_PHASE2_ROOT_SUMMARY from each worker.
 *
 * Each local summary contains:
 *   - rootTuplesSeen
 *   - positiveRootCandidatesSeen
 *   - totalRootGroupWeight
 *   - orderedReservoir entries with Efraimidis-Spirakis keys
 *
 * Output:
 *   GLOBAL_PHASE2_ROOT_SAMPLE as JSON string.
 *
 * Correctness idea:
 *   Local ES reservoirs are mergeable. The global ES reservoir is the top-n
 *   entries from the union of the local top-n reservoirs. Then the reducer
 *   performs the same online multinomial conversion using the global total
 *   root-group weight.
 */
public final class OnePassSampleReduceFunction extends ReduceFunction {

    private static final long serialVersionUID = 1L;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Set<Integer> receivedWorkers = new HashSet<Integer>();
    private final List<ReservoirCandidate> candidates = new ArrayList<ReservoirCandidate>();

    private int uid = -1;
    private String phase = "PHASE2";
    private String resultId = "PHASE2_RESULT";
    private String queryName = "";
    private String rootAlias = "";
    private String baseKey = "";
    private String datasetSeed = "";
    private int sampleSize = 1;
    private int expectedWorkers;

    private long rootTuplesSeen = 0L;
    private long positiveRootCandidatesSeen = 0L;
    private double totalRootGroupWeight = 0.0d;

    public OnePassSampleReduceFunction(int nOfP, int count, String[] parameters, int synID, int rqid) {
        super(nOfP, count, parameters, synID, rqid);
        this.expectedWorkers = nOfP <= 0 ? 1 : nOfP;
    }

    @Override
    public boolean add(Estimation e) {
        try {
            JsonNode payload = parsePayload(e.getEstimation());

            if (payload == null || payload.isNull()) {
                System.out.println("[OnePassPhase2Reduce] Ignoring null payload.");
                return false;
            }

            String type = textField(payload, "type", "");

            if (!"LOCAL_PHASE2_ROOT_SUMMARY".equals(type)) {
                System.out.println("[OnePassPhase2Reduce] Ignoring unexpected payload type: " + type);
                return false;
            }

            uid = intField(payload, "uid", e.getUID());
            phase = textField(payload, "phase", "PHASE2");
            resultId = textField(payload, "resultId", "PHASE2_RESULT_" + uid);
            queryName = textField(payload, "queryName", "");
            rootAlias = textField(payload, "rootAlias", "");
            datasetSeed = textField(payload, "datasetSeed", datasetSeed);
            sampleSize = intField(payload, "sampleSize", sampleSize);

            String payloadBaseKey = textField(payload, "baseKey", "");

            if (payloadBaseKey != null && !payloadBaseKey.trim().isEmpty()) {
                if (baseKey == null || baseKey.trim().isEmpty()) {
                    baseKey = payloadBaseKey.trim();
                }
            }

            int workerId = intField(payload, "workerId", -1);
            int payloadExpectedWorkers = intField(payload, "expectedWorkers", e.getNoOfP());

            if (payloadExpectedWorkers > 0) {
                expectedWorkers = Math.max(expectedWorkers, payloadExpectedWorkers);
            }

            if (workerId < 0) {
                System.out.println("[OnePassPhase2Reduce] Ignoring result with invalid workerId: " + payload);
                return false;
            }

            if (!receivedWorkers.add(workerId)) {
                System.out.println("[OnePassPhase2Reduce] Duplicate worker ignored: workerId=" + workerId
                        + ", resultId=" + resultId);
                return false;
            }

            rootTuplesSeen += longField(payload, "rootTuplesSeen", 0L);
            positiveRootCandidatesSeen += longField(payload, "positiveRootCandidatesSeen", 0L);
            totalRootGroupWeight += doubleField(payload, "totalRootGroupWeight", 0.0d);

            JsonNode reservoir = payload.get("orderedReservoir");

            if (reservoir != null && reservoir.isArray()) {
                for (JsonNode entry : reservoir) {
                    candidates.add(ReservoirCandidate.fromJson(workerId, entry));
                }
            }

            count = receivedWorkers.size();

            System.out.println("[OnePassPhase2Reduce] Received worker "
                    + workerId + " for " + resultId
                    + " (" + count + "/" + expectedWorkers + ")"
                    + ", localReservoir=" + (reservoir == null || !reservoir.isArray() ? 0 : reservoir.size())
                    + ", totalRootGroupWeightSoFar=" + totalRootGroupWeight);

            return count >= expectedWorkers;

        } catch (Exception ex) {
            throw new IllegalStateException("Could not add LOCAL_PHASE2_ROOT_SUMMARY to reducer.", ex);
        }
    }

    @Override
    public Object reduce() {
        try {
            List<Integer> workers = new ArrayList<Integer>(receivedWorkers);
            Collections.sort(workers);

            List<ReservoirCandidate> globalReservoir = buildGlobalReservoir();
            List<ReservoirCandidate> samples = buildMultinomialSamples(globalReservoir);

            ObjectNode payload = MAPPER.createObjectNode();

            payload.put("type", "GLOBAL_PHASE2_ROOT_SAMPLE");
            payload.put("uid", uid);
            payload.put("phase", phase);
            payload.put("resultId", resultId);
            payload.put("queryName", queryName);
            payload.put("rootAlias", rootAlias);
            payload.put("baseKey", baseKey);
            payload.put("stateRef", uid + "_PHASE2_" + resultId + "_GLOBAL_ROOT_SAMPLE");
            payload.put("datasetSeed", datasetSeed);
            payload.put("sampleSize", sampleSize);
            payload.put("expectedWorkers", expectedWorkers);
            payload.put("localResultCount", receivedWorkers.size());
            payload.set("receivedWorkers", MAPPER.valueToTree(workers));

            payload.put("rootTuplesSeen", rootTuplesSeen);
            payload.put("positiveRootCandidatesSeen", positiveRootCandidatesSeen);
            payload.put("totalRootGroupWeight", totalRootGroupWeight);
            payload.put("globalReservoirSize", globalReservoir.size());
            payload.put("sampleInstanceCount", samples.size());

            ArrayNode reservoirNode = MAPPER.createArrayNode();

            for (ReservoirCandidate candidate : globalReservoir) {
                reservoirNode.add(candidate.toJson());
            }

            payload.set("globalReservoir", reservoirNode);

            ArrayNode sampleInstances = MAPPER.createArrayNode();

            for (int i = 0; i < samples.size(); i++) {
                ReservoirCandidate candidate = samples.get(i);
                ObjectNode sample = candidate.toJson();
                sample.put("sampleInstanceId", i);
                sample.put("sourceCandidateId", candidate.globalCandidateId());
                sampleInstances.add(sample);
            }

            payload.set("sampleInstances", sampleInstances);

            return MAPPER.writeValueAsString(payload);

        } catch (Exception ex) {
            throw new IllegalStateException("Could not serialize GLOBAL_PHASE2_ROOT_SAMPLE.", ex);
        }
    }

    private List<ReservoirCandidate> buildGlobalReservoir() {
        List<ReservoirCandidate> sorted = new ArrayList<ReservoirCandidate>(candidates);
        Collections.sort(sorted, bestCandidateFirst());

        if (sorted.size() <= sampleSize) {
            return sorted;
        }

        return new ArrayList<ReservoirCandidate>(sorted.subList(0, sampleSize));
    }

    private List<ReservoirCandidate> buildMultinomialSamples(List<ReservoirCandidate> globalReservoir) {
        List<ReservoirCandidate> output = new ArrayList<ReservoirCandidate>();

        if (globalReservoir.isEmpty()) {
            return output;
        }

        Random outputRandom = new Random(stableSeed(datasetSeed) + 31L);

        List<ReservoirCandidate> introduced = new ArrayList<ReservoirCandidate>();
        double introducedWeight = 0.0d;
        int nextReservoirIndex = 0;

        for (int sampleIndex = 0; sampleIndex < sampleSize; sampleIndex++) {
            double u = nextPositiveDouble(outputRandom) * totalRootGroupWeight;

            boolean repeatPrevious = introducedWeight > 0.0d && u < introducedWeight;
            boolean noMoreReservoirEntries = nextReservoirIndex >= globalReservoir.size();

            ReservoirCandidate selected;

            if (repeatPrevious || noMoreReservoirEntries) {
                selected = drawFromIntroduced(introduced, introducedWeight, outputRandom);
            } else {
                selected = globalReservoir.get(nextReservoirIndex);
                nextReservoirIndex++;
                introduced.add(selected);
                introducedWeight += selected.rootGroupWeight;
            }

            output.add(selected);
        }

        return output;
    }

    private static ReservoirCandidate drawFromIntroduced(
            List<ReservoirCandidate> introduced,
            double introducedWeight,
            Random outputRandom) {

        if (introduced.isEmpty()) {
            throw new IllegalStateException("Cannot draw from an empty introduced set");
        }

        double u = nextPositiveDouble(outputRandom) * introducedWeight;
        double cumulative = 0.0d;

        for (ReservoirCandidate candidate : introduced) {
            cumulative += candidate.rootGroupWeight;

            if (u < cumulative) {
                return candidate;
            }
        }

        return introduced.get(introduced.size() - 1);
    }

    private static Comparator<ReservoirCandidate> bestCandidateFirst() {
        return new Comparator<ReservoirCandidate>() {
            @Override
            public int compare(ReservoirCandidate left, ReservoirCandidate right) {
                int byKey = Double.compare(left.esKey, right.esKey);

                if (byKey != 0) {
                    return byKey;
                }

                int byWorker = Integer.compare(left.workerId, right.workerId);

                if (byWorker != 0) {
                    return byWorker;
                }

                return Long.compare(left.localCandidateId, right.localCandidateId);
            }
        };
    }

    private static JsonNode parsePayload(Object estimation) throws Exception {
        if (estimation == null) {
            return null;
        }

        if (estimation instanceof JsonNode) {
            return (JsonNode) estimation;
        }

        if (estimation instanceof String) {
            String s = ((String) estimation).trim();

            if (s.isEmpty()) {
                return null;
            }

            return MAPPER.readTree(s);
        }

        return MAPPER.valueToTree(estimation);
    }

    private static String textField(JsonNode node, String fieldName, String defaultValue) {
        if (node == null || node.isNull()) {
            return defaultValue;
        }

        JsonNode field = node.get(fieldName);

        if (field == null || field.isNull()) {
            return defaultValue;
        }

        String value = field.asText();

        if (value == null || value.trim().isEmpty()) {
            return defaultValue;
        }

        return value.trim();
    }

    private static int intField(JsonNode node, String fieldName, int defaultValue) {
        if (node == null || node.isNull()) {
            return defaultValue;
        }

        JsonNode field = node.get(fieldName);

        if (field == null || field.isNull()) {
            return defaultValue;
        }

        return field.asInt(defaultValue);
    }

    private static long longField(JsonNode node, String fieldName, long defaultValue) {
        if (node == null || node.isNull()) {
            return defaultValue;
        }

        JsonNode field = node.get(fieldName);

        if (field == null || field.isNull()) {
            return defaultValue;
        }

        return field.asLong(defaultValue);
    }

    private static double doubleField(JsonNode node, String fieldName, double defaultValue) {
        if (node == null || node.isNull()) {
            return defaultValue;
        }

        JsonNode field = node.get(fieldName);

        if (field == null || field.isNull()) {
            return defaultValue;
        }

        return field.asDouble(defaultValue);
    }

    private static long stableSeed(String seed) {
        String s = seed == null ? "" : seed;
        long h = 1125899906842597L;

        for (int i = 0; i < s.length(); i++) {
            h = 31L * h + s.charAt(i);
        }

        return h;
    }

    private static double nextPositiveDouble(Random random) {
        double u = random.nextDouble();

        while (u <= 0.0d) {
            u = random.nextDouble();
        }

        return u;
    }

    private static final class ReservoirCandidate {
        private final int workerId;
        private final long localCandidateId;
        private final String rootAlias;
        private final JsonNode rootTuple;
        private final double rootGroupWeight;
        private final double esKey;
        private final long localArrivalOrder;

        private ReservoirCandidate(
                int workerId,
                long localCandidateId,
                String rootAlias,
                JsonNode rootTuple,
                double rootGroupWeight,
                double esKey,
                long localArrivalOrder) {

            this.workerId = workerId;
            this.localCandidateId = localCandidateId;
            this.rootAlias = rootAlias;
            this.rootTuple = rootTuple == null ? MAPPER.createObjectNode() : rootTuple.deepCopy();
            this.rootGroupWeight = rootGroupWeight;
            this.esKey = esKey;
            this.localArrivalOrder = localArrivalOrder;
        }

        private static ReservoirCandidate fromJson(int workerId, JsonNode entry) {
            return new ReservoirCandidate(
                    workerId,
                    longField(entry, "candidateId", -1L),
                    textField(entry, "rootAlias", ""),
                    entry.get("rootTuple"),
                    doubleField(entry, "rootGroupWeight", 0.0d),
                    doubleField(entry, "esKey", Double.POSITIVE_INFINITY),
                    longField(entry, "arrivalOrder", 0L)
            );
        }

        private String globalCandidateId() {
            return workerId + ":" + localCandidateId;
        }

        private ObjectNode toJson() {
            ObjectNode node = MAPPER.createObjectNode();
            node.put("workerId", workerId);
            node.put("candidateId", localCandidateId);
            node.put("globalCandidateId", globalCandidateId());
            node.put("rootAlias", rootAlias);
            node.set("rootTuple", rootTuple.deepCopy());
            node.put("rootGroupWeight", rootGroupWeight);
            node.put("esKey", esKey);
            node.put("arrivalOrder", localArrivalOrder);
            return node;
        }
    }
}
