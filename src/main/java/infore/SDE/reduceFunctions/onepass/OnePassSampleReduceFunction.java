package infore.SDE.reduceFunctions.onepass;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import infore.SDE.messages.Estimation;
import infore.SDE.reduceFunctions.ReduceFunction;

import java.util.*;

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
    /*
     * Bounded global ES reservoir.
     * The worst globally retained candidate is at the head.
     * Memory: O(K)
     */
    private final PriorityQueue<ReservoirCandidate> globalReservoir =
            new PriorityQueue<ReservoirCandidate>(11, worstCandidateFirst());

    private boolean sampleSizeInitialized = false;

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
            int incomingSampleSize = intField(payload, "sampleSize", -1);
            if (incomingSampleSize <= 0) {
                throw new IllegalStateException("LOCAL_PHASE2_ROOT_SUMMARY has invalid sampleSize=" + incomingSampleSize);
            }

            if (!sampleSizeInitialized) {
                sampleSize = incomingSampleSize;
                sampleSizeInitialized = true;

            } else if (sampleSize != incomingSampleSize) {
                throw new IllegalStateException("Conflicting Phase-2 sample sizes." + " expected=" + sampleSize +
                        ", received=" + incomingSampleSize);
            }

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
                    ReservoirCandidate candidate = ReservoirCandidate.fromJson(workerId, entry);
                    offerGlobalCandidate(candidate);
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
        List<ReservoirCandidate> sorted = new ArrayList<ReservoirCandidate>(globalReservoir);
        sorted.sort(bestCandidateFirst());

        return sorted;
    }

    private List<ReservoirCandidate> buildMultinomialSamples(List<ReservoirCandidate> globalReservoir) {
        List<ReservoirCandidate> output = new ArrayList<ReservoirCandidate>(sampleSize);

        if (globalReservoir.isEmpty()) {
            return output;
        }

        Random outputRandom = new Random(stableSeed(datasetSeed) + 31L);
        List<ReservoirCandidate> introduced = new ArrayList<ReservoirCandidate>(globalReservoir.size());


        /*
         * cumulativeWeights[i] is the sum of introduced candidate weights
         * from 0 through i.
         * Primitive array avoids Double boxing.
         */
        double[] cumulativeWeights = new double[globalReservoir.size()];
        double introducedWeight = 0.0d;
        int nextReservoirIndex = 0;

        for (int sampleIndex = 0; sampleIndex < sampleSize; sampleIndex++) {
            double u = nextPositiveDouble(outputRandom) * totalRootGroupWeight;

            boolean repeatPrevious = introducedWeight > 0.0d && u < introducedWeight;
            boolean noMoreReservoirEntries = nextReservoirIndex >= globalReservoir.size();

            ReservoirCandidate selected;
            if (repeatPrevious || noMoreReservoirEntries) {
                selected = drawFromIntroducedBinary(introduced, cumulativeWeights, introducedWeight, outputRandom);
            } else {
                selected = globalReservoir.get(nextReservoirIndex);
                nextReservoirIndex++;

                introduced.add(selected);
                introducedWeight += selected.rootGroupWeight;

                cumulativeWeights[introduced.size() - 1] = introducedWeight;
            }
            output.add(selected);
        }

        return output;
    }

    private static ReservoirCandidate drawFromIntroducedBinary(List<ReservoirCandidate> introduced,
                                                               double[] cumulativeWeights, double introducedWeight,
                                                               Random outputRandom) {

        if (introduced == null || introduced.isEmpty()) {
            throw new IllegalStateException("Cannot draw from an empty introduced set");
        }

        double u = nextPositiveDouble(outputRandom) * introducedWeight;
        int low = 0;
        int high = introduced.size() - 1;

        /*
         * Find the first cumulative weight strictly greater than u.
         * This is equivalent to the existing:
         *     if (u < cumulative)
         * linear scan.
         */
        while (low < high) {
            int mid = (low + high) >>> 1;
            if (u < cumulativeWeights[mid]) {
                high = mid;
            } else {
                low = mid + 1;
            }
        }

        return introduced.get(low);
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

    private void offerGlobalCandidate(ReservoirCandidate candidate) {
        if (candidate == null) {
            return;
        }

        if (globalReservoir.size() < sampleSize) {
            globalReservoir.add(candidate);
            return;
        }
        ReservoirCandidate currentWorst = globalReservoir.peek();

        if (currentWorst == null) {
            globalReservoir.add(candidate);
            return;
        }

        /*
         * bestCandidateFirst:
         * negative => candidate is globally better
         */
        if (bestCandidateFirst().compare(candidate, currentWorst) < 0) {
            globalReservoir.poll();
            globalReservoir.add(candidate);
        }
    }

    private static Comparator<ReservoirCandidate>
    worstCandidateFirst() {
        return new Comparator<ReservoirCandidate>() {
            @Override
            public int compare(ReservoirCandidate left, ReservoirCandidate right) {
                /*
                 * Exact reverse of the deterministic global ordering.
                 * The head of the PriorityQueue is therefore the candidate
                 * that should be discarded first.
                 */
                return bestCandidateFirst().compare(right, left);
            }
        };
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
