package infore.SDE.transformations.onepass.coordinator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import infore.SDE.messages.Estimation;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * First version of the One-pass* Flink coordinator.
 *
 * V0 responsibility:
 *   - receive DATA_BARRIER_ACK messages from SDE workers
 *   - group them by uid / phase / alias / barrierId
 *   - deduplicate by workerId
 *   - wait until expectedWorkers have acknowledged
 *   - emit one GLOBAL_BARRIER_READY message
 *
 * This does NOT merge Phase 1/2/3 algorithm state yet.
 * That will be added after the coordination protocol works.
 */
public final class OnePassCoordinatorOperator extends RichFlatMapFunction<Estimation, Estimation> {

    private static final long serialVersionUID = 1L;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /*
     * Current worker-side SDEcoFlatMap emits One-pass data barriers as:
     * requestID = 70
     * synopsisID = 30
     */
    private static final int ONEPASS_DATA_BARRIER_REQUEST_ID = 70;
    private static final int ONEPASS_COORDINATOR_REQUEST_ID = 71;
    private static final int ONEPASS_LOCAL_PHASE1_RESULT_REQUEST_ID = 72;
    private static final int ONEPASS_GLOBAL_PHASE1_RESULT_READY_REQUEST_ID = 73;
    private static final int ONEPASS_SYNOPSIS_ID = 30;

    private static final String TYPE_DATA_BARRIER_ACK = "DATA_BARRIER_ACK";
    private static final String TYPE_GLOBAL_BARRIER_READY = "GLOBAL_BARRIER_READY";

    private static final String TYPE_LOCAL_PHASE1_RESULT = "LOCAL_PHASE1_RESULT";
    private static final String TYPE_GLOBAL_PHASE1_RESULT_READY = "GLOBAL_PHASE1_RESULT_READY";

    private final Map<BarrierKey, BarrierAccumulator> barriers = new HashMap<BarrierKey, BarrierAccumulator>();
    private final Map<ResultKey, ResultAccumulator> phaseOneResults = new HashMap<ResultKey, ResultAccumulator>();

    private final Set<ResultKey> completedPhaseOneResults = new HashSet<ResultKey>();
    /*
     * Keeps completed barriers so that late duplicate ACKs do not emit
     * GLOBAL_BARRIER_READY twice.
     */
    private final Set<BarrierKey> completedBarriers = new HashSet<BarrierKey>();

    private final boolean DEBUG_PRINT = true;

    @Override
    public void flatMap(Estimation input, Collector<Estimation> out) throws Exception {
        if (input == null) {
            return;
        }

        if (input.getSynopsisID() != ONEPASS_SYNOPSIS_ID) {
            return;
        }

        JsonNode payload = parsePayload(input.getEstimation());

        if (payload == null || payload.isNull()) {
            return;
        }

        String type = textField(payload, "type", "");

        if (input.getRequestID() == ONEPASS_DATA_BARRIER_REQUEST_ID
                && TYPE_DATA_BARRIER_ACK.equals(type)) {
            handleDataBarrierAck(input, payload, out);
            return;
        }

        if (input.getRequestID() == ONEPASS_LOCAL_PHASE1_RESULT_REQUEST_ID
                && TYPE_LOCAL_PHASE1_RESULT.equals(type)) {
            handleLocalPhaseOneResult(input, payload, out);
        }
    }

    private void handleLocalPhaseOneResult(Estimation input, JsonNode payload, Collector<Estimation> out) throws Exception {

        int uid = intField(payload, "uid", input.getUID());
        String phase = textField(payload, "phase", "PHASE1");
        String resultId = textField(payload, "resultId", "PHASE1_RESULT_" + uid);

        int workerId = intField(payload, "workerId", -1);
        int expectedWorkers = intField(payload, "expectedWorkers", input.getNoOfP());
        int actualParallelism = intField(payload, "actualParallelism", expectedWorkers);

        String queryName = textField(payload, "queryName", "");
        String rootAlias = textField(payload, "rootAlias", "");

        if (workerId < 0) {
            System.out.println("[OnePassCoordinator] Ignoring LOCAL_PHASE1_RESULT with invalid workerId: "
                    + payload.toString());
            return;
        }

        if (expectedWorkers <= 0) {
            expectedWorkers = 1;
        }

        ResultKey key = new ResultKey(uid, resultId);

        if (completedPhaseOneResults.contains(key)) {
            System.out.println("[OnePassCoordinator] Ignoring late duplicate LOCAL_PHASE1_RESULT for completed result: "
                    + key + ", workerId=" + workerId);
            return;
        }

        ResultAccumulator acc = phaseOneResults.get(key);

        if (acc == null) {
            acc = new ResultAccumulator(uid, phase, resultId, expectedWorkers, actualParallelism,
                    input.getKey(), queryName, rootAlias);

            phaseOneResults.put(key, acc);
        }

        acc.expectedWorkers = Math.max(acc.expectedWorkers, expectedWorkers);
        acc.actualParallelism = Math.max(acc.actualParallelism, actualParallelism);

        boolean isNewWorker = acc.receivedWorkers.add(workerId);

        if (!isNewWorker) {
            System.out.println("[OnePassCoordinator] Duplicate LOCAL_PHASE1_RESULT ignored: "
                    + key + ", workerId=" + workerId);
            return;
        }

        JsonNode phaseOneResult = payload.get("phaseOneResult");

        if (phaseOneResult != null && !phaseOneResult.isNull()) {
            acc.localPhaseOneResultsByWorker.put(workerId, phaseOneResult.toString());

            if(DEBUG_PRINT){
                JsonNode seenTuplesByAlias = phaseOneResult.get("seenTuplesByAlias");
                if (seenTuplesByAlias != null && !seenTuplesByAlias.isNull()) {
                    System.out.println("[OnePassCoordinator] Worker "
                            + workerId + " Phase 1 seenTuplesByAlias = " + seenTuplesByAlias.toString());
                }
            }

        }

        System.out.println("[OnePassCoordinator] LOCAL_PHASE1_RESULT received: " + key + ", workerId=" + workerId +
                ", received=" + acc.receivedWorkers.size() + "/" + acc.expectedWorkers);

        if (acc.isComplete()) {
            Estimation ready = buildGlobalPhaseOneResultReady(acc);

            out.collect(ready);

            phaseOneResults.remove(key);
            completedPhaseOneResults.add(key);

            System.out.println("[OnePassCoordinator] GLOBAL_PHASE1_RESULT_READY emitted: "+ key
                    + ", receivedWorkers=" + acc.receivedWorkers);
        }
    }

    private Estimation buildGlobalPhaseOneResultReady(ResultAccumulator acc) throws Exception {
        Map<String, Object> payload = new LinkedHashMap<String, Object>();

        payload.put("type", TYPE_GLOBAL_PHASE1_RESULT_READY);
        payload.put("uid", acc.uid);
        payload.put("phase", acc.phase);
        payload.put("resultId", acc.resultId);
        payload.put("queryName", acc.queryName);
        payload.put("rootAlias", acc.rootAlias);
        payload.put("expectedWorkers", acc.expectedWorkers);
        payload.put("actualParallelism", acc.actualParallelism);
        payload.put("receivedWorkers", acc.receivedWorkers);
        payload.put("localResultCount", acc.localPhaseOneResultsByWorker.size());

        String json = MAPPER.writeValueAsString(payload);

        String[] param = new String[] {TYPE_GLOBAL_PHASE1_RESULT_READY, acc.resultId, acc.phase, "",
                        Integer.toString(acc.receivedWorkers.size()), Integer.toString(acc.expectedWorkers)
                };

        String estimationKey = acc.uid + "_" + acc.resultId + "_GLOBAL_PHASE1_READY";

        return new Estimation(
                acc.uid,
                estimationKey,
                ONEPASS_GLOBAL_PHASE1_RESULT_READY_REQUEST_ID,
                ONEPASS_SYNOPSIS_ID,
                acc.datasetKey,
                json,
                param,
                acc.expectedWorkers
        );
    }

    private void handleDataBarrierAck(Estimation input, JsonNode payload, Collector<Estimation> out) throws Exception {

        int uid = intField(payload, "uid", input.getUID());
        String phase = textField(payload, "phase", "UNKNOWN");
        String alias = textField(payload, "alias", "");
        String barrierId = textField(payload, "barrierId", "unknown");

        int workerId = intField(payload, "workerId", -1);
        int expectedWorkers = intField(payload, "expectedWorkers", input.getNoOfP());
        int actualParallelism = intField(payload, "actualParallelism", expectedWorkers);
        boolean foundOnePassSynopsis = booleanField(payload, "foundOnePassSynopsis", false);

        if (workerId < 0) {
            System.out.println("[OnePassCoordinator] Ignoring ACK with invalid workerId: " + payload.toString());
            return;
        }

        if (expectedWorkers <= 0) {
            expectedWorkers = 1;
        }

        BarrierKey key = new BarrierKey(uid, phase, alias, barrierId);

        if (completedBarriers.contains(key)) {
            System.out.println("[OnePassCoordinator] Ignoring late duplicate ACK for completed barrier: "
                    + key + ", workerId=" + workerId);
            return;
        }

        BarrierAccumulator acc = barriers.get(key);

        if (acc == null) {
            acc = new BarrierAccumulator(uid, phase, alias, barrierId, expectedWorkers, actualParallelism,
                    input.getKey());

            barriers.put(key, acc);
        }

        /*
         * If another worker reports a different expectedWorkers value,
         * keep the maximum. This avoids completing too early.
         */
        acc.expectedWorkers = Math.max(acc.expectedWorkers, expectedWorkers);
        acc.actualParallelism = Math.max(acc.actualParallelism, actualParallelism);

        boolean isNewWorker = acc.receivedWorkers.add(workerId);

        if (!isNewWorker) {
            System.out.println("[OnePassCoordinator] Duplicate ACK ignored: " + key + ", workerId=" + workerId);
            return;
        }

        if (!foundOnePassSynopsis) {
            acc.missingSynopsisWorkers.add(workerId);
        }

        System.out.println("[OnePassCoordinator] ACK received: " + key + ", workerId=" + workerId + ", received=" +
                acc.receivedWorkers.size() + "/" + acc.expectedWorkers);

        if (acc.isComplete()) {
            Estimation ready = buildGlobalBarrierReady(acc);

            out.collect(ready);

            barriers.remove(key);
            completedBarriers.add(key);

            System.out.println("[OnePassCoordinator] GLOBAL_BARRIER_READY emitted: " + key +
                    ", receivedWorkers=" + acc.receivedWorkers);
        }
    }

    private Estimation buildGlobalBarrierReady(BarrierAccumulator acc) throws Exception {
        Map<String, Object> payload = new LinkedHashMap<String, Object>();

        payload.put("type", TYPE_GLOBAL_BARRIER_READY);
        payload.put("uid", acc.uid);
        payload.put("phase", acc.phase);
        payload.put("alias", acc.alias);
        payload.put("barrierId", acc.barrierId);
        payload.put("expectedWorkers", acc.expectedWorkers);
        payload.put("actualParallelism", acc.actualParallelism);
        payload.put("receivedWorkers", acc.receivedWorkers);
        payload.put("missingSynopsisWorkers", acc.missingSynopsisWorkers);

        String json = MAPPER.writeValueAsString(payload);

        String[] param = new String[] {
                TYPE_GLOBAL_BARRIER_READY,
                acc.barrierId,
                acc.phase,
                acc.alias,
                Integer.toString(acc.receivedWorkers.size()),
                Integer.toString(acc.expectedWorkers)
        };

        String estimationKey = acc.uid + "_" + acc.phase + "_" + normalizeAlias(acc.alias)
                        + "_" + acc.barrierId + "_GLOBAL_READY";

        return new Estimation(
                acc.uid,
                estimationKey,
                ONEPASS_COORDINATOR_REQUEST_ID,
                ONEPASS_SYNOPSIS_ID,
                acc.datasetKey,
                json,
                param,
                acc.expectedWorkers
        );
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

        /*
         * In case Kafka/Jackson deserializes the estimation payload as a Map,
         * convert it back to a JsonNode.
         */
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

    private static boolean booleanField(JsonNode node, String fieldName, boolean defaultValue) {
        if (node == null || node.isNull()) {
            return defaultValue;
        }

        JsonNode field = node.get(fieldName);

        if (field == null || field.isNull()) {
            return defaultValue;
        }

        return field.asBoolean(defaultValue);
    }

    private static String normalizeAlias(String alias) {
        if (alias == null || alias.trim().isEmpty()) {
            return "none";
        }

        return alias.trim();
    }

    private static final class BarrierKey {
        private final int uid;
        private final String phase;
        private final String alias;
        private final String barrierId;

        private BarrierKey(int uid, String phase, String alias, String barrierId) {
            this.uid = uid;
            this.phase = phase == null ? "UNKNOWN" : phase;
            this.alias = alias == null ? "" : alias;
            this.barrierId = barrierId == null ? "unknown" : barrierId;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof BarrierKey)) {
                return false;
            }

            BarrierKey other = (BarrierKey) o;

            return uid == other.uid
                    && Objects.equals(phase, other.phase)
                    && Objects.equals(alias, other.alias)
                    && Objects.equals(barrierId, other.barrierId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(uid, phase, alias, barrierId);
        }

        @Override
        public String toString() {
            return "BarrierKey{" + "uid=" + uid + ", phase='" + phase + '\'' + ", alias='" + alias + '\''
                    + ", barrierId='" + barrierId + '\'' + '}';
        }
    }

    private static final class BarrierAccumulator {
        private final int uid;
        private final String phase;
        private final String alias;
        private final String barrierId;
        private final String datasetKey;

        private int expectedWorkers;
        private int actualParallelism;

        private final Set<Integer> receivedWorkers = new HashSet<Integer>();
        private final Set<Integer> missingSynopsisWorkers = new HashSet<Integer>();

        private BarrierAccumulator(int uid, String phase, String alias, String barrierId, int expectedWorkers,
                                   int actualParallelism, String datasetKey) {

            this.uid = uid;
            this.phase = phase;
            this.alias = alias;
            this.barrierId = barrierId;
            this.expectedWorkers = expectedWorkers;
            this.actualParallelism = actualParallelism;
            this.datasetKey = datasetKey;
        }

        private boolean isComplete() {
            return receivedWorkers.size() >= expectedWorkers;
        }
    }

    private static final class ResultKey {
        private final int uid;
        private final String resultId;

        private ResultKey(int uid, String resultId) {
            this.uid = uid;
            this.resultId = resultId == null ? "unknown" : resultId;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof ResultKey)) {
                return false;
            }

            ResultKey other = (ResultKey) o;
            return uid == other.uid && Objects.equals(resultId, other.resultId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(uid, resultId);
        }

        @Override
        public String toString() {
            return "ResultKey{" + "uid=" + uid + ", resultId='" + resultId + '\'' + '}';
        }
    }

    private static final class ResultAccumulator {
        private final int uid;
        private final String phase;
        private final String resultId;
        private final String datasetKey;
        private final String queryName;
        private final String rootAlias;

        private int expectedWorkers;
        private int actualParallelism;

        private final Set<Integer> receivedWorkers = new HashSet<Integer>();

        private final Map<Integer, String> localPhaseOneResultsByWorker = new LinkedHashMap<Integer, String>();

        private ResultAccumulator(int uid, String phase, String resultId, int expectedWorkers,
                int actualParallelism, String datasetKey, String queryName, String rootAlias) {

            this.uid = uid;
            this.phase = phase;
            this.resultId = resultId;
            this.expectedWorkers = expectedWorkers;
            this.actualParallelism = actualParallelism;
            this.datasetKey = datasetKey;
            this.queryName = queryName;
            this.rootAlias = rootAlias;
        }

        private boolean isComplete() {
            return receivedWorkers.size() >= expectedWorkers;
        }
    }
}