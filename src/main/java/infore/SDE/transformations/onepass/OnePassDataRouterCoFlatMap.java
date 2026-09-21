package infore.SDE.transformations.onepass;

import com.fasterxml.jackson.databind.JsonNode;
import infore.SDE.messages.Datapoint;
import infore.SDE.messages.Request;
import infore.SDE.synopses.OnePassSampler.OnePassTuple;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * OnePass-specific deterministic data router.
 *
 * There is deliberately no round-robin mode.
 *
 * SHARDED:
 *   Phase 1 -> first child-index owner / final parent-index owner.
 *   Phase 2 -> first root child-index owner.
 *   Phase 3 -> first child-index owner, then worker-to-worker enrichment.
 *
 * REPLICATED:
 *   Phase 1 -> keep the same deterministic Phase-1 hash for data distribution,
 *              but all continuation computation remains on that one worker.
 *   Phase 2 -> keep the same deterministic root hash, but all continuation
 *              computation remains on that one worker.
 *   Phase 3 -> route directly to the parent-edge selection owner. All
 *              continuation indexes are replicated and therefore local.
 */
public final class OnePassDataRouterCoFlatMap
        extends RichCoFlatMapFunction<Datapoint, Request, Datapoint> {

    private static final long serialVersionUID = 1L;

    private static final String ONEPASS_DATA_BARRIER_FIELD = "__onePassDataBarrier";
    private static final String ONEPASS_END_ALIAS_TYPE = "END_ALIAS";
    private static final String START_PHASE_2 = "START_PHASE_2";
    private static final String START_PHASE_3_ALIAS = "START_PHASE_3_ALIAS";
    private static final String SHARDED_PHASE3_PROTOCOL = "SHARDED_PHASE3_V1";
    private static final int ONEPASS_SYNOPSIS_ID = 30;

    private final Map<String, Integer> parallelismByBaseKey = new HashMap<String, Integer>();
    private final Map<String, CompiledOnePassPlan> planByBaseKey = new HashMap<String, CompiledOnePassPlan>();
    private final Map<String, OnePassExecutionMode> executionModeByBaseKey = new HashMap<String, OnePassExecutionMode>();
    private final Set<String> phaseOneHashRoutingActive = new HashSet<String>();
    private final Set<String> phaseThreeRoutingReady = new HashSet<String>();
    private final Set<String> phaseThreeRoutingActive = new HashSet<String>();

    @Override
    public void flatMap1(Datapoint value, Collector<Datapoint> out) throws Exception {
        if (value == null) {
            return;
        }

        String baseKey = value.getDataSetkey();
        Integer p = parallelismByBaseKey.get(baseKey);

        if (p == null || p <= 1) {
            out.collect(value);
            return;
        }

        if (isOnePassDataBarrier(value) || isOnePassEndAlias(value)) {
            broadcastToWorkers(value, baseKey, p, out);
            return;
        }

        CompiledOnePassPlan plan = planByBaseKey.get(baseKey);
        if (plan == null) {
            throw new IllegalStateException("OnePass routing has no compiled plan. baseKey=" + baseKey);
        }

        OnePassExecutionMode executionMode = executionModeByBaseKey.getOrDefault(baseKey, OnePassExecutionMode.SHARDED);

        OnePassTuple tuple = OnePassTupleExtractor.extract(value.getValues());
        String alias = tuple.getTable();

        if (!plan.containsAlias(alias)) {
            throw new IllegalStateException("Tuple alias '" + alias + "' is not present in the OnePass plan. baseKey=" + baseKey);
        }

        if (plan.isRoot(alias)) {
            int worker = OnePassShardOwnership.ownerForPhaseTwoRootTuple(tuple, plan, p);
            out.collect(copyWithKey(value, OnePassShardOwnership.workerKey(baseKey, p, worker)));
            return;
        }

        /*
         * Phase 1 keeps the current hash in BOTH architectures.
         * SHARDED:
         *   the hash also identifies the first continuation owner.
         * REPLICATED:
         *   the hash merely picks one worker for this tuple. That worker
         *   performs ALL continuation lookups locally.
         */
        if (phaseOneHashRoutingActive.contains(baseKey)) {
            int worker = OnePassShardOwnership.ownerForPhaseOneInputTuple(tuple, plan, p);
            out.collect(copyWithKey(value, OnePassShardOwnership.workerKey(baseKey, p, worker)));
            return;
        }

        if (phaseThreeRoutingReady.contains(baseKey) || phaseThreeRoutingActive.contains(baseKey)) {
            int worker;
            if (executionMode == OnePassExecutionMode.REPLICATED) {
                //Go directly to the worker that owns the final weighted selection for this parent-edge key.
                worker = OnePassShardOwnership.ownerForPhaseThreeSelectionTuple(tuple, plan, p);

            } else {

                //Existing architecture, unchanged: start on child-edge-0 owner and move work between owners.
                worker = OnePassShardOwnership.ownerForPhaseThreeInputTuple(tuple, plan, p);
            }

            out.collect(copyWithKey(value, OnePassShardOwnership.workerKey(baseKey, p, worker)));
            return;
        }

        throw new IllegalStateException("OnePass tuple arrived while no routing phase was active." +
                " baseKey=" + baseKey + ", alias=" + alias + ", mode=" + executionMode);
    }

    @Override
    public void flatMap2(Request request, Collector<Datapoint> out) throws Exception {
        if (request == null || request.getSynopsisID() != ONEPASS_SYNOPSIS_ID) {
            return;
        }

        String baseKey = request.getKey();
        if (request.getRequestID() == 1) {
            int p = request.getNoOfP();
            if (p <= 0) {
                throw new IllegalArgumentException("OnePass ADD requires noOfP > 0." + " baseKey=" + baseKey + ", noOfP=" + p);
            }

            CompiledOnePassPlan plan = CompiledOnePassPlan.from(OnePassRequestParser.parse(request));
            OnePassExecutionMode executionMode = OnePassExecutionMode.fromRequest(request);

            parallelismByBaseKey.put(baseKey, p);
            planByBaseKey.put(baseKey, plan);
            executionModeByBaseKey.put(baseKey, executionMode);
            phaseOneHashRoutingActive.add(baseKey);
            phaseThreeRoutingReady.remove(baseKey);
            phaseThreeRoutingActive.remove(baseKey);

            System.out.println("[OnePassDataRouter] Registered baseKey=" + baseKey + ", parallelism=" + p +
                    ", executionMode=" + executionMode + ", phase1HashFields=" + describePhaseOneHashFields(plan));

            return;
        }

        if (request.getRequestID() == 7) {
            JsonNode payload = request.getParameters();
            String type = textField(payload, "type", "");
            String protocol = textField(payload, "protocol", "");

            if (START_PHASE_2.equals(type)) {
                phaseOneHashRoutingActive.remove(baseKey);
                phaseThreeRoutingReady.add(baseKey);
                System.out.println("[OnePassDataRouter] Phase 1 routing complete." + " baseKey=" + baseKey);
            }

            if (START_PHASE_3_ALIAS.equals(type) && SHARDED_PHASE3_PROTOCOL.equals(protocol)) {

                phaseOneHashRoutingActive.remove(baseKey);
                phaseThreeRoutingActive.add(baseKey);

                System.out.println("[OnePassDataRouter] Phase 3 routing active." + " baseKey=" + baseKey +
                        ", executionMode=" + executionModeByBaseKey.getOrDefault(baseKey, OnePassExecutionMode.SHARDED) +
                        ", alias=" + textField(payload, "phaseThreeAlias",
                        textField(payload, "alias", "")));
            }

            return;
        }

        if (request.getRequestID() == 2) {
            parallelismByBaseKey.remove(baseKey);
            planByBaseKey.remove(baseKey);
            executionModeByBaseKey.remove(baseKey);
            phaseOneHashRoutingActive.remove(baseKey);
            phaseThreeRoutingReady.remove(baseKey);
            phaseThreeRoutingActive.remove(baseKey);

            System.out.println("[OnePassDataRouter] Removed baseKey=" + baseKey);
        }
    }

    private static void broadcastToWorkers(Datapoint value, String baseKey, int parallelism, Collector<Datapoint> out) {

        for (int worker = 0; worker < parallelism; worker++) {
            out.collect(copyWithKey(value, OnePassShardOwnership.workerKey(baseKey, parallelism, worker)));
        }
    }

    private static String describePhaseOneHashFields(CompiledOnePassPlan plan) {

        StringBuilder out = new StringBuilder();
        boolean first = true;
        for (String alias : plan.getLeafToRootOrder()) {
            CompiledOnePassPlan.DirectedJoinEdge edge = plan.getParentEdge(alias);
            if (edge == null) {
                continue;
            }

            if (!first) {
                out.append(", ");
            }

            first = false;
            out.append(alias).append("->").append(edge.getParentAlias()).append(':').append(edge.getChildFields());
        }

        return out.toString();
    }

    private static Datapoint copyWithKey(Datapoint source, String newKey) {
        return new Datapoint(newKey, source.getStreamID(), source.getValues());
    }

    private static boolean isOnePassDataBarrier(Datapoint value) {
        if (value == null || value.getValues() == null || value.getValues().isNull()) {
            return false;
        }

        JsonNode marker = value.getValues().get(ONEPASS_DATA_BARRIER_FIELD);
        return marker != null && marker.asBoolean(false);
    }

    public static boolean isOnePassEndAlias(Datapoint value) {
        if (value == null || value.getValues() == null || value.getValues().isNull()) {
            return false;
        }

        JsonNode values = value.getValues();
        String type = values.has("type") ? values.get("type").asText("") : "";
        int synopsisId = values.has("synopsisID") ? values.get("synopsisID").asInt(-1) : -1;
        return ONEPASS_END_ALIAS_TYPE.equals(type) && synopsisId == ONEPASS_SYNOPSIS_ID;
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
        return value == null || value.trim().isEmpty() ? defaultValue : value.trim();
    }
}