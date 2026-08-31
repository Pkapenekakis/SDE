package infore.SDE.transformations.onepass;

import com.fasterxml.jackson.databind.JsonNode;
import infore.SDE.messages.Datapoint;
import infore.SDE.messages.Request;
import infore.SDE.synopses.OnePassSampler.OnePassTuple;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * OnePass-specific data router.
 *
 * Supported routing modes:
 *
 * 1) ROUND_ROBIN
 *    Normal tuples are distributed cyclically across the logical OnePass workers.
 *
 * 2) JOIN_KEY_HASH
 *    During Phase 1, every non-root tuple is routed according to the child-side
 *    join key of its parent edge:
 *
 *        worker = hash(parent-edge child join key) mod P
 *
 *    This keeps all tuples contributing to the same Phase-1 continuation-weight
 *    entry on the same worker, reducing duplicate join-key entries across worker
 *    partial indexes before the global merge.
 *
 *    Once START_PHASE_2 is observed, routing falls back to ROUND_ROBIN so that
 *    Phase 2 / Phase 3 behaviour remains unchanged by this optimization.
 *
 * END_ALIAS and the older OnePass data barrier are always broadcast to all
 * logical OnePass workers.
 *
 * Request routing remains the responsibility of RqRouterFlatMap.
 */
public final class OnePassDataRouterCoFlatMap
        extends RichCoFlatMapFunction<Datapoint, Request, Datapoint> {

    private static final long serialVersionUID = 1L;

    private static final String ONEPASS_DATA_BARRIER_FIELD =
            "__onePassDataBarrier";

    private static final String ONEPASS_END_ALIAS_TYPE =
            "END_ALIAS";

    private static final String START_PHASE_2 =
            "START_PHASE_2";

    private static final int ONEPASS_SYNOPSIS_ID = 30;

    public enum RoutingMode {
        ROUND_ROBIN,
        JOIN_KEY_HASH;

        public static RoutingMode fromString(String value) {

            if (value == null || value.trim().isEmpty()) {
                return ROUND_ROBIN;
            }

            String normalized =
                    value.trim().toUpperCase(Locale.ROOT);

            if ("ROUND_ROBIN".equals(normalized)
                    || "ROUNDROBIN".equals(normalized)
                    || "RR".equals(normalized)) {

                return ROUND_ROBIN;
            }

            if ("JOIN_KEY_HASH".equals(normalized)
                    || "JOINKEYHASH".equals(normalized)
                    || "HASH".equals(normalized)
                    || "KEY_HASH".equals(normalized)) {

                return JOIN_KEY_HASH;
            }

            throw new IllegalArgumentException(
                    "Unknown OnePass routing mode '"
                            + value
                            + "'. Expected ROUND_ROBIN or JOIN_KEY_HASH."
            );
        }
    }

    private final RoutingMode routingMode;

    private final Map<String, Integer> parallelismByBaseKey =
            new HashMap<String, Integer>();

    private final Map<String, Integer> nextWorkerByBaseKey =
            new HashMap<String, Integer>();

    private final Map<String, CompiledOnePassPlan> planByBaseKey =
            new HashMap<String, CompiledOnePassPlan>();

    private final Set<String> phaseOneHashRoutingActive =
            new HashSet<String>();

    /**
     * Backwards-compatible default: preserve current round-robin behaviour.
     */
    public OnePassDataRouterCoFlatMap() {
        this(RoutingMode.ROUND_ROBIN);
    }

    public OnePassDataRouterCoFlatMap(RoutingMode routingMode) {

        this.routingMode =
                routingMode == null
                        ? RoutingMode.ROUND_ROBIN
                        : routingMode;
    }

    @Override
    public void flatMap1(
            Datapoint value,
            Collector<Datapoint> out) throws Exception {

        if (value == null) {
            return;
        }

        String baseKey = value.getDataSetkey();

        Integer p = parallelismByBaseKey.get(baseKey);

        if (p == null || p <= 1) {
            out.collect(value);
            return;
        }

        if (isOnePassDataBarrier(value)
                || isOnePassEndAlias(value)) {

            broadcastToWorkers(
                    value,
                    baseKey,
                    p,
                    out
            );

            return;
        }

        if (routingMode == RoutingMode.JOIN_KEY_HASH
                && phaseOneHashRoutingActive.contains(baseKey)) {

            CompiledOnePassPlan plan =
                    planByBaseKey.get(baseKey);

            if (plan == null) {
                throw new IllegalStateException(
                        "JOIN_KEY_HASH routing is active without a compiled OnePass plan. "
                                + "baseKey="
                                + baseKey
                );
            }

            OnePassTuple tuple =
                    OnePassTupleExtractor.extract(
                            value.getValues()
                    );

            String alias = tuple.getTable();

            if (!plan.containsAlias(alias)) {
                throw new IllegalStateException(
                        "Tuple alias '"
                                + alias
                                + "' is not present in the compiled OnePass plan. "
                                + "baseKey="
                                + baseKey
                );
            }

            if (!plan.isRoot(alias)) {

                int worker =
                        chooseJoinKeyWorker(
                                tuple,
                                alias,
                                plan,
                                p
                        );

                out.collect(
                        copyWithKey(
                                value,
                                routedKey(
                                        baseKey,
                                        p,
                                        worker
                                )
                        )
                );

                return;
            }
        }

        routeRoundRobin(
                value,
                baseKey,
                p,
                out
        );
    }

    @Override
    public void flatMap2(
            Request request,
            Collector<Datapoint> out) throws Exception {

        if (request == null) {
            return;
        }

        if (request.getSynopsisID() != ONEPASS_SYNOPSIS_ID) {
            return;
        }

        String baseKey = request.getKey();

        if (request.getRequestID() == 1) {

            int p = request.getNoOfP();

            if (p <= 0) {
                throw new IllegalArgumentException(
                        "OnePass ADD requires noOfP > 0. "
                                + "baseKey="
                                + baseKey
                                + ", noOfP="
                                + p
                );
            }

            parallelismByBaseKey.put(baseKey, p);
            nextWorkerByBaseKey.put(baseKey, 0);

            if (routingMode == RoutingMode.JOIN_KEY_HASH) {

                CompiledOnePassPlan plan =
                        CompiledOnePassPlan.from(
                                OnePassRequestParser.parse(
                                        request
                                )
                        );

                planByBaseKey.put(baseKey, plan);
                phaseOneHashRoutingActive.add(baseKey);

                System.out.println(
                        "[OnePassDataRouter] Registered "
                                + "baseKey="
                                + baseKey
                                + ", parallelism="
                                + p
                                + ", mode="
                                + routingMode
                                + ", phase1HashFields="
                                + describePhaseOneHashFields(plan)
                );

            } else {

                planByBaseKey.remove(baseKey);
                phaseOneHashRoutingActive.remove(baseKey);

                System.out.println(
                        "[OnePassDataRouter] Registered "
                                + "baseKey="
                                + baseKey
                                + ", parallelism="
                                + p
                                + ", mode="
                                + routingMode
                );
            }

            return;
        }

        if (request.getRequestID() == 7
                && routingMode == RoutingMode.JOIN_KEY_HASH) {

            JsonNode payload = request.getParameters();

            String type =
                    textField(
                            payload,
                            "type",
                            ""
                    );

            if (START_PHASE_2.equals(type)) {

                phaseOneHashRoutingActive.remove(baseKey);

                System.out.println(
                        "[OnePassDataRouter] Phase 1 hash routing complete. "
                                + "baseKey="
                                + baseKey
                                + ", fallback="
                                + RoutingMode.ROUND_ROBIN
                );
            }

            return;
        }

        if (request.getRequestID() == 2) {

            parallelismByBaseKey.remove(baseKey);
            nextWorkerByBaseKey.remove(baseKey);
            planByBaseKey.remove(baseKey);
            phaseOneHashRoutingActive.remove(baseKey);

            System.out.println(
                    "[OnePassDataRouter] Removed "
                            + "baseKey="
                            + baseKey
                            + ", mode="
                            + routingMode
            );
        }
    }

    private static int chooseJoinKeyWorker(
            OnePassTuple tuple,
            String alias,
            CompiledOnePassPlan plan,
            int parallelism) {

        CompiledOnePassPlan.DirectedJoinEdge parentEdge =
                plan.getParentEdge(alias);

        if (parentEdge == null) {
            throw new IllegalStateException(
                    "Non-root alias '"
                            + alias
                            + "' has no parent edge."
            );
        }

        List<String> joinFields =
                parentEdge.getChildFields();

        if (joinFields == null || joinFields.isEmpty()) {
            throw new IllegalStateException(
                    "Parent edge "
                            + parentEdge.getEdgeId()
                            + " has no child-side join fields for alias "
                            + alias
            );
        }

        String routingKey =
                buildCanonicalJoinKey(
                        tuple,
                        joinFields
                );

        return stableWorkerHash(
                routingKey,
                parallelism
        );
    }

    private static String buildCanonicalJoinKey(
            OnePassTuple tuple,
            List<String> fields) {

        StringBuilder key = new StringBuilder();

        for (String field : fields) {

            JsonNode value = tuple.getField(field);

            if (value == null || value.isNull()) {
                throw new IllegalStateException(
                        "Missing Phase-1 join-key field '"
                                + field
                                + "' in alias "
                                + tuple.getTable()
                                + ". tuple="
                                + tuple
                );
            }

            String canonicalValue = canonicalJsonValue(value);

            key.append(field.length());
            key.append(':');
            key.append(field);
            key.append('=');
            key.append(canonicalValue.length());
            key.append(':');
            key.append(canonicalValue);
            key.append('|');
        }

        return key.toString();
    }

    private static String canonicalJsonValue(JsonNode value) {

        if (value.isTextual()) {
            return "S:" + value.asText();
        }

        if (value.isIntegralNumber()) {
            return "I:" + value.bigIntegerValue().toString();
        }

        if (value.isFloatingPointNumber()) {
            return "D:"
                    + value.decimalValue()
                    .stripTrailingZeros()
                    .toPlainString();
        }

        if (value.isBoolean()) {
            return "B:" + Boolean.toString(value.asBoolean());
        }

        return "J:" + value.toString();
    }

    /**
     * Deterministic FNV-1a 64-bit hash folded to an int.
     */
    private static int stableWorkerHash(
            String routingKey,
            int parallelism) {

        if (parallelism <= 1) {
            return 0;
        }

        byte[] bytes =
                routingKey.getBytes(StandardCharsets.UTF_8);

        long hash = -3750763034362895579L;
        final long prime = 1099511628211L;

        for (byte b : bytes) {
            hash ^= (b & 0xff);
            hash *= prime;
        }

        int folded =
                (int) (hash ^ (hash >>> 32));

        return Math.floorMod(
                folded,
                parallelism
        );
    }

    private void routeRoundRobin(
            Datapoint value,
            String baseKey,
            int parallelism,
            Collector<Datapoint> out) {

        int nextWorker =
                nextWorkerByBaseKey.containsKey(baseKey)
                        ? nextWorkerByBaseKey.get(baseKey)
                        : 0;

        out.collect(
                copyWithKey(
                        value,
                        routedKey(
                                baseKey,
                                parallelism,
                                nextWorker
                        )
                )
        );

        nextWorker++;

        if (nextWorker >= parallelism) {
            nextWorker = 0;
        }

        nextWorkerByBaseKey.put(
                baseKey,
                nextWorker
        );
    }

    private static void broadcastToWorkers(
            Datapoint value,
            String baseKey,
            int parallelism,
            Collector<Datapoint> out) {

        for (int worker = 0;
             worker < parallelism;
             worker++) {

            out.collect(
                    copyWithKey(
                            value,
                            routedKey(
                                    baseKey,
                                    parallelism,
                                    worker
                            )
                    )
            );
        }
    }

    private static String describePhaseOneHashFields(
            CompiledOnePassPlan plan) {

        StringBuilder out = new StringBuilder();
        boolean first = true;

        for (String alias : plan.getLeafToRootOrder()) {

            CompiledOnePassPlan.DirectedJoinEdge edge =
                    plan.getParentEdge(alias);

            if (edge == null) {
                continue;
            }

            if (!first) {
                out.append(", ");
            }

            first = false;

            out.append(alias);
            out.append("->");
            out.append(edge.getParentAlias());
            out.append(":");
            out.append(edge.getChildFields());
        }

        return out.toString();
    }

    private static Datapoint copyWithKey(
            Datapoint source,
            String newKey) {

        JsonNode values = source.getValues();

        return new Datapoint(
                newKey,
                source.getStreamID(),
                values
        );
    }

    private static String routedKey(
            String baseKey,
            int parallelism,
            int workerId) {

        return baseKey
                + "_"
                + parallelism
                + "_KEYED_"
                + workerId;
    }

    private static boolean isOnePassDataBarrier(Datapoint value) {

        if (value == null
                || value.getValues() == null
                || value.getValues().isNull()) {

            return false;
        }

        JsonNode marker =
                value.getValues().get(ONEPASS_DATA_BARRIER_FIELD);

        return marker != null && marker.asBoolean(false);
    }

    public static boolean isOnePassEndAlias(Datapoint value) {

        if (value == null
                || value.getValues() == null
                || value.getValues().isNull()) {

            return false;
        }

        JsonNode values = value.getValues();

        String type =
                values.has("type")
                        ? values.get("type").asText("")
                        : "";

        int synopsisId =
                values.has("synopsisID")
                        ? values.get("synopsisID").asInt(-1)
                        : -1;

        return ONEPASS_END_ALIAS_TYPE.equals(type)
                && synopsisId == ONEPASS_SYNOPSIS_ID;
    }

    private static String textField(
            JsonNode node,
            String fieldName,
            String defaultValue) {

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
}
