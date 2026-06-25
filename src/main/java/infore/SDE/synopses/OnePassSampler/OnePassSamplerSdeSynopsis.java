package infore.SDE.synopses.OnePassSampler;

import com.fasterxml.jackson.databind.ObjectMapper;
import infore.SDE.messages.Estimation;
import infore.SDE.messages.Onepass.OnePassParams;
import infore.SDE.messages.Request;
import infore.SDE.synopses.OnePassSampler.PhaseOne.OnePassPhaseOneResult;
import infore.SDE.synopses.OnePassSampler.PhaseTwo.OnePassRootSampleResult;
import infore.SDE.synopses.Synopsis;
import infore.SDE.transformations.onepass.CompiledOnePassPlan;
import infore.SDE.transformations.onepass.OnePassRequestParser;
import infore.SDE.synopses.OnePassSampler.PhaseThree.OnePassPhaseThreeResult;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * SDE-facing wrapper for the full One-pass* lifecycle.
 *
 * This is the class that SDE should instantiate as synopsisID = 30.
 *
 * Internally it owns one OnePassSamplerSynopsis, which owns:
 *
 *   Phase 1: OnePassPhaseOneState
 *   Phase 2: OnePassPhaseTwoState / OnePassRootSampler
 *   Phase 3: placeholder for later
 *
 * Supported SDE requests:
 *
 *   ADD, requestID = 1:
 *       Create this synopsis from Request.parameters.onePassParams.
 *
 *   UPDATE, requestID = 7:
 *       FINISH_PHASE_1
 *       FINISH_PHASE_2
 *       START_PHASE_3_ALIAS
 *       FINISH_PHASE_3_ALIAS
 *       FINISH_PHASE_3
 *       STATUS
 *
 *   ESTIMATE, requestID = 3 or 6:
 *       Return current status/result.
 */
public final class OnePassSamplerSdeSynopsis extends Synopsis {

    private static final long serialVersionUID = 1L;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static final String COMMAND_FINISH_PHASE_1 = "FINISH_PHASE_1";
    public static final String COMMAND_FINISH_PHASE_2 = "FINISH_PHASE_2";
    public static final String COMMAND_START_PHASE_3_ALIAS = "START_PHASE_3_ALIAS";
    public static final String COMMAND_FINISH_PHASE_3_ALIAS = "FINISH_PHASE_3_ALIAS";
    public static final String COMMAND_FINISH_PHASE_3 = "FINISH_PHASE_3";
    public static final String COMMAND_STATUS = "STATUS";

    private final OnePassParams params;
    private final CompiledOnePassPlan plan;
    private final OnePassSamplerSynopsis lifecycle;

    public OnePassSamplerSdeSynopsis(int uid, Request request) {
        super(uid, "-1", "-1");

        this.params = OnePassRequestParser.parse(request);
        this.plan = CompiledOnePassPlan.from(params);
        this.lifecycle = new OnePassSamplerSynopsis(plan);
    }

    @Override
    public void add(Object payload) {
        if (lifecycle.getPhase() == OnePassSamplerSynopsis.Phase.DONE) {
            System.out.println("[OnePassSamplerSdeSynopsis] Ignoring tuple because lifecycle is DONE.");
            return;
        }

        lifecycle.add(payload);
    }

    @Override
    public Object estimate(Object key) {
        return buildStatusJson();
    }

    @Override
    public Estimation estimate(Request request) {
        return buildEstimation(request);
    }

    /**
     * Called from SDEcoFlatMap when requestID == 7.
     */
    public Estimation handleControlRequest(Request request) {
        String command = resolveCommand(request);

        if (COMMAND_FINISH_PHASE_1.equalsIgnoreCase(command)) {
            lifecycle.finishPhaseOne();
            return buildEstimation(request);
        }

        if (COMMAND_FINISH_PHASE_2.equalsIgnoreCase(command)) {
            lifecycle.finishPhaseTwo();
            return buildEstimation(request);
        }

        /*
         * Accept both:
         *
         *   START_PHASE_3_ALIAS
         *
         * and compact forms such as:
         *
         *   START_PHASE_3_ALIAS:o
         *   START_PHASE_3_ALIAS=o
         */
        if (isStartPhaseThreeAliasCommand(command)) {
            String alias = resolvePhaseThreeAlias(request, command);
            lifecycle.startPhaseThreeAlias(alias);
            return buildEstimation(request);
        }

        if (COMMAND_FINISH_PHASE_3_ALIAS.equalsIgnoreCase(command)) {
            lifecycle.finishPhaseThreeAlias();
            return buildEstimation(request);
        }

        if (COMMAND_FINISH_PHASE_3.equalsIgnoreCase(command)) {
            lifecycle.finishPhaseThree();
            return buildEstimation(request);
        }

        if (COMMAND_STATUS.equalsIgnoreCase(command)) {
            return buildEstimation(request);
        }

        throw new IllegalArgumentException(
                "Unknown OnePass control command: " + command + ". Expected one of: "
                        + COMMAND_FINISH_PHASE_1
                        + ", "
                        + COMMAND_FINISH_PHASE_2
                        + ", "
                        + COMMAND_START_PHASE_3_ALIAS
                        + ", "
                        + COMMAND_FINISH_PHASE_3_ALIAS
                        + ", "
                        + COMMAND_FINISH_PHASE_3
                        + ", "
                        + COMMAND_STATUS
        );
    }

    private Estimation buildEstimation(Request request) {
        return new Estimation(
                request,
                buildStatusJson(),
                Integer.toString(request.getUID())
        );
    }

    private String resolveCommand(Request request) {
        /*
         * Preferred JSON form:
         *
         *   "parameters": {
         *       "onePassCommand": "FINISH_PHASE_1"
         *   }
         */
        JsonNode parameters = request.getParameters();

        if (parameters != null && !parameters.isNull()) {
            JsonNode commandNode = parameters.get("onePassCommand");

            if (commandNode != null && !commandNode.isNull()) {
                String command = commandNode.asText();

                if (command != null && !command.trim().isEmpty()) {
                    return command.trim();
                }
            }
        }

        /*
         * Backwards/simple form:
         *
         *   "param": ["FINISH_PHASE_1"]
         */
        String[] param = request.getParam();

        if (param != null && param.length > 0) {
            if (param[0] != null && !param[0].trim().isEmpty()) {
                return param[0].trim();
            }
        }

        return COMMAND_STATUS;
    }

    private boolean isStartPhaseThreeAliasCommand(String command) {
        if (command == null) {
            return false;
        }

        String trimmed = command.trim();

        return COMMAND_START_PHASE_3_ALIAS.equalsIgnoreCase(trimmed)
                || trimmed.toUpperCase().startsWith(COMMAND_START_PHASE_3_ALIAS + ":")
                || trimmed.toUpperCase().startsWith(COMMAND_START_PHASE_3_ALIAS + "=");
    }

    private String resolvePhaseThreeAlias(Request request, String command) {
        /*
         * Preferred JSON form:
         *
         *   "parameters": {
         *       "onePassCommand": "START_PHASE_3_ALIAS",
         *       "onePassAlias": "o"
         *   }
         *
         * or:
         *
         *   "parameters": {
         *       "onePassCommand": "START_PHASE_3_ALIAS",
         *       "phaseThreeAlias": "o"
         *   }
         */
        JsonNode parameters = request.getParameters();

        if (parameters != null && !parameters.isNull()) {
            JsonNode aliasNode = parameters.get("onePassAlias");

            if (aliasNode == null || aliasNode.isNull()) {
                aliasNode = parameters.get("phaseThreeAlias");
            }

            if (aliasNode != null && !aliasNode.isNull()) {
                String alias = aliasNode.asText();

                if (alias != null && !alias.trim().isEmpty()) {
                    return alias.trim();
                }
            }
        }

        /*
         * Compact command form:
         *   START_PHASE_3_ALIAS:o
         *   START_PHASE_3_ALIAS=o
         */
        if (command != null) {
            String trimmed = command.trim();

            int colon = trimmed.indexOf(':');

            int equals = trimmed.indexOf('=');

            int separator = -1;

            if (colon >= 0) {
                separator = colon;
            } else if (equals >= 0) {
                separator = equals;
            }

            if (separator >= 0 && separator + 1 < trimmed.length()) {
                String alias = trimmed.substring(separator + 1).trim();

                if (!alias.isEmpty()) {
                    return alias;
                }
            }
        }

        /*
         * Backwards/simple param form:
         *
         *   "param": ["START_PHASE_3_ALIAS", "o"]
         */
        String[] param = request.getParam();

        if (param != null && param.length > 1) {
            String alias = param[1];

            if (alias != null && !alias.trim().isEmpty()) {
                return alias.trim();
            }
        }

        throw new IllegalArgumentException("Missing Phase 3 alias for command " + command +
                ". Use parameters.onePassAlias, parameters.phaseThreeAlias, " +
                "param[1], or compact command START_PHASE_3_ALIAS:<alias>.");
    }

    private String buildStatusJson() {
        try {
            return MAPPER.writeValueAsString(buildStatusPayload());
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Could not serialize OnePass SDE status payload",
                    e
            );
        }
    }

    private Map<String, Object> buildStatusPayload() {
        Map<String, Object> out = new LinkedHashMap<String, Object>();

        out.put("queryName", plan.getQueryName());
        out.put("rootAlias", plan.getRootAlias());
        out.put("phase", lifecycle.getPhase().name());
        out.put("sampleSize", plan.getSampleSize());

        /*
         * Always copy plan collections into normal mutable JDK containers.
         */
        out.put("leafToRootOrder", new ArrayList<String>(plan.getLeafToRootOrder()));

        out.put("rootToLeafOrder", new ArrayList<String>(plan.getRootToLeafOrder()));

        out.put("weightsByAlias", new LinkedHashMap<String, String>(plan.getWeightsByAlias()));

        OnePassPhaseOneResult phaseOneResult = lifecycle.getPhaseOneResult();

        if (phaseOneResult != null) {
            out.put("phaseOneComplete", true);

            out.put("seenTuplesByAlias", new LinkedHashMap<String, Long>(phaseOneResult.getSeenTuplesByAlias()));

            /*
             * Do NOT put Phase1LinkWeightIndex objects in the Estimation.
             *
             * Those objects contain JoinValue instances with unmodifiable
             * collections. Flink/Kryo fails when it tries to copy them.
             *
             * For the lifecycle test we only need to know that indexes exist.
             * The standalone Phase 1 debug test can remain responsible for
             * exporting full index contents.
             */
            Map rawIndexes = phaseOneResult.copyRawIndexesByEdgeId();

            out.put("edgeIndexCount", rawIndexes.size());
            out.put("edgeIndexIds", new ArrayList(rawIndexes.keySet()));
        } else {
            out.put("phaseOneComplete", false);
            out.put("edgeIndexCount", 0);
            out.put("edgeIndexIds", new ArrayList());
        }

        OnePassRootSampleResult phaseTwoResult = lifecycle.getPhaseTwoResult();

        if (phaseTwoResult != null) {
            out.put("phaseTwoComplete", true);

            out.put("rootTuplesSeen", phaseTwoResult.getRootTuplesSeen());
            out.put("positiveRootCandidatesSeen", phaseTwoResult.getPositiveRootCandidatesSeen());
            out.put("totalRootGroupWeight", phaseTwoResult.getTotalRootGroupWeight());

            /*
             * This is safe because buildStatusJson() serializes it to String
             * before Flink/Kryo sees the Estimation payload.
             */
            out.put("sampleInstances", new ArrayList(phaseTwoResult.getSampleInstances()));
        } else {
            out.put("phaseTwoComplete", false);
            out.put("rootTuplesSeen", 0L);
            out.put("positiveRootCandidatesSeen", 0L);
            out.put("totalRootGroupWeight", 0.0d);
            out.put("sampleInstances", new ArrayList());
        }

        OnePassPhaseThreeResult phaseThreeResult = lifecycle.getPhaseThreeResult();

        out.put("phaseThreeAliasActive", lifecycle.isPhaseThreeAliasActive());
        out.put("phaseThreeActiveAlias", lifecycle.getPhaseThreeActiveAlias());

        if (phaseThreeResult != null) {
            out.put("phaseThreeComplete", true);

            out.put("completedSampleCount", phaseThreeResult.getCompletedSamples().size());
            out.put("requestedPhaseThreeSampleSize", phaseThreeResult.getRequestedSampleSize());

            /*
             * Safe because buildStatusJson() serializes the payload to a String
             * before it enters the SDE Estimation object.
             *
             * Keep this for tests. Later, if output becomes too large, replace this
             * with projected samples or a sample preview.
             */
            out.put("completedSamples", new ArrayList(phaseThreeResult.getCompletedSamples()));
        } else {
            out.put("phaseThreeComplete", false);
            out.put("completedSampleCount", 0);
            out.put("requestedPhaseThreeSampleSize", plan.getSampleSize());
            out.put("completedSamples", new ArrayList());
        }

        return out;
    }

    public OnePassSamplerSynopsis getLifecycle() {
        return lifecycle;
    }

    public CompiledOnePassPlan getPlan() {
        return plan;
    }

    public OnePassParams getParams() {
        return params;
    }

    @Override
    public Synopsis merge(Synopsis other) {
        /*
         * Parallel merge is intentionally not implemented yet.
         *
         * Multiworker One-pass* will need explicit global barriers:
         *
         *   Phase 1 local states -> global merged Phase 1 result
         *   Phase 2 local samples -> global merged root sample
         *
         * For now the project constraint remains noOfP = 1.
         */
        return null;
    }
}