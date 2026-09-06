package infore.SDE.synopses.OnePassSampler;

import com.fasterxml.jackson.databind.ObjectMapper;
import infore.SDE.messages.Estimation;
import infore.SDE.messages.Onepass.OnePassParams;
import infore.SDE.messages.Request;
import infore.SDE.synopses.OnePassSampler.PhaseOne.JoinValue;
import infore.SDE.synopses.OnePassSampler.PhaseOne.OnePassPhaseOneResult;
import infore.SDE.synopses.OnePassSampler.PhaseTwo.OnePassRootSampleResult;
import infore.SDE.synopses.Synopsis;
import infore.SDE.transformations.onepass.CompiledOnePassPlan;
import infore.SDE.transformations.onepass.OnePassRequestParser;
import infore.SDE.synopses.OnePassSampler.PhaseThree.OnePassPhaseThreeResult;
import infore.SDE.synopses.OnePassSampler.PhaseThree.OnePassCompletedSample;
import com.fasterxml.jackson.databind.JsonNode;
import infore.SDE.synopses.OnePassSampler.PhaseOne.Phase1LinkWeightIndex;
import infore.SDE.synopses.OnePassSampler.PhaseTwo.OnePassPhaseTwoState;
import infore.SDE.synopses.OnePassSampler.PhaseTwo.OnePassRootSampleCandidate;
import infore.SDE.synopses.OnePassSampler.PhaseTwo.WeightedReservoirEntry;
import infore.SDE.synopses.OnePassSampler.PhaseTwo.OnePassRootSampleInstance;
import infore.SDE.synopses.OnePassSampler.PhaseOne.OnePassPhaseOneContribution;
import infore.SDE.synopses.OnePassSampler.PhaseOne.JoinValue;

import java.util.*;

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

    private final Map<String, Long> addRowsByPhaseAlias = new LinkedHashMap<String, Long>();
    private final Map<String, Long> addNanosByPhaseAlias = new LinkedHashMap<String, Long>();

    private long totalAddRows = 0L;
    private long totalAddNanos = 0L;

    private static final int FULL_COMPLETED_SAMPLES_STATUS_LIMIT = 200;
    private static final int COMPLETED_SAMPLES_PREVIEW_LIMIT = 5;

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

        String profileKey = profileKeyForPayload(payload);
        long startNanos = System.nanoTime();

        try {
            lifecycle.add(payload);
        } finally {
            long elapsedNanos = System.nanoTime() - startNanos;
            recordAddProfile(profileKey, elapsedNanos);
        }
    }

    @Override
    public Object estimate(Object key) {
        return buildStatusJson();
    }

    @Override
    public Estimation estimate(Request request) {
        return buildEstimation(request);
    }

    private String profileKeyForPayload(Object payload) {
        String phase = lifecycle.getPhase().name();
        String alias = aliasForPayload(payload);

        if (lifecycle.getPhase() == OnePassSamplerSynopsis.Phase.PHASE_3
                && lifecycle.isPhaseThreeAliasActive()
                && lifecycle.getPhaseThreeActiveAlias() != null
                && !lifecycle.getPhaseThreeActiveAlias().trim().isEmpty()) {
            alias = lifecycle.getPhaseThreeActiveAlias().trim();
        }

        if (alias.trim().isEmpty()) {
            alias = "unknown";
        }

        return phase + ":" + alias;
    }

    private String aliasForPayload(Object payload) {
        if (payload instanceof JsonNode) {
            JsonNode node = (JsonNode) payload;
            JsonNode aliasNode = node.get("alias");

            if (aliasNode != null && !aliasNode.isNull()) {
                String alias = aliasNode.asText();

                if (alias != null && !alias.trim().isEmpty()) {
                    return alias.trim();
                }
            }
        }

        return "unknown";
    }

    private void recordAddProfile(String profileKey, long elapsedNanos) {
        if (profileKey == null || profileKey.trim().isEmpty()) {
            profileKey = "unknown";
        }

        Long rows = addRowsByPhaseAlias.get(profileKey);

        if (rows == null) {
            rows = 0L;
        }

        addRowsByPhaseAlias.put(profileKey, rows + 1L);

        Long nanos = addNanosByPhaseAlias.get(profileKey);

        if (nanos == null) {
            nanos = 0L;
        }

        addNanosByPhaseAlias.put(profileKey, nanos + elapsedNanos);

        totalAddRows++;
        totalAddNanos += elapsedNanos;
    }

    private Map<String, Object> buildAddProfilePayload() {
        Map<String, Object> out = new LinkedHashMap<String, Object>();

        out.put("totalAddRows", totalAddRows);
        out.put("totalAddNanos", totalAddNanos);
        out.put("totalAddSeconds", totalAddNanos / 1_000_000_000.0d);

        double rowsPerSecond = totalAddNanos <= 0L ? 0.0d : totalAddRows / (totalAddNanos / 1_000_000_000.0d);

        out.put("totalAddRowsPerSecond", rowsPerSecond);
        out.put("rowsByPhaseAlias", new LinkedHashMap<String, Long>(addRowsByPhaseAlias));
        out.put("addNanosByPhaseAlias", new LinkedHashMap<String, Long>(addNanosByPhaseAlias));

        Map<String, Double> addSecondsByPhaseAlias = new LinkedHashMap<String, Double>();
        Map<String, Double> rowsPerSecondByPhaseAlias = new LinkedHashMap<String, Double>();

        for (Map.Entry<String, Long> entry : addNanosByPhaseAlias.entrySet()) {
            String key = entry.getKey();

            long nanos = entry.getValue() == null ? 0L : entry.getValue().longValue();
            long rows = 0L;
            Long rowCount = addRowsByPhaseAlias.get(key);

            if (rowCount != null) {
                rows = rowCount.longValue();
            }

            double seconds = nanos / 1_000_000_000.0d;

            addSecondsByPhaseAlias.put(key, seconds);
            rowsPerSecondByPhaseAlias.put(key, seconds <= 0.0d ? 0.0d : rows / seconds
            );
        }

        out.put("addSecondsByPhaseAlias", addSecondsByPhaseAlias);
        out.put("addRowsPerSecondByPhaseAlias", rowsPerSecondByPhaseAlias);

        return out;
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
        out.put("projection", new ArrayList<String>(plan.getProjection()));
        out.put("requiredFieldsByAlias", copyRequiredFieldsByAlias(plan.getRequiredFieldsByAlias()));
        out.put("onePassAddProfile", buildAddProfilePayload());
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
            out.put("phaseOneSummary", phaseOneResult.toSummaryMap(0));
            out.put("phaseOneEdgeSummaries", phaseOneResult.getEdgeSummaries(0));
        } else {
            out.put("phaseOneComplete", false);
            out.put("edgeIndexCount", 0);
            out.put("edgeIndexIds", new ArrayList());
            out.put("phaseOneSummary", new LinkedHashMap<String, Object>());
            out.put("phaseOneEdgeSummaries", new LinkedHashMap<String, Object>());
        }

        OnePassRootSampleResult phaseTwoResult = lifecycle.getPhaseTwoResult();

        if (phaseTwoResult != null) {
            int sampleInstanceCount = phaseTwoResult.getSampleInstances().size();

            out.put("phaseTwoComplete", true);

            out.put("rootTuplesSeen", phaseTwoResult.getRootTuplesSeen());
            out.put("positiveRootCandidatesSeen", phaseTwoResult.getPositiveRootCandidatesSeen());
            out.put("totalRootGroupWeight", phaseTwoResult.getTotalRootGroupWeight());

            out.put("sampleInstanceCount", sampleInstanceCount);
            out.put("requestedPhaseTwoSampleSize", phaseTwoResult.getRequestedSampleSize());

            boolean includeFullSampleInstances = sampleInstanceCount <= FULL_COMPLETED_SAMPLES_STATUS_LIMIT;

            out.put("sampleInstancesIncluded", includeFullSampleInstances);
            out.put("sampleInstancesTruncated", !includeFullSampleInstances);

            if (includeFullSampleInstances) {
                out.put("sampleInstances", new ArrayList(phaseTwoResult.getSampleInstances()));
                out.put("sampleInstancesPreview", new ArrayList());
            } else {
                /*
                 * Benchmark / large-sample mode:
                 * Do not send thousands of full root sample tuples through Kafka.
                 * Keep the exact count and a tiny preview only.
                 */
                out.put("sampleInstances", new ArrayList());

                int previewSize = Math.min(COMPLETED_SAMPLES_PREVIEW_LIMIT, sampleInstanceCount);

                out.put("sampleInstancesPreview", new ArrayList(phaseTwoResult.getSampleInstances()
                                        .subList(0, previewSize)));
            }
        } else {
            out.put("phaseTwoComplete", false);
            out.put("rootTuplesSeen", 0L);
            out.put("positiveRootCandidatesSeen", 0L);
            out.put("totalRootGroupWeight", 0.0d);

            out.put("sampleInstanceCount", 0);
            out.put("requestedPhaseTwoSampleSize", plan.getSampleSize());
            out.put("sampleInstancesIncluded", true);
            out.put("sampleInstancesTruncated", false);
            out.put("sampleInstances", new ArrayList());
            out.put("sampleInstancesPreview", new ArrayList());
        }

        OnePassPhaseThreeResult phaseThreeResult = lifecycle.getPhaseThreeResult();

        out.put("phaseThreeAliasActive", lifecycle.isPhaseThreeAliasActive());
        out.put("phaseThreeActiveAlias", lifecycle.getPhaseThreeActiveAlias());

        if (phaseThreeResult != null) {
            int completedCount = phaseThreeResult.getCompletedSamples().size();

            out.put("phaseThreeComplete", true);
            out.put("completedSampleCount", completedCount);
            out.put("requestedPhaseThreeSampleSize", phaseThreeResult.getRequestedSampleSize());

            boolean includeFullCompletedSamples = completedCount <= FULL_COMPLETED_SAMPLES_STATUS_LIMIT;

            out.put("completedSamplesIncluded", includeFullCompletedSamples);
            out.put("completedSamplesTruncated", !includeFullCompletedSamples);

            if (includeFullCompletedSamples) {
                /*
                 * Debug/correctness mode:
                 * Keep full joined samples so tests can validate join conditions.
                 */
                out.put("completedSamples", new ArrayList(phaseThreeResult.getCompletedSamples()));
                out.put("completedSamplesPreview", new ArrayList());
            } else {
                /*
                 * Benchmark mode:
                 * Do not send thousands of full joined samples through Kafka.
                 */
                out.put("completedSamples", new ArrayList());

                int previewSize = Math.min(COMPLETED_SAMPLES_PREVIEW_LIMIT, completedCount);

                out.put("completedSamplesPreview", new ArrayList(phaseThreeResult.getCompletedSamples().
                        subList(0, previewSize)));
            }

            /*
             * Final SELECT-projected output.
             *
             * This is the output that should be compared with the original
             * standalone One-pass* implementation.
             */
            out.put("projectedOutput", true);

            boolean includeProjectedSamples = completedCount <= FULL_COMPLETED_SAMPLES_STATUS_LIMIT;

            out.put("projectedSamplesIncluded", includeProjectedSamples);
            out.put("projectedSamplesTruncated", !includeProjectedSamples);

            if (includeProjectedSamples) {
                out.put("projectedCompletedSamples", projectCompletedSamples(phaseThreeResult.getCompletedSamples()));
                out.put("projectedCompletedSamplesPreview", new ArrayList());
            } else {
                out.put("projectedCompletedSamples", new ArrayList());

                int previewSize = Math.min(COMPLETED_SAMPLES_PREVIEW_LIMIT, completedCount);

                out.put("projectedCompletedSamplesPreview",
                        projectCompletedSamples(phaseThreeResult.getCompletedSamples().subList(0, previewSize)));
            }
        } else {
            out.put("phaseThreeComplete", false);
            out.put("completedSampleCount", 0);
            out.put("requestedPhaseThreeSampleSize", plan.getSampleSize());

            out.put("completedSamplesIncluded", true);
            out.put("completedSamplesTruncated", false);
            out.put("completedSamples", new ArrayList());
            out.put("completedSamplesPreview", new ArrayList());

            out.put("projectedOutput", true);
            out.put("projectedSamplesIncluded", true);
            out.put("projectedSamplesTruncated", false);
            out.put("projectedCompletedSamples", new ArrayList());
            out.put("projectedCompletedSamplesPreview", new ArrayList());
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
        return null;
    }

    private List<Map<String, Object>> projectCompletedSamples(List<OnePassCompletedSample> completedSamples) {

        List<Map<String, Object>> out = new ArrayList<Map<String, Object>>();

        if (completedSamples == null || completedSamples.isEmpty()) {
            return out;
        }

        for (OnePassCompletedSample completedSample : completedSamples) {
            out.add(projectCompletedSample(completedSample));
        }

        return out;
    }

    private Map<String, Object> projectCompletedSample(OnePassCompletedSample completedSample) {

        Map<String, Object> out = new LinkedHashMap<String, Object>();

        out.put("sampleInstanceId", completedSample.getSampleInstanceId());
        out.put("sourceRootCandidateId", completedSample.getSourceRootCandidateId());

        Map<String, Object> projected = new LinkedHashMap<String, Object>();

        for (String projectionItem : plan.getProjection()) {
            if (projectionItem == null) {
                continue;
            }

            String trimmed = projectionItem.trim();

            if (trimmed.isEmpty()) {
                continue;
            }

            /*
             * SELECT * should normally already be resolved by the compiler into
             * the catalog projection. Keep this fallback only for robustness.
             */
            if ("*".equals(trimmed)) {
                projected.put("*", completedSample.getTuplesByAlias());
                continue;
            }

            int dot = trimmed.indexOf('.');

            if (dot <= 0 || dot == trimmed.length() - 1) {
                throw new IllegalStateException("Invalid compiled projection item: " + trimmed
                                + ". Expected alias.field");
            }

            String alias = trimmed.substring(0, dot);
            String field = trimmed.substring(dot + 1);

            OnePassTuple tuple = completedSample.getTuple(alias);

            if (tuple == null) {
                throw new IllegalStateException("Cannot project field " + trimmed + " because completed sample " +
                                completedSample.getSampleInstanceId() + " has no tuple for alias " + alias);
            }

            JsonNode value = tuple.getField(field);

            if (value == null || value.isNull()) {
                projected.put(trimmed, null);
            } else {
                /*
                 * Keep the JsonNode value so Jackson preserves numeric/boolean/string
                 * types when serializing the status payload.
                 */
                projected.put(trimmed, value);
            }
        }

        out.put("projected", projected);

        return out;
    }

    private Map<String, List<String>> copyRequiredFieldsByAlias(Map<String, Set<String>> requiredFieldsByAlias) {
        Map<String, List<String>> out = new LinkedHashMap<String, List<String>>();

        if (requiredFieldsByAlias == null) {
            return out;
        }

        for (Map.Entry<String, Set<String>> entry : requiredFieldsByAlias.entrySet()) {
            out.put(entry.getKey(), new ArrayList<String>(entry.getValue()));
        }

        return out;
    }

    public Estimation buildLocalPhaseOneResultEstimation(
            Request request,
            int workerId,
            int expectedWorkers,
            int actualParallelism,
            String resultId,
            String activeAlias) {

        if (request == null) {
            throw new IllegalArgumentException(
                    "request must not be null"
            );
        }

        return buildLocalPhaseOneResultEstimation(
                request.getKey(),
                request.getUID(),
                workerId,
                expectedWorkers,
                actualParallelism,
                resultId,
                activeAlias,
                "",
                ""
        );
    }

    /**
     * Builds the worker-local Phase 1 merge payload directly from an END_ALIAS
     * data-path marker.
     *
     * This method does NOT advance the OnePass lifecycle.
     */
    public Estimation buildLocalPhaseOneResultEstimation(String workerKey, int uid, int workerId, int expectedWorkers,
                                                         int actualParallelism, String resultId, String activeAlias,
                                                         String nextCommand, String nextAlias) {

        //OnePassPhaseOneResult phaseOneResult = lifecycle.exportLocalPhaseOneResult();

        Map<String, Object> payload = new LinkedHashMap<String, Object>();
        String normalizedWorkerKey = workerKey == null ? "" : workerKey.trim();
        String baseKey = stripOnePassWorkerSuffix(normalizedWorkerKey, expectedWorkers, workerId);

        payload.put("type", "LOCAL_PHASE1_RESULT");
        payload.put("uid", uid);
        payload.put("workerId", workerId);
        payload.put("expectedWorkers", expectedWorkers);
        payload.put("actualParallelism", actualParallelism);
        payload.put("phase", "PHASE1");
        payload.put("resultId", resultId);
        payload.put("queryName", plan.getQueryName());
        payload.put("rootAlias", plan.getRootAlias());
        payload.put("workerKey", normalizedWorkerKey);
        payload.put("baseKey", baseKey);

        String normalizedActiveAlias = activeAlias == null ? "" : activeAlias.trim();
        String activeEdgeId = "";

        if (!normalizedActiveAlias.isEmpty() && !plan.isRoot(normalizedActiveAlias)) {

            CompiledOnePassPlan.DirectedJoinEdge parentEdge = plan.getParentEdge(normalizedActiveAlias);

            if (parentEdge != null) {
                activeEdgeId = parentEdge.getEdgeId();
            }
        }

        /*
         * All workers contribute the active edge.
         *
         * Worker 0 additionally carries stable already-global state once.
         * Since the reducer waits for every expected worker, worker 0 is guaranteed
         * to participate before the result is emitted.
         */
        boolean includesStableState = workerId == 0;

        OnePassPhaseOneResult phaseOneResult =
                lifecycle.exportLocalP1ResultForDistMerge(normalizedActiveAlias, activeEdgeId, includesStableState);

        if (phaseOneResult == null) {
            throw new IllegalStateException("Cannot export LOCAL_PHASE1_RESULT because local Phase 1 state is null.");
        }

        payload.put("activeAlias", normalizedActiveAlias);
        payload.put("activeEdgeId", activeEdgeId);

        /*
         * These fields are carried through the reducer into
         * GLOBAL_PHASE1_RESULT.
         *
         * The Step-3 request splitter will later use them to create the
         * transition following GLOBAL_STATE_COMMIT.
         */
        payload.put("nextCommand", nextCommand == null ? "" : nextCommand.trim());

        payload.put("nextAlias", nextAlias == null ? "" : nextAlias.trim());

        /*
         * Current merge representation.
         *
         * The reducer knows that already-global stable edges must only be
         * copied once while the active edge is summed across workers.
         */
        payload.put("phaseOneResult", phaseOneResult.toDebugMap());
        payload.put("includesStableState", includesStableState);

        String json;

        try {
            json = MAPPER.writeValueAsString(payload);
        } catch (Exception exception) {
            throw new IllegalStateException("Could not serialize LOCAL_PHASE1_RESULT", exception);
        }

        String[] param = new String[] {"LOCAL_PHASE1_RESULT", resultId, "PHASE1",
                        normalizedActiveAlias, Integer.toString(workerId), Integer.toString(expectedWorkers),
                        nextCommand == null ? "" : nextCommand.trim(), nextAlias == null ? "" : nextAlias.trim()};

        String reduceKey = uid + "_PHASE1_" + resultId;

        return new Estimation(uid, reduceKey, 72, 30, reduceKey, json, param, expectedWorkers);
    }

    private static String stripOnePassWorkerSuffix(String workerKey, int expectedWorkers, int workerId) {
        if (workerKey == null) {
            return "";
        }

        String suffix = "_" + expectedWorkers + "_KEYED_" + workerId;

        if (workerKey.endsWith(suffix)) {
            return workerKey.substring(0, workerKey.length() - suffix.length());
        }

        return workerKey;
    }

    public Map<String, Object> installGlobalPhaseOneIndex(JsonNode state, String activeAlias) {
        if (state == null || state.isNull()) {
            throw new IllegalArgumentException("Global Phase 1 state must not be null");
        }

        Map<String, Phase1LinkWeightIndex> indexes = new LinkedHashMap<String, Phase1LinkWeightIndex>();

        JsonNode edgeSummaries = state.get("edgeSummaries");

        if (edgeSummaries != null && edgeSummaries.isObject()) {
            Iterator<Map.Entry<String, JsonNode>> fields = edgeSummaries.fields();

            while (fields.hasNext()) {
                String edgeId = fields.next().getKey();
                indexes.put(edgeId, new Phase1LinkWeightIndex(edgeId));
            }
        }

        JsonNode entries = state.get("entries");

        if (entries != null && entries.isArray()) {
            for (JsonNode entry : entries) {
                String edgeId = textField(entry, "edgeId", "");
                String joinKey = textField(entry, "joinKey", "");
                double globalWeight = entry.has("globalWeight")
                        ? entry.get("globalWeight").asDouble(0.0d)
                        : 0.0d;

                if (edgeId == null || edgeId.trim().isEmpty()) {
                    continue;
                }

                if (joinKey == null || joinKey.trim().isEmpty()) {
                    continue;
                }

                Phase1LinkWeightIndex index = indexes.get(edgeId);

                if (index == null) {
                    index = new Phase1LinkWeightIndex(edgeId);
                    indexes.put(edgeId, index);
                }

                index.add(parseJoinValue(joinKey), globalWeight);
            }
        }

        Map<String, Long> seenTuplesByAlias = new LinkedHashMap<String, Long>();

        JsonNode seen = state.get("seenTuplesByAlias");

        if (seen != null && seen.isObject()) {
            Iterator<Map.Entry<String, JsonNode>> fields = seen.fields();

            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> entry = fields.next();
                seenTuplesByAlias.put(entry.getKey(), entry.getValue().asLong(0L));
            }
        }

        OnePassPhaseOneResult globalPhaseOneResult =
                new OnePassPhaseOneResult(plan, indexes, seenTuplesByAlias);

        String resolvedActiveAlias = activeAlias;

        if (resolvedActiveAlias == null || resolvedActiveAlias.trim().isEmpty()) {
            resolvedActiveAlias = textField(state, "activeAlias", "");
        }

        boolean phaseOneComplete = isLastPhaseOneAlias(resolvedActiveAlias);

        lifecycle.installGlobalPhaseOneResult(globalPhaseOneResult, phaseOneComplete);

        Map<String, Object> summary = new LinkedHashMap<String, Object>();

        summary.put("installed", true);
        summary.put("stateRef", textField(state, "stateRef", ""));
        summary.put("entryCount", entries != null && entries.isArray() ? entries.size() : 0);
        summary.put("edgeSummaries", globalPhaseOneResult.getEdgeSummaries(0));
        summary.put("seenTuplesByAlias", new LinkedHashMap<String, Long>(seenTuplesByAlias));
        summary.put("activeAlias", resolvedActiveAlias);
        summary.put("phaseOneComplete", phaseOneComplete);
        summary.put("nextLifecyclePhase", lifecycle.getPhase().name());

        System.out.println("[OnePassSamplerSdeSynopsis] Installed global Phase 1 index: "
                + summary);

        return summary;
    }

    private static JoinValue parseJoinValue(String joinKey) {
        if (joinKey == null) {
            throw new IllegalArgumentException("joinKey must not be null");
        }

        String trimmed = joinKey.trim();

        if (trimmed.indexOf('|') >= 0) {
            return new JoinValue(Arrays.asList(trimmed.split("\\|", -1)));
        }

        return JoinValue.ofSingle(trimmed);
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

    private boolean isLastPhaseOneAlias(String alias) {
        if (alias == null || alias.trim().isEmpty()) {
            return false;
        }

        List<String> order = plan.getLeafToRootOrder();

        if (order == null || order.isEmpty()) {
            return true;
        }

        String last = order.get(order.size() - 1);

        return alias.trim().equals(last);
    }

    /**
     * Distributed Phase-1 entry point. The returned contribution is already fully
     * calculated; only ownership / transfer remains to be decided by SDE.
     */
    public OnePassPhaseOneContribution computePhaseOneContribution(Object payload) {
        String profileKey = profileKeyForPayload(payload);
        long startNanos = System.nanoTime();

        try {
            return lifecycle.computePhaseOneContribution(payload);
        } finally {
            recordAddProfile(profileKey, System.nanoTime() - startNanos);
        }
    }

    public void applyPhaseOneContribution(String edgeId, JoinValue joinKey, double delta) {
        lifecycle.applyPhaseOneContribution(edgeId, joinKey, delta);
    }

    public Map<String, Object> getLocalPhaseOneEdgeSummary(String edgeId) {
        return lifecycle.getLocalPhaseOneEdgeSummary(edgeId);
    }

    public long getLocalPhaseOneSeenTupleCount(String alias) {
        return lifecycle.getLocalPhaseOneSeenTupleCount(alias);
    }

    public Estimation buildLocalPhaseTwoRootSummaryEstimation(
            Request request,
            int workerId,
            int expectedWorkers,
            int actualParallelism,
            String resultId) {

        OnePassPhaseTwoState phaseTwoState = lifecycle.getPhaseTwoState();

        if (phaseTwoState == null) {
            throw new IllegalStateException("Cannot export LOCAL_PHASE2_ROOT_SUMMARY because Phase 2 state is null.");
        }

        Map<String, Object> payload = new LinkedHashMap<String, Object>();

        payload.put("type", "LOCAL_PHASE2_ROOT_SUMMARY");
        payload.put("uid", request.getUID());
        payload.put("workerId", workerId);
        payload.put("expectedWorkers", expectedWorkers);
        payload.put("actualParallelism", actualParallelism);
        payload.put("phase", "PHASE2");
        payload.put("resultId", resultId);
        payload.put("queryName", plan.getQueryName());
        payload.put("rootAlias", plan.getRootAlias());
        payload.put("baseKey", stripOnePassWorkerSuffix(request.getKey(), expectedWorkers, workerId));
        payload.put("datasetSeed", plan.getDatasetSeed());
        payload.put("sampleSize", phaseTwoState.getSampleSize());
        payload.put("rootTuplesSeen", phaseTwoState.getRootTuplesSeen());
        payload.put("positiveRootCandidatesSeen", phaseTwoState.getPositiveRootCandidatesSeen());
        payload.put("totalRootGroupWeight", phaseTwoState.getTotalRootGroupWeight());

        List<Map<String, Object>> reservoir = new ArrayList<Map<String, Object>>();

        for (WeightedReservoirEntry<OnePassRootSampleCandidate> entry : phaseTwoState.getOrderedReservoir()) {
            OnePassRootSampleCandidate candidate = entry.getItem();

            Map<String, Object> candidateJson = new LinkedHashMap<String, Object>();
            candidateJson.put("candidateId", candidate.getCandidateId());
            candidateJson.put("rootAlias", candidate.getRootAlias());
            candidateJson.put("rootTuple", candidate.getRootTuple());
            candidateJson.put("rootGroupWeight", candidate.getRootGroupWeight());
            candidateJson.put("esKey", entry.getKey());
            candidateJson.put("arrivalOrder", entry.getArrivalOrder());
            reservoir.add(candidateJson);
        }

        payload.put("orderedReservoir", reservoir);

        String json;

        try {
            json = MAPPER.writeValueAsString(payload);
        } catch (Exception e) {
            throw new IllegalStateException("Could not serialize LOCAL_PHASE2_ROOT_SUMMARY", e);
        }

        String[] param = new String[] {
                "LOCAL_PHASE2_ROOT_SUMMARY",
                resultId,
                "PHASE2",
                plan.getRootAlias(),
                Integer.toString(workerId),
                Integer.toString(expectedWorkers)
        };

        String reduceKey = request.getUID() + "_PHASE2_" + resultId;

        return new Estimation(
                request.getUID(),
                reduceKey,
                82,
                30,
                reduceKey,
                json,
                param,
                expectedWorkers
        );
    }

    public Map<String, Object> installGlobalPhaseTwoRootSample(JsonNode state) {
        if (state == null || state.isNull()) {
            throw new IllegalArgumentException("Global Phase 2 root sample state must not be null");
        }

        String rootAlias = textField(state, "rootAlias", plan.getRootAlias());
        int sampleSize = intField(state, "sampleSize", 0);
        long rootTuplesSeen = longField(state, "rootTuplesSeen", 0L);
        long positiveRootCandidatesSeen = longField(state, "positiveRootCandidatesSeen", 0L);
        double totalRootGroupWeight = doubleField(state, "totalRootGroupWeight", 0.0d);

        JsonNode samples = state.get("entries");

        if (samples == null || !samples.isArray()) {
            samples = state.get("sampleInstances");
        }

        if (samples == null || !samples.isArray()) {
            throw new IllegalArgumentException("Global Phase 2 root sample state has no entries/sampleInstances array: "
                    + state);
        }

        List<OnePassRootSampleInstance> instances = new ArrayList<OnePassRootSampleInstance>();

        for (int i = 0; i < samples.size(); i++) {
            JsonNode sample = samples.get(i);

            long sampleInstanceId = longField(sample, "sampleInstanceId", i);
            long candidateId = longField(sample, "candidateId", sampleInstanceId);
            JsonNode rootTuple = sample.get("rootTuple");
            double rootGroupWeight = doubleField(sample, "rootGroupWeight", 0.0d);

            if (rootTuple == null || rootTuple.isNull()) {
                throw new IllegalArgumentException("Sample is missing rootTuple: " + sample);
            }

            OnePassRootSampleCandidate candidate = new OnePassRootSampleCandidate(
                    candidateId,
                    rootAlias,
                    rootTuple,
                    rootGroupWeight
            );

            instances.add(new OnePassRootSampleInstance(sampleInstanceId, candidate));
        }

        OnePassRootSampleResult globalResult = new OnePassRootSampleResult(
                rootAlias,
                sampleSize,
                rootTuplesSeen,
                positiveRootCandidatesSeen,
                totalRootGroupWeight,
                instances
        );

        lifecycle.installGlobalPhaseTwoRootSampleResult(globalResult);

        Map<String, Object> summary = new LinkedHashMap<String, Object>();
        summary.put("installed", true);
        summary.put("stateRef", textField(state, "stateRef", ""));
        summary.put("rootAlias", rootAlias);
        summary.put("sampleSize", sampleSize);
        summary.put("sampleInstanceCount", instances.size());
        summary.put("rootTuplesSeen", rootTuplesSeen);
        summary.put("positiveRootCandidatesSeen", positiveRootCandidatesSeen);
        summary.put("totalRootGroupWeight", totalRootGroupWeight);
        summary.put("nextLifecyclePhase", lifecycle.getPhase().name());

        System.out.println("[OnePassSamplerSdeSynopsis] Installed global Phase 2 root sample: " + summary);

        return summary;
    }

    public Estimation buildLocalPhaseThreeAliasResultEstimation(
            Request request,
            int workerId,
            int expectedWorkers,
            int actualParallelism,
            String resultId,
            String alias) {

        Map<String, Object> localChoices = lifecycle.exportPhaseThreeActiveAliasLocalChoices();

        Map<String, Object> payload = new LinkedHashMap<String, Object>();
        String workerKey = request.getKey();
        String baseKey = stripOnePassWorkerSuffix(workerKey, expectedWorkers, workerId);

        payload.put("type", "LOCAL_PHASE3_ALIAS_RESULT");
        payload.put("uid", request.getUID());
        payload.put("workerId", workerId);
        payload.put("expectedWorkers", expectedWorkers);
        payload.put("actualParallelism", actualParallelism);
        payload.put("phase", "PHASE3");
        payload.put("resultId", resultId);
        payload.put("queryName", plan.getQueryName());
        payload.put("rootAlias", plan.getRootAlias());
        payload.put("phaseThreeAlias", alias);
        payload.put("alias", alias);
        payload.put("sampleSize", plan.getSampleSize());
        payload.put("workerKey", workerKey);
        payload.put("baseKey", baseKey);
        payload.putAll(localChoices);

        String json;

        try {
            json = MAPPER.writeValueAsString(payload);
        } catch (Exception e) {
            throw new IllegalStateException("Could not serialize LOCAL_PHASE3_ALIAS_RESULT", e);
        }

        String[] param = new String[] {
                "LOCAL_PHASE3_ALIAS_RESULT",
                resultId,
                alias,
                Integer.toString(workerId),
                Integer.toString(expectedWorkers)
        };

        String reduceKey = request.getUID() + "_PHASE3_ALIAS_" + resultId;

        Estimation out = new Estimation(
                request.getUID(),
                reduceKey,
                92,
                30,
                reduceKey,   // IMPORTANT: this must be reduceKey, not request.getKey()
                json,
                param,
                expectedWorkers
        );

        out.setKey(reduceKey);
        out.setEstimationkey(reduceKey);

        return out;
    }

    public Map<String, Object> installGlobalPhaseThreeAliasSelections(JsonNode state) {
        if (state == null || state.isNull()) {
            throw new IllegalArgumentException("state must not be null");
        }

        String alias = textField(state, "phaseThreeAlias", textField(state, "alias", ""));

        if (alias == null || alias.trim().isEmpty()) {
            throw new IllegalArgumentException("Missing phaseThreeAlias in global selections state: " + state);
        }

        JsonNode entries = state.get("entries");

        if (entries == null || !entries.isArray()) {
            throw new IllegalArgumentException("Missing entries array in global selections state: " + state);
        }

        lifecycle.installGlobalPhaseThreeAliasSelections(alias, entries);

        Map<String, Object> summary = new LinkedHashMap<String, Object>();
        summary.put("type", "INSTALL_PHASE3_ALIAS_SELECTIONS_SUMMARY");
        summary.put("queryName", plan.getQueryName());
        summary.put("rootAlias", plan.getRootAlias());
        summary.put("phaseThreeAlias", alias);
        summary.put("entryCount", entries.size());
        summary.put("phase", lifecycle.getPhase().name());
        summary.put("phaseThreeAliasActive", lifecycle.isPhaseThreeAliasActive());

        return summary;
    }

    private static long longField(JsonNode node, String fieldName, long defaultValue) {
        if (node == null || node.isNull()) return defaultValue;
        JsonNode field = node.get(fieldName);
        if (field == null || field.isNull()) return defaultValue;
        return field.asLong(defaultValue);
    }

    private static double doubleField(JsonNode node, String fieldName, double defaultValue) {
        if (node == null || node.isNull()) return defaultValue;
        JsonNode field = node.get(fieldName);
        if (field == null || field.isNull()) return defaultValue;
        return field.asDouble(defaultValue);
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

    /**
     * DEBUG / VALIDATION ONLY.
     */
    public Map<String, Map<String, Double>> debugCopyPhaseOneRawIndexesForValidator() {
        return lifecycle.debugCopyPhaseOneRawIndexesForValidator();
    }

}