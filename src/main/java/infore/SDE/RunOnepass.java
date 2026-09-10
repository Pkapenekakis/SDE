package infore.SDE;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import infore.SDE.messages.Datapoint;
import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;
import infore.SDE.sources.kafkaProducerEstimation;
import infore.SDE.sources.kafkaStringConsumer;
import infore.SDE.sources.kafkaStringProducer;
import infore.SDE.transformations.GReduceFlatMap;
import infore.SDE.transformations.ReduceFlatMap;
import infore.SDE.transformations.RqRouterFlatMap;
import infore.SDE.transformations.SDEcoFlatMap;
import infore.SDE.transformations.onepass.*;
import infore.SDE.transformations.onepass.coordinator.OnePassWorkerPartitioner;
import infore.SDE.transformations.onepass.worker.PhaseOne.OnePassPhaseOneEnrichmentBuffer;
import infore.SDE.transformations.onepass.worker.PhaseThree.OnePassPhaseThreeEnrichmentBuffer;
import infore.SDE.transformations.onepass.worker.PhaseTwo.OnePassPhaseTwoEnrichmentBuffer;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * SDE runtime with the coordinator-free OnePass StateTopic architecture.
 */
public class RunOnepass {

    private static final int ONEPASS_SYNOPSIS_ID = 30;

    private static String kafkaDataInputTopic;
    private static String kafkaRequestInputTopic;
    private static String kafkaBrokersList;
    private static int parallelism;
    private static String kafkaOutputTopic;
    private static String kafkaOnePassStateTopic;

    private static OnePassDataRouterCoFlatMap.RoutingMode onePassRoutingMode =
            OnePassDataRouterCoFlatMap.RoutingMode.JOIN_KEY_HASH;

    public static void main(String[] args) throws Exception {
        initializeParameters(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        kafkaStringConsumer dataConsumer = new kafkaStringConsumer(kafkaBrokersList, kafkaDataInputTopic, true);
        kafkaStringConsumer requestConsumer = new kafkaStringConsumer(kafkaBrokersList, kafkaRequestInputTopic);
        kafkaStringConsumer onePassStateConsumer = new kafkaStringConsumer(kafkaBrokersList, kafkaOnePassStateTopic);
        kafkaProducerEstimation estimationProducer = new kafkaProducerEstimation(kafkaBrokersList, kafkaOutputTopic);

        //RequestTopic feedback is used for stateless OnePass lifecycle
        kafkaProducerEstimation requestFeedbackProducer = new kafkaProducerEstimation(kafkaBrokersList, kafkaRequestInputTopic);
        kafkaStringProducer onePassStateProducer = new kafkaStringProducer(kafkaBrokersList, kafkaOnePassStateTopic);

        DataStream<String> kafkaDataStream = env.addSource(dataConsumer.getFc());
        DataStream<String> kafkaRequestStream = env.addSource(requestConsumer.getFc());
        DataStream<String> kafkaOnePassStateStream = env.addSource(onePassStateConsumer.getFc());

        // ================================================================
        // NORMAL DATA SOURCE
        // ================================================================
        DataStream<Datapoint> parsedDataStream = kafkaDataStream.map(new MapFunction<String, Datapoint>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Datapoint map(String node) throws IOException {
                ObjectMapper objectMapper = new ObjectMapper();
                return objectMapper.readValue(node, Datapoint.class);
            }
        }).name("DATA_SOURCE").keyBy((KeySelector<Datapoint, String>) Datapoint::getKey);

        // ================================================================
        // REQUEST SOURCE
        // ================================================================

        DataStream<Request> parsedRequestStream = kafkaRequestStream.map(new MapFunction<String, Request>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Request map(String node) throws IOException {
                ObjectMapper objectMapper = new ObjectMapper();
                return objectMapper.readValue(node, Request.class);
            }
        }).name("REQUEST_SOURCE").keyBy((KeySelector<Request, String>) Request::getKey);
        DataStream<Request> synopsisRequests = parsedRequestStream.flatMap(new RqRouterFlatMap()).name("REQUEST_ROUTER");

        // ================================================================
        // ONEPASS STATE TOPIC SOURCE
        // ================================================================
        /*
         * Targeted request-78 messages already contain workerKey.
         *
         * Global Phase-2 sample chunks are written only once to Kafka and
         * OnePassStateTopicParser fans them out to the physical workers
         * after Kafka.
         */
        DataStream<Datapoint> onePassStateTopicDataStream = kafkaOnePassStateStream.
                flatMap(new OnePassStateTopicParser()).name("ONEPASS_STATE_TOPIC_PARSER");

        // ================================================================
        // ONEPASS-AWARE DATA ROUTING
        // ================================================================
        DataStream<Datapoint> routedDataStream = parsedDataStream.connect(parsedRequestStream).
                flatMap(new OnePassDataRouterCoFlatMap(onePassRoutingMode)).name("ONEPASS_AWARE_DATA_ROUTER");

        //StateTopic records enter exactly the same physical worker path as ordinary routed data.
        DataStream<Datapoint> dataStreamWithState = routedDataStream.union(onePassStateTopicDataStream);

        /*
         * IMPORTANT:
         * Do not keyBy again after partitionCustom.
         *
         * _KEYED_0, _KEYED_1, ... must reach their intended physical
         * Flink subtasks.
         */
        DataStream<Datapoint> partitionedDataStream = dataStreamWithState.
                partitionCustom(new OnePassWorkerPartitioner(), (KeySelector<Datapoint, String>) Datapoint::getKey);

        DataStream<Request> partitionedSynopsisRequests = synopsisRequests.
                partitionCustom(new OnePassWorkerPartitioner(), (KeySelector<Request, String>) Request::getKey);

        // ================================================================
        // SYNOPSIS MAINTENANCE
        // ================================================================
        DataStream<Estimation> estimationStream = partitionedDataStream.
                connect(partitionedSynopsisRequests).flatMap(new SDEcoFlatMap()).name("SYNOPSES_MAINTENANCE");

        // ================================================================
        // STATE TOPIC WORK - REQUEST 78
        // ================================================================
        DataStream<Estimation> onePassStateTransferStream = estimationStream.filter(new FilterFunction<Estimation>() {
            private static final long serialVersionUID = 1L;

            @Override
            public boolean filter(Estimation value) {
                return isOnePassStateTransferMessage(value);
            }
        }).name("ONEPASS_STATE_TRANSFER_BRANCH");

        // ================================================================
        // NORMAL REDUCE INPUT
        // ================================================================
        /*
         * request 78 is StateTopic traffic and must not reach ReduceFlatMap.
         * IMPORTANT:
         *
         * request 85 is NOT filtered here.
         * LOCAL_PHASE2_ROOT_SAMPLE_INSTALLED must enter ReduceFlatMap so
         * P local installation-ready messages become request 86.
         */
        DataStream<Estimation> normalEstimationStream = estimationStream.filter(new FilterFunction<Estimation>() {
            private static final long serialVersionUID = 1L;

            @Override
            public boolean filter(Estimation value) {
                return !isOnePassControlAck(value) && !isOnePassStateTransferMessage(value);
            }
        }).name("NORMAL_ESTIMATION_STREAM");

        //Preserve existing SDE single-vs-multi behavior.
        SplitStream<Estimation> split = normalEstimationStream.split(new OutputSelector<Estimation>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Iterable<String> select(Estimation value) {
                List<String> output = new ArrayList<String>();
                output.add(value.getNoOfP() == 1 ? "single" : "multy");

                return output;
            }
        });

        DataStream<Estimation> single = split.select("single");
        DataStream<Estimation> multy = split.select("multy").
                keyBy((KeySelector<Estimation, String>) Estimation::getKey);

        single.addSink(estimationProducer.getProducer()).name("SINGLE_ESTIMATION_OUTPUT");

        /*
         * The normal federated merge point.
         *
         * OnePass currently uses:
         *
         * 76 -> LOCAL_PHASE1_SHARD_READY
         * 82 -> LOCAL_PHASE2_ROOT_SUMMARY
         * 85 -> LOCAL_PHASE2_ROOT_SAMPLE_INSTALLED
         */
        DataStream<Estimation> partialOutputStream = multy.flatMap(new ReduceFlatMap()).name("REDUCE");
        // ================================================================
        // PHASE 1 GLOBAL READINESS - REQUEST 77
        // ================================================================

        DataStream<Estimation> onePassPhaseOneAliasReady = partialOutputStream.filter(new FilterFunction<Estimation>() {
            private static final long serialVersionUID = 1L;

            @Override
            public boolean filter(Estimation value) {
                return value != null &&
                        value.getSynopsisID() == ONEPASS_SYNOPSIS_ID &&
                        value.getRequestID() == 77 &&
                        "GLOBAL_PHASE1_ALIAS_READY".equals(firstParam(value));
            }
        }).name("ONEPASS_PHASE1_ALIAS_READY");

        /*
         * Stateless transition:
         * GLOBAL_PHASE1_ALIAS_READY  -> START_NEXT_ALIAS / START_PHASE_2
         */
        DataStream<Estimation> onePassPhaseOneTransitions = onePassPhaseOneAliasReady.
                flatMap(new OnePassPhaseOneTransitionMapper()).name("ONEPASS_PHASE1_TRANSITION_MAPPER").setParallelism(1);

        // ================================================================
        // PHASE 2 GLOBAL ROOT SAMPLE - REQUEST 83
        // ================================================================
        DataStream<Estimation> onePassGlobalPhaseTwoRootSamples = partialOutputStream.filter(new FilterFunction<Estimation>() {
            private static final long serialVersionUID = 1L;

            @Override
            public boolean filter(Estimation value) {
                return value != null &&
                        value.getSynopsisID() == ONEPASS_SYNOPSIS_ID &&
                        value.getRequestID() == 83 &&
                        "GLOBAL_PHASE2_ROOT_SAMPLE".equals(firstParam(value));
            }
        }).name("ONEPASS_GLOBAL_PHASE2_ROOT_SAMPLE");

        // ================================================================
        // PHASE 2 GLOBAL INSTALL READY - REQUEST 86
        // ================================================================
        DataStream<Estimation> onePassGlobalPhaseTwoInstalled = partialOutputStream.filter(new FilterFunction<Estimation>() {
            private static final long serialVersionUID = 1L;

            @Override
            public boolean filter(Estimation value) {
                return value != null &&
                        value.getSynopsisID() == ONEPASS_SYNOPSIS_ID &&
                        value.getRequestID() == 86 &&
                        "GLOBAL_PHASE2_ROOT_SAMPLE_INSTALLED".equals(firstParam(value));
            }
        }).name("ONEPASS_GLOBAL_PHASE2_ROOT_SAMPLE_INSTALLED");

        // ================================================================
        // PHASE 3 GLOBAL ALIAS SELECTIONS - REQUEST 88
        // ================================================================
        DataStream<Estimation> onePassGlobalPhaseThreeAliasSelections =
                partialOutputStream.filter(new FilterFunction<Estimation>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public boolean filter(Estimation value) {
                        return value != null &&
                                value.getSynopsisID() == ONEPASS_SYNOPSIS_ID &&
                                value.getRequestID() == 88 &&
                                "GLOBAL_PHASE3_ALIAS_SELECTIONS".equals(firstParam(value));
                    }
                }).name("ONEPASS_GLOBAL_PHASE3_ALIAS_SELECTIONS");

        // ================================================================
        // PHASE 3 GLOBAL INSTALL READY - REQUEST 91
        // ================================================================
        DataStream<Estimation> onePassGlobalPhaseThreeInstalled =
                partialOutputStream.filter(new FilterFunction<Estimation>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public boolean filter(Estimation value) {
                        return value != null &&
                                value.getSynopsisID() == ONEPASS_SYNOPSIS_ID &&
                                value.getRequestID() == 91 &&
                                "GLOBAL_PHASE3_ALIAS_SELECTIONS_INSTALLED".equals(firstParam(value));
                    }
                }).name("ONEPASS_GLOBAL_PHASE3_ALIAS_SELECTIONS_INSTALLED");

        // ================================================================
        // UNIFIED STATE TOPIC OUTPUT
        // ================================================================

        /*
         * request 78: targeted sharded computation/state work.
         *
         * request 83: bounded global root sample.
         *
         * OnePassStateTopicEmitter understands both.
         */
        DataStream<Estimation> onePassStateTopicFeedback = onePassStateTransferStream.
                union(onePassGlobalPhaseTwoRootSamples, onePassGlobalPhaseThreeAliasSelections);

        onePassStateTopicFeedback.flatMap(new OnePassStateTopicEmitter()).name("ONEPASS_STATE_TOPIC_EMITTER").
                addSink(onePassStateProducer.getProducer()).name("ONEPASS_STATE_TOPIC_OUTPUT");

        /*
         * Stateless Phase-3 transitions:
         *   request 86 -> START_PHASE_3_ALIAS(first)
         *   request 91 -> START_PHASE_3_ALIAS(next), unless final.
         */
        DataStream<Estimation> onePassPhaseThreeTransitions = onePassGlobalPhaseTwoInstalled.
                union(onePassGlobalPhaseThreeInstalled).flatMap(new OnePassPhaseThreeTransitionMapper()).
                name("ONEPASS_PHASE3_TRANSITION_MAPPER").setParallelism(1);

        // ================================================================
        // REQUEST TOPIC LIFECYCLE FEEDBACK
        // ================================================================

        /*
         * For the current Phase-1 + Phase-2 implementation the automatic
         * RequestTopic feedback is only:
         *
         * request 77 -> START_NEXT_ALIAS / START_PHASE_2
         *
         * When sharded Phase 3 is implemented, its transition should begin from request 86.
         */
        onePassPhaseOneTransitions.union(onePassPhaseThreeTransitions).addSink(requestFeedbackProducer.getProducer()).
                name("ONEPASS_REQUEST_TOPIC_FEEDBACK").setParallelism(1);

        // ================================================================
        // PHASE 2 COMPLETION OUTPUT
        // ================================================================

        /*
         * request 86 means every worker has installed the complete global
         * Phase-2 root sample.
         *
         * Until sharded Phase 3 exists, this is the clean Phase-2 endpoint.
         */
        onePassGlobalPhaseTwoInstalled.addSink(estimationProducer.getProducer()).name("ONEPASS_PHASE2_INSTALLED_OUTPUT");

        // ================================================================
        // PHASE 3 COMPLETION OUTPUT
        // ================================================================
        DataStream<Estimation> onePassPhaseThreeComplete =
                onePassGlobalPhaseThreeInstalled.filter(
                        new FilterFunction<Estimation>() {
                            private static final long serialVersionUID = 1L;

                            @Override
                            public boolean filter(Estimation value) {
                                return isPhaseThreeComplete(value);
                            }
                        }).name("ONEPASS_PHASE3_COMPLETE");

        onePassPhaseThreeComplete
                .addSink(estimationProducer.getProducer())
                .name("ONEPASS_PHASE3_COMPLETE_OUTPUT");

        // ================================================================
        // GENERIC SDE GLOBAL REDUCE
        // ================================================================
        /*
         * 77, 83 and 86 are already globally reduced and have dedicated
         * OnePass branches above.
         *
         * They should not go through GReduceFlatMap.
         */
        DataStream<Estimation> genericPartialOutputStream = partialOutputStream.filter(new FilterFunction<Estimation>() {
            private static final long serialVersionUID = 1L;
            @Override
            public boolean filter(Estimation value) {
                return !isOnePassInternalReducedFeedback(value);
            }
        }).name("GENERIC_PARTIAL_OUTPUT");

        DataStream<Estimation> finalStream = genericPartialOutputStream.flatMap(new GReduceFlatMap()).
                name("GLOBAL_REDUCE").setParallelism(1);

        finalStream.addSink(estimationProducer.getProducer()).name("FINAL_STREAM_EXTERNAL_OUTPUT");

        env.execute("Streaming SDE");
    }

    /**
     * request 78 StateTopic traffic.
     */
    private static boolean isOnePassStateTransferMessage(Estimation value) {

        if (value == null || value.getSynopsisID() != ONEPASS_SYNOPSIS_ID || value.getRequestID() != 78) {
            return false;
        }

        String type = firstParam(value);
        return "SHARD_BATCH".equals(type) ||
                "SOURCE_DONE".equals(type) ||
                OnePassPhaseOneEnrichmentBuffer.TYPE_ENRICH_BATCH.equals(type) ||
                OnePassPhaseOneEnrichmentBuffer.TYPE_ENRICH_SOURCE_DONE.equals(type) ||
                OnePassPhaseTwoEnrichmentBuffer.TYPE_ROOT_ENRICH_BATCH.equals(type) ||
                OnePassPhaseTwoEnrichmentBuffer.TYPE_ROOT_ENRICH_SOURCE_DONE.equals(type) ||
                OnePassPhaseThreeEnrichmentBuffer.TYPE_ENRICH_BATCH.equals(type) ||
                OnePassPhaseThreeEnrichmentBuffer.TYPE_ENRICH_SOURCE_DONE.equals(type);
    }

    /**
     * Temporary compatibility filter for the remaining worker-local
     * request-7 status messages.
     * <p>
     * request 85 is deliberately NOT included.
     */
    private static boolean isOnePassControlAck(Estimation value) {
        if (value == null || value.getSynopsisID() != ONEPASS_SYNOPSIS_ID || value.getRequestID() != 7) {
            return false;
        }

        String type = firstParam(value);
        return "FINISH_PHASE_1".equals(type) ||
                "FINISH_PHASE_2".equals(type) ||
                "START_PHASE_3_ALIAS".equals(type) ||
                "FINISH_PHASE_3_ALIAS".equals(type) ||
                "FINISH_PHASE_3".equals(type) ||
                "STATUS".equals(type);
    }

    /**
     * OnePass results that are already globally reduced and have their own
     * feedback/output path.
     */
    private static boolean isOnePassInternalReducedFeedback(Estimation value) {
        if (value == null || value.getSynopsisID() != ONEPASS_SYNOPSIS_ID) {
            return false;
        }

        String type = firstParam(value);

        if (value.getRequestID() == 77 && "GLOBAL_PHASE1_ALIAS_READY".equals(type)) {
            return true;
        }

        if (value.getRequestID() == 83 && "GLOBAL_PHASE2_ROOT_SAMPLE".equals(type)) {
            return true;
        }

        if (value.getRequestID() == 86 && "GLOBAL_PHASE2_ROOT_SAMPLE_INSTALLED".equals(type)) {
            return true;
        }

        if (value.getRequestID() == 88 && "GLOBAL_PHASE3_ALIAS_SELECTIONS".equals(type)) {
            return true;
        }

        return value.getRequestID() == 91
                && "GLOBAL_PHASE3_ALIAS_SELECTIONS_INSTALLED".equals(type);
    }

    private static boolean isPhaseThreeComplete(Estimation value) {
        if (value == null || value.getEstimation() == null) {
            return false;
        }

        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode payload;
            if (value.getEstimation() instanceof JsonNode) {
                payload = (JsonNode) value.getEstimation();
            } else if (value.getEstimation() instanceof String) {
                payload = mapper.readTree((String) value.getEstimation());
            } else {
                payload = mapper.valueToTree(value.getEstimation());
            }

            JsonNode complete = payload.get("phaseThreeComplete");
            return complete != null && !complete.isNull() && complete.asBoolean(false);
        } catch (Exception e) {
            throw new IllegalStateException("Could not parse GLOBAL_PHASE3_ALIAS_SELECTIONS_INSTALLED payload", e);
        }
    }

    private static String firstParam(Estimation value) {

        if (value == null) {
            return "";
        }

        String[] param = value.getParam();

        if (param == null || param.length == 0 || param[0] == null) {

            return "";
        }

        return param[0].trim();
    }

    private static void initializeParameters(String[] args) {

        if (args.length > 4) {

            System.out.println("[INFO] User Defined program arguments");
            kafkaDataInputTopic = args[0];
            kafkaRequestInputTopic = args[1];
            kafkaOutputTopic = args[2];
            kafkaBrokersList = args[3];
            parallelism = Integer.parseInt(args[4]);
            kafkaOnePassStateTopic = args.length > 5 ? args[5] : "onepassStateTopic";

            //JOIN_KEY_HASH is the correct fallback for the current distributed OnePass design.
            onePassRoutingMode = args.length > 6 ? OnePassDataRouterCoFlatMap.RoutingMode.fromString(args[6]) :
                    OnePassDataRouterCoFlatMap.RoutingMode.JOIN_KEY_HASH;

        } else {

            System.out.println("[INFO] Default values");
            kafkaDataInputTopic = "dataTopic";
            kafkaRequestInputTopic = "requestTopic";
            kafkaOutputTopic = "estimationTopic";
            kafkaBrokersList = "localhost:9092";
            parallelism = 4;
            kafkaOnePassStateTopic = "onepassStateTopic";
            onePassRoutingMode = OnePassDataRouterCoFlatMap.RoutingMode.JOIN_KEY_HASH;
        }

//        System.out.println("[INFO] dataTopic=" + kafkaDataInputTopic);
//        System.out.println("[INFO] requestTopic=" + kafkaRequestInputTopic);
//        System.out.println("[INFO] outputTopic=" + kafkaOutputTopic);
//        System.out.println("[INFO] onePassStateTopic=" + kafkaOnePassStateTopic);
//        System.out.println("[INFO] brokers=" + kafkaBrokersList);
//        System.out.println("[INFO] parallelism=" + parallelism);
//        System.out.println("[INFO] onePassRoutingMode=" + onePassRoutingMode);
    }
}