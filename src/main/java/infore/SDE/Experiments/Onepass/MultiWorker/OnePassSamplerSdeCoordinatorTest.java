package infore.SDE.Experiments.Onepass.MultiWorker;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import infore.SDE.messages.Onepass.OnePassParams;
import infore.SDE.transformations.onepass.CompiledOnePassPlan;
import infore.SDE.transformations.onepass.sql.OnePassCatalog;
import infore.SDE.transformations.onepass.sql.OnePassQueryCatalogLoader;
import infore.SDE.transformations.onepass.sql.OnePassSqlCompiler;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

/**
 * Phase-1-only local integration/benchmark test for the sharded OnePass* design.
 *
 * Benchmark semantics:
 *
 *   1) All Phase-1 TPC-H rows are parsed, serialized and written to Kafka
 *      BEFORE the OnePass* algorithm timer starts.
 *
 *   2) Each Phase-1 alias is written in its own open Kafka transaction.
 *      The SDE data consumer must use isolation.level=read_committed.
 *
 *   3) The ADD request is sent after preload.
 *
 *   4) The measured Phase-1 run starts.
 *
 *   5) Alias transactions are committed one by one:
 *
 *          commit(alias E)
 *              -> rows + END_ALIAS(E) become visible
 *              -> workers compute / shard contributions
 *              -> SHARD_BATCH + SOURCE_DONE
 *              -> LOCAL_PHASE1_SHARD_READY x P
 *              -> GLOBAL_PHASE1_ALIAS_READY
 *              -> START_NEXT_ALIAS / START_PHASE_2 on RequestTopic
 *
 *   6) The primary metric is phase1_algorithm_total.
 *
 * IMPORTANT:
 *   This test intentionally stops after Phase 1. The sharded Phase-1 draft
 *   produces START_PHASE_2 but does not activate the old Phase-2 worker logic,
 *   because the old Phase 2 assumes a fully replicated Phase-1 index.
 */
public final class OnePassSamplerSdeCoordinatorTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // ---------------------------------------------------------------------
    // LOCAL TEST SETTINGS
    // ---------------------------------------------------------------------

    private static final String BOOTSTRAP_SERVERS = System.getProperty("onepass.kafka", "localhost:9092");
    private static final String DATA_TOPIC = System.getProperty("onepass.dataTopic", "dataTopic");
    private static final String REQUEST_TOPIC = System.getProperty("onepass.requestTopic", "requestTopic");

    /*
     * The running SDE job still needs its State Topic configured, but this
     * driver does not consume it directly.
     */
    private static final String STATE_TOPIC = System.getProperty("onepass.stateTopic", "onepassStateTopic");

    private static final String TEST_TPCH_DIR =
            System.getProperty("onepass.tpchDir", "/home/vboxuser/Desktop/Thesis/tpch-data/sf1");

    private static final String PHASE1_BENCHMARK_CSV_PATH =
            System.getProperty("onepass.phase1Csv",
                    "/home/vboxuser/Desktop/Thesis/onepass_multiworker_phase1_sharded_local.csv");

    // ---------------------------------------------------------------------
    // TEST CONFIGURATION
    // ---------------------------------------------------------------------

    private static final String TEST_ONEPASS_SQL =
            "SELECT * FROM wq3_alias WEIGHTED BY (" +
                    "o.o_totalprice * (l.l_extendedprice * (1 - l.l_discount))) " +
                    "LIMIT 1000 /* catalog='tpch-onepass-catalog.json', seed='test123', scalefactor=1 */";

    /*
     * Default kept at 1,000,000 to match the uploaded test.
     *
     * For a first smoke test without editing the file:
     *   -Donepass.testRowLimit=10000
     *
     * Use -1 for the full TPC-H relation.
     */
    private static final long TEST_ROW_LIMIT =
            Long.parseLong(System.getProperty("onepass.testRowLimit", "1000000"));

    private static final int EXPECTED_WORKERS =
            Integer.parseInt(System.getProperty("onepass.workers", "4"));

    private static final long TIMEOUT_MS = Long.parseLong(System.getProperty("onepass.timeoutMs",
            Long.toString(30L * 60L * 1000L)));

    /*
     * Kafka transaction timeout.
     *
     * Keep this <= the broker's transaction.max.timeout.ms.
     * Kafka's common default broker maximum is 15 minutes; 10 minutes is used
     * here for the local first pass.
     */
    private static final int TRANSACTION_TIMEOUT_MS =
            Integer.parseInt(System.getProperty("onepass.transactionTimeoutMs", "600000"));

    private static final boolean ENABLE_REQUIRED_FIELD_PRUNING = true;
    private static final boolean WRITE_PHASE1_BENCHMARK_CSV = true;

    private static final int SYNOPSIS_ID = 30;
    private static final int REQUEST_ADD = 1;
    private static final int REQUEST_UPDATE = 7;

    /*
     * Timing maps deliberately contain only algorithm timings.
     * Kafka/TPC-H preload is tracked separately and never added to
     * phase1_algorithm_total.
     */
    private static final Map<String, Long> benchmarkNanos = new LinkedHashMap<String, Long>();

    private static final Map<String, Long> benchmarkCounts = new LinkedHashMap<String, Long>();

    private OnePassSamplerSdeCoordinatorTest() {
    }

    public static void main(String[] args) throws Exception {

        int uid = UUID.randomUUID().toString().hashCode() & 0x7fffffff;

        benchmarkNanos.clear();
        benchmarkCounts.clear();

        String streamId = "onepass-sharded-phase1-local-test";
        String baseKey = "onepass-phase1-" + uid;

        System.out.println("=======================================================");
        System.out.println(" OnePass* SHARDED PHASE 1 - LOCAL TEST");
        System.out.println("=======================================================");
        System.out.println("uid              = " + uid);
        System.out.println("baseKey          = " + baseKey);
        System.out.println("workers          = " + EXPECTED_WORKERS);
        System.out.println("bootstrap        = " + BOOTSTRAP_SERVERS);
        System.out.println("dataTopic        = " + DATA_TOPIC);
        System.out.println("requestTopic     = " + REQUEST_TOPIC);
        System.out.println("stateTopic(SDE)  = " + STATE_TOPIC);
        System.out.println("TPC-H dir        = " + TEST_TPCH_DIR);
        System.out.println("TEST_ROW_LIMIT   = " + TEST_ROW_LIMIT);
        System.out.println("transactionTimeoutMs = " + TRANSACTION_TIMEOUT_MS);
        System.out.println("SQL:");
        System.out.println(TEST_ONEPASS_SQL);
        System.out.println();

        OnePassParams params = OnePassSqlCompiler.compile(TEST_ONEPASS_SQL);
        CompiledOnePassPlan plan = CompiledOnePassPlan.from(params);
        OnePassCatalog catalog =
                OnePassQueryCatalogLoader.load(params.getDataset().getDbConfig());

        validatePlanForShardedPhaseOneV1(plan);

        System.out.println("Compiled plan:");
        System.out.println(plan);
        System.out.println("Root alias: " + plan.getRootAlias());
        System.out.println("Leaf-to-root order: " + plan.getLeafToRootOrder());
        System.out.println("Required fields by alias: "
                + plan.getRequiredFieldsByAlias());
        System.out.println();

        KafkaProducer<String, String> controlProducer = createProducer();

        KafkaConsumer<String, String> feedbackConsumer =
                createObserverConsumer();

        List<PreparedAliasTransaction> preparedPhaseOne =
                new ArrayList<PreparedAliasTransaction>();

        try {
            /*
             * Position the observer BEFORE ADD so no transition generated by
             * this UID can be missed.
             */
            initializeObserver(feedbackConsumer, REQUEST_TOPIC);

            /*
             * -------------------------------------------------------------
             * PRELOAD - OUTSIDE THE ONEPASS* ALGORITHM TIMER
             * -------------------------------------------------------------
             *
             * Rows + END_ALIAS are written into Kafka now, but remain invisible
             * to the read_committed Flink data source until commitTransaction().
             */
            System.out.println();
            System.out.println("Preloading all Phase-1 aliases into Kafka " + "transactions BEFORE starting OnePass*...");
            System.out.println();

            long preloadStartNanos = tic();

            preparedPhaseOne = preparePhaseOneTransactions(
                    uid,
                    baseKey,
                    streamId,
                    catalog,
                    plan
            );

            long preloadNanos = System.nanoTime() - preloadStartNanos;

            System.out.printf(
                    "Phase-1 Kafka preload completed OUTSIDE algorithm timer: %.3f s%n",
                    preloadNanos / 1_000_000_000.0d
            );

            long totalPreparedRows = 0L;

            for (PreparedAliasTransaction prepared : preparedPhaseOne) {
                totalPreparedRows += prepared.rows;

                System.out.println(
                        "  PREPARED alias=" + prepared.alias
                                + ", epoch=" + prepared.epoch
                                + ", rows=" + prepared.rows
                                + ", committed=" + prepared.committed
                );
            }

            if (totalPreparedRows <= 0L) {
                throw new IllegalStateException(
                        "No Phase-1 rows were preloaded."
                );
            }

            /*
             * ADD is setup, not algorithm runtime.
             */
            System.out.println();
            System.out.println(
                    "Sending ADD OnePass request with noOfP="
                            + EXPECTED_WORKERS
                            + "..."
            );

            ObjectNode addRequest = buildOnePassAddRequest(
                    baseKey,
                    streamId,
                    uid,
                    EXPECTED_WORKERS
            );

            sendJson(
                    controlProducer,
                    REQUEST_TOPIC,
                    baseKey,
                    addRequest
            );

            controlProducer.flush();

            /*
             * Temporary existing ADD synchronization.
             * Keep outside the measured Phase-1 runtime.
             */
            Thread.sleep(3000L);

            /*
             * -------------------------------------------------------------
             * ONEPASS* PHASE-1 ALGORITHM TIMER STARTS HERE
             * -------------------------------------------------------------
             */
            System.out.println();
            System.out.println("=======================================================");
            System.out.println(" STARTING MEASURED ONEPASS* PHASE 1");
            System.out.println(" TPC-H parsing + Kafka sends are already complete.");
            System.out.println("=======================================================");
            System.out.println();

            long phaseOneTotalStartNanos = tic();

            for (PreparedAliasTransaction prepared : preparedPhaseOne) {

                String alias = prepared.alias;
                int epoch = prepared.epoch;
                long aliasStartNanos = tic();

                String resultId =
                        "PHASE1_" + alias + "_" + uid;

                boolean last =
                        epoch == plan.getLeafToRootOrder().size();

                String expectedNextCommand =
                        last
                                ? "START_PHASE_2"
                                : "START_NEXT_ALIAS";

                String expectedNextAlias =
                        last
                                ? plan.getRootAlias()
                                : plan.getLeafToRootOrder().get(epoch);

                System.out.println();
                System.out.println("-------------------------------------------------------");
                System.out.println(
                        "Releasing alias=" + alias
                                + ", epoch=" + epoch
                                + ", rows=" + prepared.rows
                );
                System.out.println(
                        "Expected transition: "
                                + expectedNextCommand
                                + " -> "
                                + expectedNextAlias
                );
                System.out.println("-------------------------------------------------------");

                /*
                 * commitTransaction() is intentionally inside the measured
                 * interval. It is the release/start signal.
                 *
                 * All expensive file reading / JSON creation / producer.send()
                 * calls already happened during preload.
                 */
                prepared.producer.commitTransaction();
                prepared.committed = true;

                System.out.println(
                        "Kafka transaction committed. "
                                + alias
                                + " is now visible to the read_committed SDE source."
                );

                JsonNode transition =
                        waitForShardedPhaseOneTransition(
                                feedbackConsumer,
                                uid,
                                epoch,
                                alias,
                                resultId,
                                expectedNextCommand,
                                expectedNextAlias,
                                TIMEOUT_MS
                        );

                long globalSeen =
                        longField(
                                transition,
                                "globalSeenTuples",
                                -1L
                        );

                long globalKeyCount =
                        longField(
                                transition,
                                "globalKeyCount",
                                -1L
                        );

                double globalTotalWeight =
                        doubleField(
                                transition,
                                "globalTotalWeight",
                                0.0d
                        );

                if (globalSeen != prepared.rows) {
                    throw new IllegalStateException(
                            "Phase-1 seen-tuple mismatch for alias="
                                    + alias
                                    + ": expected="
                                    + prepared.rows
                                    + ", globalSeen="
                                    + globalSeen
                                    + ". Transition="
                                    + transition
                    );
                }

                /*
                 * For normal positive-weight WQ3 data, the active shard must
                 * contain at least one key and positive total weight.
                 */
                if (globalKeyCount <= 0L) {
                    throw new IllegalStateException(
                            "Invalid global shard key count after alias="
                                    + alias
                                    + ": "
                                    + globalKeyCount
                                    + ". Transition="
                                    + transition
                    );
                }

                if (globalTotalWeight <= 0.0d) {
                    throw new IllegalStateException(
                            "Invalid global shard total weight after alias="
                                    + alias
                                    + ": "
                                    + globalTotalWeight
                                    + ". Transition="
                                    + transition
                    );
                }

                recordCount(
                        "phase1_rows_processed",
                        prepared.rows
                );

                recordCount(
                        "phase1_alias_" + alias + "_rows_processed",
                        prepared.rows
                );

                recordDuration(
                        "phase1_alias_" + alias + "_algorithm",
                        aliasStartNanos
                );

                System.out.println(
                        "Alias complete: alias="
                                + alias
                                + ", epoch="
                                + epoch
                                + ", globalSeenTuples="
                                + globalSeen
                                + ", globalKeyCount="
                                + globalKeyCount
                                + ", globalTotalWeight="
                                + globalTotalWeight
                );
            }

            recordDuration(
                    "phase1_algorithm_total",
                    phaseOneTotalStartNanos
            );

            /*
             * -------------------------------------------------------------
             * PHASE-1 MEASUREMENT ENDS HERE
             * -------------------------------------------------------------
             */
            System.out.println();
            System.out.println("=======================================================");
            System.out.println(" SHARDED PHASE 1 COMPLETE");
            System.out.println("=======================================================");

            printPhaseOneBenchmarkSummary(
                    plan,
                    preloadNanos
            );

            writePhaseOneBenchmarkCsv(
                    plan,
                    preloadNanos,
                    "SDE_KAFKA_MULTIWORKER_SHARDED_PHASE1_LOCAL"
            );

            System.out.println();
            System.out.println(
                    "SUCCESS: sharded Phase 1 completed locally. "
                            + "START_PHASE_2 was observed, but this test stops here "
                            + "because Phase 2 has not been migrated to sharded Phase-1 state yet."
            );

        } finally {

            try {
                System.out.println();
                System.out.println(
                        "Removing OnePass synopsis uid="
                                + uid
                );

                ObjectNode removeRequest =
                        buildOnePassRemoveRequest(
                                baseKey,
                                streamId,
                                uid,
                                EXPECTED_WORKERS
                        );

                sendJson(
                        controlProducer,
                        REQUEST_TOPIC,
                        baseKey,
                        removeRequest
                );

                controlProducer.flush();

            } catch (Exception cleanupError) {
                System.err.println(
                        "WARNING: OnePass cleanup request failed for uid="
                                + uid
                );
                cleanupError.printStackTrace();
            }

            /*
             * Any transaction that did not reach commitTransaction() must be
             * explicitly aborted before closing.
             */
            for (PreparedAliasTransaction prepared : preparedPhaseOne) {

                if (prepared == null || prepared.producer == null) {
                    continue;
                }

                try {
                    if (!prepared.committed) {
                        System.err.println(
                                "Aborting uncommitted Phase-1 transaction: alias="
                                        + prepared.alias
                                        + ", epoch="
                                        + prepared.epoch
                        );

                        prepared.producer.abortTransaction();
                    }
                } catch (Exception abortError) {
                    System.err.println(
                            "WARNING: could not abort transaction for alias="
                                    + prepared.alias
                    );
                    abortError.printStackTrace();
                }

                try {
                    prepared.producer.close();
                } catch (Exception closeError) {
                    System.err.println(
                            "WARNING: could not close transactional producer for alias="
                                    + prepared.alias
                    );
                    closeError.printStackTrace();
                }
            }

            try {
                feedbackConsumer.close();
            } catch (Exception ignored) {
            }

            try {
                controlProducer.close();
            } catch (Exception ignored) {
            }
        }
    }

    // =====================================================================
    // PHASE-1 PRELOAD
    // =====================================================================

    private static List<PreparedAliasTransaction> preparePhaseOneTransactions(
            int uid,
            String baseKey,
            String streamId,
            OnePassCatalog catalog,
            CompiledOnePassPlan plan) throws Exception {

        List<PreparedAliasTransaction> prepared =
                new ArrayList<PreparedAliasTransaction>();

        int position = 0;

        for (String alias : plan.getLeafToRootOrder()) {

            position++;

            int epoch = position;

            boolean last =
                    position
                            == plan.getLeafToRootOrder().size();

            String resultId =
                    "PHASE1_" + alias + "_" + uid;

            String nextCommand =
                    last
                            ? "START_PHASE_2"
                            : "START_NEXT_ALIAS";

            String nextAlias =
                    last
                            ? plan.getRootAlias()
                            : plan.getLeafToRootOrder().get(position);

            String transactionalId =
                    "onepass-p1-"
                            + uid
                            + "-"
                            + epoch
                            + "-"
                            + alias
                            + "-"
                            + Long.toHexString(System.nanoTime());

            KafkaProducer<String, String> aliasProducer =
                    createTransactionalProducer(
                            transactionalId
                    );

            boolean success = false;

            try {
                System.out.println(
                        "Preparing Kafka transaction for alias="
                                + alias
                                + ", epoch="
                                + epoch
                                + "..."
                );

                long rows =
                        streamAlias(
                                aliasProducer,
                                DATA_TOPIC,
                                baseKey,
                                streamId,
                                catalog,
                                plan,
                                alias,
                                TEST_ROW_LIMIT,
                                plan.getRequiredFieldsByAlias()
                        );

                if (rows <= 0L) {
                    throw new IllegalStateException(
                            "No rows were read for Phase-1 alias "
                                    + alias
                    );
                }

                ObjectNode endAlias =
                        buildEndAliasDatapoint(
                                baseKey,
                                streamId,
                                uid,
                                alias,
                                epoch,
                                resultId,
                                EXPECTED_WORKERS,
                                nextCommand,
                                nextAlias
                        );

                /*
                 * END_ALIAS is in the SAME transaction and uses the SAME Kafka
                 * key as the alias tuples. Therefore it becomes visible only
                 * after all tuple records for this alias.
                 */
                sendJsonAsync(
                        aliasProducer,
                        DATA_TOPIC,
                        baseKey,
                        endAlias
                );

                aliasProducer.flush();

                /*
                 * IMPORTANT: do NOT commit here.
                 * The transaction remains open until the measured algorithm
                 * reaches this alias.
                 */
                prepared.add(
                        new PreparedAliasTransaction(
                                alias,
                                epoch,
                                rows,
                                aliasProducer
                        )
                );

                success = true;

                System.out.println(
                        "Prepared UNCOMMITTED transaction: alias="
                                + alias
                                + ", epoch="
                                + epoch
                                + ", rows="
                                + rows
                );

            } finally {

                /*
                 * If preparation itself fails before ownership of the producer
                 * moves into the prepared list, abort/close immediately.
                 */
                if (!success) {
                    try {
                        aliasProducer.abortTransaction();
                    } catch (Exception ignored) {
                    }

                    try {
                        aliasProducer.close();
                    } catch (Exception ignored) {
                    }
                }
            }
        }

        return prepared;
    }

    private static KafkaProducer<String, String> createTransactionalProducer(
            String transactionalId) {

        Properties props = baseProducerProperties();

        props.put(
                "enable.idempotence",
                "true"
        );

        props.put(
                "transactional.id",
                transactionalId
        );

        props.put(
                "transaction.timeout.ms",
                Integer.toString(
                        TRANSACTION_TIMEOUT_MS
                )
        );

        /*
         * Larger batch than the small control producer because this producer
         * is used to preload relation data.
         */
        props.put(
                "batch.size",
                "65536"
        );

        props.put(
                "linger.ms",
                "5"
        );

        KafkaProducer<String, String> producer =
                new KafkaProducer<String, String>(
                        props
                );

        producer.initTransactions();
        producer.beginTransaction();

        return producer;
    }

    private static final class PreparedAliasTransaction {

        private final String alias;
        private final int epoch;
        private final long rows;
        private final KafkaProducer<String, String> producer;

        private boolean committed;

        private PreparedAliasTransaction(
                String alias,
                int epoch,
                long rows,
                KafkaProducer<String, String> producer) {

            this.alias = alias;
            this.epoch = epoch;
            this.rows = rows;
            this.producer = producer;
            this.committed = false;
        }
    }

    // =====================================================================
    // SHARDED PHASE-1 REQUEST-TOPIC OBSERVER
    // =====================================================================

    private static JsonNode waitForShardedPhaseOneTransition(
            KafkaConsumer<String, String> consumer,
            int uid,
            int completedEpoch,
            String completedAlias,
            String expectedResultId,
            String expectedType,
            String expectedNextAlias,
            long timeoutMs) throws Exception {

        long deadline =
                System.currentTimeMillis()
                        + timeoutMs;

        int recordsSeen = 0;

        while (System.currentTimeMillis() < deadline) {

            ConsumerRecords<String, String> records =
                    consumer.poll(1000L);

            for (ConsumerRecord<String, String> record : records) {

                recordsSeen++;

                String value = record.value();

                if (value == null
                        || value.trim().isEmpty()) {
                    continue;
                }

                JsonNode request;

                try {
                    request = MAPPER.readTree(value);
                } catch (Exception ignored) {
                    continue;
                }

                if (intField(
                        request,
                        "uid",
                        -1
                ) != uid) {
                    continue;
                }

                if (intField(
                        request,
                        "synopsisID",
                        -1
                ) != SYNOPSIS_ID) {
                    continue;
                }

                if (intField(
                        request,
                        "requestID",
                        -1
                ) != REQUEST_UPDATE) {
                    continue;
                }

                JsonNode payload =
                        request.get(
                                "parameters"
                        );

                if (payload == null
                        || !payload.isObject()) {
                    continue;
                }

                if (!"SHARDED_PHASE1_V1".equals(
                        textField(
                                payload,
                                "protocol",
                                ""
                        ))) {
                    continue;
                }

                if (!expectedType.equals(
                        textField(
                                payload,
                                "type",
                                ""
                        ))) {
                    continue;
                }

                if (intField(
                        payload,
                        "completedEpoch",
                        -1
                ) != completedEpoch) {
                    continue;
                }

                if (!completedAlias.equals(
                        textField(
                                payload,
                                "completedAlias",
                                ""
                        ))) {
                    continue;
                }

                if (!expectedResultId.equals(
                        textField(
                                payload,
                                "resultId",
                                ""
                        ))) {
                    continue;
                }

                if (!expectedNextAlias.equals(
                        textField(
                                payload,
                                "nextAlias",
                                ""
                        ))) {
                    continue;
                }

                int nextEpoch =
                        intField(
                                payload,
                                "epoch",
                                -1
                        );

                if (nextEpoch != completedEpoch + 1) {
                    throw new IllegalStateException(
                            "Transition epoch mismatch. completedEpoch="
                                    + completedEpoch
                                    + ", expected next epoch="
                                    + (completedEpoch + 1)
                                    + ", actual="
                                    + nextEpoch
                                    + ". Payload="
                                    + payload
                    );
                }

                int expectedWorkers =
                        intField(
                                payload,
                                "expectedWorkers",
                                -1
                        );

                if (expectedWorkers != EXPECTED_WORKERS) {
                    throw new IllegalStateException(
                            "Transition expectedWorkers mismatch. configured="
                                    + EXPECTED_WORKERS
                                    + ", payload="
                                    + expectedWorkers
                                    + ". Payload="
                                    + payload
                    );
                }

                System.out.println(
                        "Observed sharded Phase-1 transition: "
                                + expectedType
                                + ", completedAlias="
                                + completedAlias
                                + ", completedEpoch="
                                + completedEpoch
                                + ", nextAlias="
                                + expectedNextAlias
                                + ", globalSeenTuples="
                                + longField(
                                payload,
                                "globalSeenTuples",
                                -1L
                        )
                                + ", globalKeyCount="
                                + longField(
                                payload,
                                "globalKeyCount",
                                -1L
                        )
                );

                return payload;
            }
        }

        throw new IllegalStateException(
                "Timed out waiting for sharded Phase-1 transition. "
                        + "uid="
                        + uid
                        + ", epoch="
                        + completedEpoch
                        + ", alias="
                        + completedAlias
                        + ", resultId="
                        + expectedResultId
                        + ", expectedType="
                        + expectedType
                        + ", nextAlias="
                        + expectedNextAlias
                        + ", recordsSeen="
                        + recordsSeen
        );
    }

    // =====================================================================
    // REQUEST BUILDERS
    // =====================================================================

    private static ObjectNode buildOnePassAddRequest(
            String datasetKey,
            String streamId,
            int uid,
            int noOfP) {

        ObjectNode request =
                MAPPER.createObjectNode();

        request.put(
                "dataSetkey",
                datasetKey
        );

        request.put(
                "key",
                datasetKey
        );

        request.put(
                "requestID",
                REQUEST_ADD
        );

        request.put(
                "synopsisID",
                SYNOPSIS_ID
        );

        request.put(
                "uid",
                uid
        );

        request.put(
                "streamID",
                streamId
        );

        request.put(
                "noOfP",
                noOfP
        );

        ArrayNode param =
                MAPPER.createArrayNode();

        param.add(
                "ONEPASS_SQL"
        );

        request.set(
                "param",
                param
        );

        ObjectNode parameters =
                MAPPER.createObjectNode();

        parameters.put(
                "onePassSql",
                TEST_ONEPASS_SQL
        );

        request.set(
                "parameters",
                parameters
        );

        return request;
    }

    private static ObjectNode buildOnePassRemoveRequest(
            String datasetKey,
            String streamId,
            int uid,
            int noOfP) {

        ObjectNode request =
                MAPPER.createObjectNode();

        request.put(
                "dataSetkey",
                datasetKey
        );

        request.put(
                "key",
                datasetKey
        );

        request.put(
                "requestID",
                2
        );

        request.put(
                "synopsisID",
                SYNOPSIS_ID
        );

        request.put(
                "uid",
                uid
        );

        request.put(
                "streamID",
                streamId
        );

        request.put(
                "noOfP",
                noOfP
        );

        ArrayNode param =
                MAPPER.createArrayNode();

        param.add(
                "REMOVE"
        );

        request.set(
                "param",
                param
        );

        return request;
    }

    private static ObjectNode buildEndAliasDatapoint(
            String datasetKey,
            String streamId,
            int uid,
            String alias,
            int epoch,
            String resultId,
            int expectedWorkers,
            String nextCommand,
            String nextAlias) {

        ObjectNode marker =
                MAPPER.createObjectNode();

        marker.put(
                "type",
                "END_ALIAS"
        );

        marker.put(
                "synopsisID",
                SYNOPSIS_ID
        );

        marker.put(
                "uid",
                uid
        );

        marker.put(
                "phase",
                "PHASE1"
        );

        marker.put(
                "alias",
                alias
        );

        marker.put(
                "epoch",
                epoch
        );

        marker.put(
                "resultId",
                resultId
        );

        marker.put(
                "expectedWorkers",
                expectedWorkers
        );

        marker.put(
                "nextCommand",
                nextCommand
        );

        marker.put(
                "nextAlias",
                nextAlias
        );

        ObjectNode datapoint =
                MAPPER.createObjectNode();

        datapoint.put(
                "dataSetkey",
                datasetKey
        );

        datapoint.put(
                "streamID",
                streamId
        );

        datapoint.set(
                "values",
                marker
        );

        return datapoint;
    }

    // =====================================================================
    // TPC-H -> DATAPOINT PRELOAD
    // =====================================================================

    private static long streamAlias(
            KafkaProducer<String, String> producer,
            String topic,
            String datasetKey,
            String streamId,
            OnePassCatalog catalog,
            CompiledOnePassPlan plan,
            String alias,
            long maxRows,
            Map<String, Set<String>> requiredFieldsByAlias) throws Exception {

        File file =
                tableFileForAlias(
                        catalog,
                        plan,
                        alias
                );

        List<String> columns =
                columnsForAlias(
                        catalog,
                        plan,
                        alias
                );

        String separator =
                separatorForAlias(
                        catalog,
                        plan,
                        alias
                );

        Set<String> requiredFields =
                requiredFieldsByAlias == null
                        ? null
                        : requiredFieldsByAlias.get(
                        alias
                );

        if (ENABLE_REQUIRED_FIELD_PRUNING
                && requiredFields == null) {

            throw new IllegalStateException(
                    "Required-field pruning is enabled, "
                            + "but plan has no required fields for alias: "
                            + alias
            );
        }

        System.out.println(
                "  file: "
                        + file.getAbsolutePath()
        );

        System.out.println(
                "  required fields: "
                        + requiredFields
        );

        long count = 0L;

        BufferedReader br =
                new BufferedReader(
                        new FileReader(
                                file
                        )
                );

        try {
            String line;

            while ((line = br.readLine()) != null) {

                if (maxRows >= 0L
                        && count >= maxRows) {
                    break;
                }

                ObjectNode tuple =
                        tupleJsonFromLine(
                                alias,
                                columns,
                                separator,
                                line,
                                requiredFields
                        );

                ObjectNode datapoint =
                        wrapTupleAsDatapoint(
                                datasetKey,
                                streamId,
                                tuple
                        );

                sendJsonAsync(
                        producer,
                        topic,
                        datasetKey,
                        datapoint
                );

                count++;

                if (count % 50000L == 0L) {
                    System.out.println(
                            "    prepared "
                                    + count
                                    + " rows for alias "
                                    + alias
                    );
                }
            }

        } finally {
            br.close();
        }

        return count;
    }

    private static File tableFileForAlias(
            OnePassCatalog catalog,
            CompiledOnePassPlan plan,
            String alias) {

        CompiledOnePassPlan.RelationNode relation =
                plan.getRelation(
                        alias
                );

        if (relation == null) {
            throw new IllegalStateException(
                    "Unknown alias in plan: "
                            + alias
            );
        }

        OnePassCatalog.CatalogTable table =
                catalog.getDataset()
                        .getTables()
                        .get(
                                relation.getTable()
                        );

        if (table == null) {
            throw new IllegalStateException(
                    "Catalog does not define table '"
                            + relation.getTable()
                            + "' for alias '"
                            + alias
                            + "'"
            );
        }

        File file =
                new File(
                        TEST_TPCH_DIR,
                        table.getFile()
                );

        if (!file.exists()) {
            throw new IllegalStateException(
                    "Missing TPC-H file for alias '"
                            + alias
                            + "': "
                            + file.getAbsolutePath()
            );
        }

        return file;
    }

    private static List<String> columnsForAlias(
            OnePassCatalog catalog,
            CompiledOnePassPlan plan,
            String alias) {

        CompiledOnePassPlan.RelationNode relation =
                plan.getRelation(
                        alias
                );

        OnePassCatalog.CatalogTable table =
                catalog.getDataset()
                        .getTables()
                        .get(
                                relation.getTable()
                        );

        List<String> columns =
                table.getColumns();

        if (columns == null
                || columns.isEmpty()) {

            throw new IllegalStateException(
                    "Catalog table '"
                            + relation.getTable()
                            + "' has no columns"
            );
        }

        return columns;
    }

    private static String separatorForAlias(
            OnePassCatalog catalog,
            CompiledOnePassPlan plan,
            String alias) {

        CompiledOnePassPlan.RelationNode relation =
                plan.getRelation(
                        alias
                );

        OnePassCatalog.CatalogTable table =
                catalog.getDataset()
                        .getTables()
                        .get(
                                relation.getTable()
                        );

        String separator =
                table.getSeparator();

        if (separator == null
                || separator.length() == 0) {
            return "|";
        }

        return separator;
    }

    private static ObjectNode tupleJsonFromLine(
            String alias,
            List<String> columns,
            String separator,
            String line,
            Set<String> requiredFields) {

        String[] parts =
                line.split(
                        "\\Q"
                                + separator
                                + "\\E",
                        -1
                );

        ObjectNode tuple =
                MAPPER.createObjectNode();

        tuple.put(
                "alias",
                alias
        );

        int limit =
                Math.min(
                        columns.size(),
                        parts.length
                );

        for (int i = 0;
             i < limit;
             i++) {

            String fieldName =
                    columns.get(
                            i
                    );

            if (ENABLE_REQUIRED_FIELD_PRUNING
                    && requiredFields != null
                    && !requiredFields.contains("*")
                    && !requiredFields.contains(fieldName)) {

                continue;
            }

            putTypedValue(
                    tuple,
                    fieldName,
                    parts[i]
            );
        }

        return tuple;
    }

    private static void putTypedValue(
            ObjectNode tuple,
            String fieldName,
            String rawValue) {

        if (fieldName == null
                || fieldName.trim().isEmpty()) {
            return;
        }

        if (rawValue == null) {
            tuple.put(
                    fieldName,
                    ""
            );
            return;
        }

        String value =
                rawValue.trim();

        if (value.length() == 0) {
            tuple.put(
                    fieldName,
                    ""
            );
            return;
        }

        Long asLong =
                tryParseLong(
                        value
                );

        if (asLong != null) {
            tuple.put(
                    fieldName,
                    asLong.longValue()
            );
            return;
        }

        Double asDouble =
                tryParseDouble(
                        value
                );

        if (asDouble != null) {
            tuple.put(
                    fieldName,
                    asDouble.doubleValue()
            );
            return;
        }

        tuple.put(
                fieldName,
                value
        );
    }

    private static Long tryParseLong(
            String value) {

        try {
            if (value.indexOf('.') >= 0) {
                return null;
            }

            return Long.valueOf(
                    Long.parseLong(
                            value
                    )
            );

        } catch (Exception e) {
            return null;
        }
    }

    private static Double tryParseDouble(
            String value) {

        try {
            return Double.valueOf(
                    Double.parseDouble(
                            value
                    )
            );

        } catch (Exception e) {
            return null;
        }
    }

    private static ObjectNode wrapTupleAsDatapoint(
            String datasetKey,
            String streamId,
            ObjectNode tuple) {

        ObjectNode datapoint =
                MAPPER.createObjectNode();

        datapoint.put(
                "dataSetkey",
                datasetKey
        );

        datapoint.put(
                "streamID",
                streamId
        );

        datapoint.set(
                "values",
                tuple.deepCopy()
        );

        return datapoint;
    }

    // =====================================================================
    // KAFKA
    // =====================================================================

    private static Properties baseProducerProperties() {

        Properties props =
                new Properties();

        props.put(
                "bootstrap.servers",
                BOOTSTRAP_SERVERS
        );

        props.put(
                "acks",
                "all"
        );

        props.put(
                "retries",
                "3"
        );

        props.put(
                "buffer.memory",
                "268435456"
        );

        props.put(
                "max.request.size",
                "104857600"
        );

        props.put(
                "delivery.timeout.ms",
                "900000"
        );

        props.put(
                "request.timeout.ms",
                "300000"
        );

        props.put(
                "key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer"
        );

        props.put(
                "value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer"
        );

        return props;
    }

    private static KafkaProducer<String, String> createProducer() {

        Properties props =
                baseProducerProperties();

        props.put(
                "batch.size",
                "16384"
        );

        props.put(
                "linger.ms",
                "1"
        );

        return new KafkaProducer<String, String>(
                props
        );
    }

    private static KafkaConsumer<String, String> createObserverConsumer() {

        Properties props =
                new Properties();

        props.put(
                "bootstrap.servers",
                BOOTSTRAP_SERVERS
        );

        /*
         * This consumer is a passive observer with explicit partition
         * assignment. It does not participate in the SDE consumer group.
         */
        props.put(
                "enable.auto.commit",
                "false"
        );

        props.put(
                "auto.offset.reset",
                "latest"
        );

        props.put(
                "request.timeout.ms",
                "300000"
        );

        props.put(
                "fetch.max.bytes",
                "104857600"
        );

        props.put(
                "max.partition.fetch.bytes",
                "104857600"
        );

        props.put(
                "key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer"
        );

        props.put(
                "value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer"
        );

        return new KafkaConsumer<String, String>(
                props
        );
    }

    private static void initializeObserver(
            KafkaConsumer<String, String> consumer,
            String topic) {

        List<PartitionInfo> partitionInfos =
                consumer.partitionsFor(
                        topic
                );

        if (partitionInfos == null
                || partitionInfos.isEmpty()) {

            throw new IllegalStateException(
                    "Kafka topic has no discoverable partitions: "
                            + topic
            );
        }

        List<TopicPartition> partitions =
                new ArrayList<TopicPartition>();

        for (PartitionInfo info : partitionInfos) {
            partitions.add(
                    new TopicPartition(
                            topic,
                            info.partition()
                    )
            );
        }

        consumer.assign(
                partitions
        );

        /*
         * Start after everything already in RequestTopic so the observer sees
         * only feedback produced by this new UID/run.
         */
        consumer.seekToEnd(
                partitions
        );

        System.out.println(
                "RequestTopic observer READY on "
                        + topic
                        + ", partitions="
                        + partitions
        );
    }

    private static void sendJsonAsync(
            KafkaProducer<String, String> producer,
            String topic,
            String key,
            JsonNode json) {

        producer.send(
                new ProducerRecord<String, String>(
                        topic,
                        key,
                        json.toString()
                )
        );
    }

    private static void sendJson(
            KafkaProducer<String, String> producer,
            String topic,
            String key,
            JsonNode json) throws Exception {

        producer.send(
                new ProducerRecord<String, String>(
                        topic,
                        key,
                        json.toString()
                )
        ).get();
    }

    // =====================================================================
    // PLAN VALIDATION
    // =====================================================================

    private static void validatePlanForShardedPhaseOneV1(
            CompiledOnePassPlan plan) {

        if (plan == null) {
            throw new IllegalArgumentException(
                    "Compiled plan must not be null"
            );
        }

        if (plan.getLeafToRootOrder() == null
                || plan.getLeafToRootOrder().isEmpty()) {

            throw new IllegalStateException(
                    "Compiled plan has empty leafToRootOrder: "
                            + plan
            );
        }

        /*
         * Phase-1 v1 currently supports chains / aliases with at most one
         * child continuation lookup. Fail before Kafka preload if the query
         * requires the branching-tree extension.
         */
        for (String alias : plan.getLeafToRootOrder()) {

            int childEdgeCount =
                    plan.getChildEdges(
                            alias
                    ).size();

            if (childEdgeCount > 1) {
                throw new UnsupportedOperationException(
                        "Sharded Phase 1 v1 supports at most one child edge "
                                + "per Phase-1 alias. alias="
                                + alias
                                + ", childEdges="
                                + childEdgeCount
                );
            }

            if (plan.getParentEdge(alias) == null) {
                throw new IllegalStateException(
                        "Phase-1 alias has no parent edge: "
                                + alias
                );
            }
        }
    }

    // =====================================================================
    // BENCHMARK
    // =====================================================================

    private static long tic() {
        return System.nanoTime();
    }

    private static void recordDuration(
            String label,
            long startNanos) {

        long elapsed =
                System.nanoTime()
                        - startNanos;

        Long current =
                benchmarkNanos.get(
                        label
                );

        benchmarkNanos.put(
                label,
                current == null
                        ? elapsed
                        : current.longValue()
                        + elapsed
        );
    }

    private static void recordCount(
            String label,
            long value) {

        Long current =
                benchmarkCounts.get(
                        label
                );

        benchmarkCounts.put(
                label,
                current == null
                        ? value
                        : current.longValue()
                        + value
        );
    }

    private static long nanosFor(
            String label) {

        Long value =
                benchmarkNanos.get(
                        label
                );

        return value == null
                ? 0L
                : value.longValue();
    }

    private static double secondsFor(
            String label) {

        return nanosFor(
                label
        ) / 1_000_000_000.0d;
    }

    private static long countFor(
            String label) {

        Long value =
                benchmarkCounts.get(
                        label
                );

        return value == null
                ? 0L
                : value.longValue();
    }

    private static double rowsPerSecond(
            long rows,
            double seconds) {

        if (seconds <= 0.0d) {
            return 0.0d;
        }

        return rows / seconds;
    }

    private static void printPhaseOneBenchmarkSummary(
            CompiledOnePassPlan plan,
            long preloadNanos) {

        double preloadSeconds =
                preloadNanos
                        / 1_000_000_000.0d;

        double algorithmSeconds =
                secondsFor(
                        "phase1_algorithm_total"
                );

        long rows =
                countFor(
                        "phase1_rows_processed"
                );

        System.out.println();
        System.out.println(
                "=== Sharded OnePass* Phase 1 benchmark ==="
        );

        System.out.printf(
                "%-42s %12.3f s  [OUTSIDE TIMER]%n",
                "phase1_kafka_preload",
                preloadSeconds
        );

        System.out.printf(
                "%-42s %12.3f s%n",
                "phase1_algorithm_total",
                algorithmSeconds
        );

        System.out.printf(
                "%-42s %12d%n",
                "phase1_rows_processed",
                rows
        );

        System.out.printf(
                "%-42s %12.3f rows/s%n",
                "phase1_algorithm_rows_per_sec",
                rowsPerSecond(
                        rows,
                        algorithmSeconds
                )
        );

        System.out.println();
        System.out.println(
                "Per-alias algorithm timings:"
        );

        for (String alias : plan.getLeafToRootOrder()) {

            long aliasRows =
                    countFor(
                            "phase1_alias_"
                                    + alias
                                    + "_rows_processed"
                    );

            double aliasAlgorithm =
                    secondsFor(
                            "phase1_alias_"
                                    + alias
                                    + "_algorithm"
                    );

            System.out.println(
                    "  alias="
                            + alias
                            + ", rows="
                            + aliasRows
                            + ", algorithm_s="
                            + aliasAlgorithm
                            + ", rows_per_s="
                            + rowsPerSecond(
                            aliasRows,
                            aliasAlgorithm
                    )
            );
        }

        System.out.println(
                "==========================================="
        );
        System.out.println();
    }

    private static void writePhaseOneBenchmarkCsv(
            CompiledOnePassPlan plan,
            long preloadNanos,
            String implementation) throws Exception {

        if (!WRITE_PHASE1_BENCHMARK_CSV) {
            return;
        }

        File csvFile =
                new File(
                        PHASE1_BENCHMARK_CSV_PATH
                );

        File parent =
                csvFile.getParentFile();

        if (parent != null
                && !parent.exists()) {

            /*
             * Do not fail simply because the local results directory has not
             * been created yet.
             */
            parent.mkdirs();
        }

        boolean writeHeader =
                !csvFile.exists()
                        || csvFile.length() == 0L;

        double preloadSeconds =
                preloadNanos
                        / 1_000_000_000.0d;

        double algorithmSeconds =
                secondsFor(
                        "phase1_algorithm_total"
                );

        long rows =
                countFor(
                        "phase1_rows_processed"
                );

        FileWriter writer =
                new FileWriter(
                        csvFile,
                        true
                );

        try {

            if (writeHeader) {
                writer.write(
                        "timestamp_ms,"
                                + "implementation,"
                                + "workers,"
                                + "query_name,"
                                + "seed,"
                                + "test_row_limit,"
                                + "sample_size_limit,"
                                + "root_alias,"
                                + "leaf_to_root_order,"
                                + "phase1_rows_processed,"
                                + "phase1_kafka_preload_s,"
                                + "phase1_algorithm_total_s,"
                                + "phase1_algorithm_rows_per_sec,"
                                + "phase1_alias_rows_processed,"
                                + "phase1_alias_algorithm_s"
                                + System.lineSeparator()
                );
            }

            writer.write(
                    Long.toString(
                            System.currentTimeMillis()
                    )
                            + ","
                            + csv(
                            implementation
                    )
                            + ","
                            + EXPECTED_WORKERS
                            + ","
                            + csv(
                            plan.getQueryName()
                    )
                            + ","
                            + csv(
                            plan.getDatasetSeed()
                    )
                            + ","
                            + csv(
                            formatRowLimit(
                                    TEST_ROW_LIMIT
                            )
                    )
                            + ","
                            + plan.getSampleSize()
                            + ","
                            + csv(
                            plan.getRootAlias()
                    )
                            + ","
                            + csv(
                            String.valueOf(
                                    plan.getLeafToRootOrder()
                            )
                    )
                            + ","
                            + rows
                            + ","
                            + preloadSeconds
                            + ","
                            + algorithmSeconds
                            + ","
                            + rowsPerSecond(
                            rows,
                            algorithmSeconds
                    )
                            + ","
                            + csv(
                            String.valueOf(
                                    phaseOneAliasRowsMap(
                                            plan
                                    )
                            )
                    )
                            + ","
                            + csv(
                            String.valueOf(
                                    phaseOneAliasAlgorithmSecondsMap(
                                            plan
                                    )
                            )
                    )
                            + System.lineSeparator()
            );

        } finally {
            writer.close();
        }

        System.out.println(
                "Phase-1 benchmark CSV appended to: "
                        + csvFile.getAbsolutePath()
        );
    }

    private static Map<String, Long> phaseOneAliasRowsMap(
            CompiledOnePassPlan plan) {

        Map<String, Long> out =
                new LinkedHashMap<String, Long>();

        for (String alias : plan.getLeafToRootOrder()) {

            out.put(
                    alias,
                    countFor(
                            "phase1_alias_"
                                    + alias
                                    + "_rows_processed"
                    )
            );
        }

        return out;
    }

    private static Map<String, Double> phaseOneAliasAlgorithmSecondsMap(
            CompiledOnePassPlan plan) {

        Map<String, Double> out =
                new LinkedHashMap<String, Double>();

        for (String alias : plan.getLeafToRootOrder()) {

            out.put(
                    alias,
                    secondsFor(
                            "phase1_alias_"
                                    + alias
                                    + "_algorithm"
                    )
            );
        }

        return out;
    }

    private static String csv(
            String value) {

        if (value == null) {
            return "";
        }

        String escaped =
                value.replace(
                        "\"",
                        "\"\""
                );

        return "\""
                + escaped
                + "\"";
    }

    private static String formatRowLimit(
            long rowLimit) {

        return rowLimit < 0L
                ? "FULL"
                : Long.toString(
                rowLimit
        );
    }

    // =====================================================================
    // JSON HELPERS
    // =====================================================================

    private static String textField(
            JsonNode node,
            String fieldName,
            String defaultValue) {

        if (node == null
                || node.isNull()) {
            return defaultValue;
        }

        JsonNode field =
                node.get(
                        fieldName
                );

        if (field == null
                || field.isNull()) {
            return defaultValue;
        }

        String value =
                field.asText();

        if (value == null
                || value.trim().isEmpty()) {
            return defaultValue;
        }

        return value.trim();
    }

    private static int intField(
            JsonNode node,
            String fieldName,
            int defaultValue) {

        if (node == null
                || node.isNull()) {
            return defaultValue;
        }

        JsonNode field =
                node.get(
                        fieldName
                );

        if (field == null
                || field.isNull()) {
            return defaultValue;
        }

        return field.asInt(
                defaultValue
        );
    }

    private static long longField(
            JsonNode node,
            String fieldName,
            long defaultValue) {

        if (node == null
                || node.isNull()) {
            return defaultValue;
        }

        JsonNode field =
                node.get(
                        fieldName
                );

        if (field == null
                || field.isNull()) {
            return defaultValue;
        }

        return field.asLong(
                defaultValue
        );
    }

    private static double doubleField(
            JsonNode node,
            String fieldName,
            double defaultValue) {

        if (node == null
                || node.isNull()) {
            return defaultValue;
        }

        JsonNode field =
                node.get(
                        fieldName
                );

        if (field == null
                || field.isNull()) {
            return defaultValue;
        }

        return field.asDouble(
                defaultValue
        );
    }
}
