package infore.SDE.transformations.onepass.debug;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import infore.SDE.synopses.OnePassSampler.OnePassSamplerSdeSynopsis;

import java.io.File;
import java.util.Map;

/**
 * DEBUG / VALIDATION ONLY.
 *
 * Dumps the exact Phase-1 indexes stored by one worker.
 *
 * This class performs no OnePass computation, ownership calculation,
 * state transfer, reduction, or merging.
 */
public final class OnePassPhaseOneValidatorExporter {

    private static final ObjectMapper MAPPER =
            new ObjectMapper();

    private static final boolean ENABLED =
            Boolean.parseBoolean(System.getProperty("sde.onepass.debug.exportPhase1Indexes", "false"));

    private static final String OUTPUT_DIRECTORY =
            System.getProperty("sde.onepass.debug.phase1IndexDir", "/tmp/onepass-phase1-validator");

    private OnePassPhaseOneValidatorExporter() {
    }

    public static boolean isEnabled() {
        return ENABLED;
    }

    public static String getOutputDirectory() {
        return OUTPUT_DIRECTORY;
    }

    public static File exportWorkerShard(OnePassSamplerSdeSynopsis onePass, int uid, int workerId, int expectedWorkers,
                                         String outputDirectory) throws Exception {

        Map<String, Map<String, Double>> indexes = onePass.debugCopyPhaseOneRawIndexesForValidator();

        File runDirectory = new File(outputDirectory, "uid-" + uid);

        if (!runDirectory.exists() && !runDirectory.mkdirs()) {

            throw new IllegalStateException("Could not create Phase-1 validator directory: " + runDirectory.getAbsolutePath());
        }

        ObjectNode output = MAPPER.createObjectNode();

        output.put("type", "ONEPASS_PHASE1_WORKER_SHARD");
        output.put("uid", uid);
        output.put("workerId", workerId);
        output.put("expectedWorkers", expectedWorkers);
        output.set("edgeIndexes", MAPPER.valueToTree(indexes));

        File file = new File(runDirectory, "worker-" + workerId + ".json");

        MAPPER.writerWithDefaultPrettyPrinter().writeValue(file, output);

        System.out.println("[OnePass DEBUG VALIDATOR EXPORT]" + " uid=" + uid + ", worker=" + workerId +
                ", file=" + file.getAbsolutePath());

        return file;
    }
}