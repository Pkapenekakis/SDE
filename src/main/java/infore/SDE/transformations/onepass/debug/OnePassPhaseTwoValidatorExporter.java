package infore.SDE.transformations.onepass.debug;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import infore.SDE.synopses.OnePassSampler.OnePassSamplerSdeSynopsis;
import infore.SDE.synopses.OnePassSampler.PhaseTwo.OnePassRootSampleChecksum;
import infore.SDE.synopses.OnePassSampler.PhaseTwo.OnePassRootSampleResult;

import java.io.File;

/**
 * DEBUG / VALIDATION ONLY.
 *
 * Reads the Phase-2 result that is ALREADY installed on one worker and writes
 * a deterministic validation artifact.
 *
 * This class performs no OnePass computation, no sampling, no routing, no
 * reduction, and no state transfer.
 */
public final class OnePassPhaseTwoValidatorExporter {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final boolean ENABLED =
            Boolean.parseBoolean(System.getProperty("sde.onepass.debug.validatePhase2RootSample", "false"));
    private static final String OUTPUT_DIRECTORY =
            System.getProperty("sde.onepass.debug.phase2ValidationDir", "/tmp/onepass-phase2-validator");

    private OnePassPhaseTwoValidatorExporter() {
    }

    public static boolean isEnabled() {
        return ENABLED;
    }

    public static String getOutputDirectory() {
        return OUTPUT_DIRECTORY;
    }

    public static File exportInstalledRootSample(OnePassSamplerSdeSynopsis onePass, int uid, int workerId, int expectedWorkers, String outputDirectory) throws Exception {
        if (onePass == null) {
            throw new IllegalArgumentException("onePass must not be null");
        }

        OnePassRootSampleResult result = onePass.getLifecycle().getPhaseTwoResult();

        if (result == null) {
            throw new IllegalStateException("Cannot validate Phase-2 root sample because worker " +
                    workerId + " has no installed Phase-2 result.");
        }

        String checksum = OnePassRootSampleChecksum.sha256(result);
        File runDirectory = new File(outputDirectory, "uid-" + uid);

        if (!runDirectory.exists() && !runDirectory.mkdirs()) {
            throw new IllegalStateException("Could not create Phase-2 validator directory: " + runDirectory.getAbsolutePath());
        }

        ObjectNode output = MAPPER.createObjectNode();
        output.put("type", "ONEPASS_PHASE2_WORKER_ROOT_SAMPLE_VALIDATION");
        output.put("uid", uid);
        output.put("workerId", workerId);
        output.put("expectedWorkers", expectedWorkers);
        output.put("rootAlias", result.getRootAlias());
        output.put("requestedSampleSize", result.getRequestedSampleSize());
        output.put("sampleInstanceCount", result.getSampleInstances().size());
        output.put("rootTuplesSeen", result.getRootTuplesSeen());
        output.put("positiveRootCandidatesSeen", result.getPositiveRootCandidatesSeen());
        output.put("totalRootGroupWeight", result.getTotalRootGroupWeight());
        output.put("checksumVersion", OnePassRootSampleChecksum.VERSION);
        output.put("checksum", checksum);


        File file = new File(runDirectory, "worker-" + workerId + ".json");
        MAPPER.writerWithDefaultPrettyPrinter().writeValue(file, output);

        System.out.println("[OnePass PHASE2 DEBUG VALIDATOR EXPORT]" + " uid=" + uid + ", worker=" + workerId +
                ", checksum=" + checksum + ", file=" + file.getAbsolutePath());


        return file;
    }
}