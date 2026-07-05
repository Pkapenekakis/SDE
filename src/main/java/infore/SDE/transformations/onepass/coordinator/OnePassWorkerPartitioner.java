package infore.SDE.transformations.onepass.coordinator;

import org.apache.flink.api.common.functions.Partitioner;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Sends OnePass routed keys to the intended Flink worker.
 *
 * Expected key format:
 *
 *   baseKey_2_KEYED_0
 *   baseKey_2_KEYED_1
 *
 * The final number is interpreted as the target partition.
 */
public final class OnePassWorkerPartitioner implements Partitioner<String> {

    private static final long serialVersionUID = 1L;

    private static final Pattern ROUTED_KEY_PATTERN =
            Pattern.compile(".*_([0-9]+)_(KEYED|RANDOM)_([0-9]+)$");

    @Override
    public int partition(String key, int numPartitions) {
        if (numPartitions <= 0) {
            return 0;
        }

        if (key != null) {
            Matcher matcher = ROUTED_KEY_PATTERN.matcher(key);

            if (matcher.matches()) {
                int workerId = Integer.parseInt(matcher.group(3));
                return Math.floorMod(workerId, numPartitions);
            }

            return Math.floorMod(key.hashCode(), numPartitions);
        }

        return 0;
    }
}