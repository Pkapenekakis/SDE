package infore.SDE.transformations.onepass;

import com.fasterxml.jackson.databind.JsonNode;
import infore.SDE.messages.Datapoint;
import infore.SDE.messages.Request;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * OnePass-specific data router for parallel experiments.
 *
 * Purpose:
 *   - keep one logical input key, e.g. onepass-phase1-123
 *   - route normal tuples round-robin to:
 *       onepass-phase1-123_2_KEYED_0
 *       onepass-phase1-123_2_KEYED_1
 *   - broadcast data barriers to all worker keys
 *
 * Requests are not emitted here.
 * RqRouterFlatMap is still responsible for duplicating ADD / UPDATE requests.
 */
public final class RoundRobinDataRouterCoFlatMap extends RichCoFlatMapFunction<Datapoint, Request, Datapoint> {

    private static final long serialVersionUID = 1L;
    private static final String ONEPASS_DATA_BARRIER_FIELD = "__onePassDataBarrier";
    private final Map<String, Integer> parallelismByBaseKey = new HashMap<String, Integer>();
    private final Map<String, Integer> nextWorkerByBaseKey = new HashMap<String, Integer>();

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

        if (isOnePassDataBarrier(value)) {
            for (int worker = 0; worker < p; worker++) {
                out.collect(copyWithKey(value, routedKey(baseKey, p, worker)));
            }

            return;
        }

        int nextWorker = nextWorkerByBaseKey.containsKey(baseKey) ? nextWorkerByBaseKey.get(baseKey) : 0;

        out.collect(copyWithKey(value, routedKey(baseKey, p, nextWorker)));

        nextWorker++;
        if (nextWorker >= p) {
            nextWorker = 0;
        }

        nextWorkerByBaseKey.put(baseKey, nextWorker);
    }

    @Override
    public void flatMap2(Request request, Collector<Datapoint> out) throws Exception {
        if (request == null) {
            return;
        }

        if (request.getSynopsisID() != 30) {
            return;
        }

        /*
         * Register parallelism for OnePass ADD.
         *
         * The original request key is the logical base key.
         * RqRouterFlatMap will separately create the worker-specific requests.
         */
        if (request.getRequestID() == 1 && request.getNoOfP() > 1) {
            String baseKey = request.getKey();

            parallelismByBaseKey.put(baseKey, request.getNoOfP());

            if (!nextWorkerByBaseKey.containsKey(baseKey)) {
                nextWorkerByBaseKey.put(baseKey, 0);
            }

            System.out.println("[OnePassRoundRobinRouter] Registered baseKey=" + baseKey +
                    ", parallelism=" + request.getNoOfP());

            return;
        }

        /*
         * Optional cleanup for remove request.
         */
        if (request.getRequestID() == 2) {
            String baseKey = request.getKey();

            parallelismByBaseKey.remove(baseKey);
            nextWorkerByBaseKey.remove(baseKey);

            System.out.println("[OnePassRoundRobinRouter] Removed baseKey=" + baseKey);
        }
    }

    private static Datapoint copyWithKey(Datapoint source, String newKey) {
        return new Datapoint(newKey, source.getStreamID(), source.getValues());
    }

    private static String routedKey(String baseKey, int parallelism, int workerId) {
        return baseKey + "_" + parallelism + "_KEYED_" + workerId;
    }

    private static boolean isOnePassDataBarrier(Datapoint value) {
        if (value == null || value.getValues() == null || value.getValues().isNull()) {
            return false;
        }

        JsonNode marker = value.getValues().get(ONEPASS_DATA_BARRIER_FIELD);
        return marker != null && marker.asBoolean(false);
    }
}