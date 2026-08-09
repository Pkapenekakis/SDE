package infore.SDE.transformations.onepass.worker;

import com.fasterxml.jackson.databind.JsonNode;

import java.io.Serializable;
import java.util.*;

/**
 * Worker-local bounded tuple gate for OnePass.
 *
 * The gate tracks one currently allowed alias per OnePass UID.
 *
 * Tuples belonging to another alias are retained until that alias becomes
 * active through START_NEXT_ALIAS / START_PHASE_2.
 *
 * This class is intentionally OnePass-specific and contains no generic
 * SDE synopsis logic.
 */
public final class OnePassTupleBufferGate implements Serializable {

    private static final long serialVersionUID = 1L;
    private final int maxBufferedTuplesPerUid;
    private final Map<Integer, String> allowedAliasByUid = new HashMap<Integer, String>();
    private final Map<Integer, LinkedHashMap<String, ArrayDeque<JsonNode>>>
            bufferedByUidAndAlias = new HashMap<Integer, LinkedHashMap<String, ArrayDeque<JsonNode>>>();

    private final Map<Integer, Integer> bufferedCountByUid = new HashMap<Integer, Integer>();
    private final Map<Integer, Set<String>> sealedAliasesByUid = new HashMap<Integer, Set<String>>();

    public OnePassTupleBufferGate(int maxBufferedTuplesPerUid) {

        if (maxBufferedTuplesPerUid <= 0) {
            throw new IllegalArgumentException("maxBufferedTuplesPerUid must be greater than zero");
        }

        this.maxBufferedTuplesPerUid = maxBufferedTuplesPerUid;
    }

    public void registerIfAbsent(int uid, String initialAlias) {

        requireAlias(initialAlias);

        if (!allowedAliasByUid.containsKey(uid)) {
            allowedAliasByUid.put(uid, initialAlias.trim());
        }
    }

    public boolean isRegistered(int uid) {

        return allowedAliasByUid.containsKey(uid);
    }

    public String getAllowedAlias(
            int uid) {

        return allowedAliasByUid.get(uid);
    }

    public boolean isAllowed(int uid, String alias) {

        requireAlias(alias);

        String allowed = allowedAliasByUid.get(uid);
        String normalizedAlias = alias.trim();

        return allowed != null && allowed.equals(normalizedAlias) && !isSealed(uid, normalizedAlias);
    }

    public void buffer(int uid, String alias, JsonNode payload) {

        requireAlias(alias);

        if (isSealed(uid, alias)) {
            throw new IllegalStateException("Received tuple for sealed OnePass alias " + alias + ", uid=" + uid +
                    ". The tuple arrived after END_ALIAS.");
        }

        if (payload == null || payload.isNull()) {

            throw new IllegalArgumentException("Cannot buffer a null OnePass tuple payload");
        }

        int currentCount = getBufferedCount(uid);

        if (currentCount >= maxBufferedTuplesPerUid) {
            throw new IllegalStateException(
                    "OnePass tuple buffer limit exceeded for uid=" + uid + ". buffered=" + currentCount + ", max="
                            + maxBufferedTuplesPerUid + ", incomingAlias=" + alias + ", allowedAlias=" + getAllowedAlias(uid)
                            + ". This indicates excessive phase overlap or " + "missing feedback/transition progress.");
        }

        LinkedHashMap<String, ArrayDeque<JsonNode>> byAlias = bufferedByUidAndAlias.get(uid);

        if (byAlias == null) {
            byAlias = new LinkedHashMap<String, ArrayDeque<JsonNode>>();

            bufferedByUidAndAlias.put(uid, byAlias);
        }

        String normalizedAlias = alias.trim();
        ArrayDeque<JsonNode> queue = byAlias.get(normalizedAlias);

        if (queue == null) {
            queue = new ArrayDeque<JsonNode>();
            byAlias.put(normalizedAlias, queue);
        }

        queue.addLast(payload.deepCopy()
        );

        bufferedCountByUid.put(uid, currentCount + 1);
    }

    public List<JsonNode> activateAliasAndDrain(int uid, String alias) {

        requireAlias(alias);

        String normalizedAlias = alias.trim();

        allowedAliasByUid.put(uid, normalizedAlias);

        List<JsonNode> released = new ArrayList<JsonNode>();

        LinkedHashMap<String, ArrayDeque<JsonNode>> byAlias = bufferedByUidAndAlias.get(uid);

        if (byAlias == null) {
            return released;
        }

        ArrayDeque<JsonNode> queue = byAlias.remove(normalizedAlias);

        if (queue == null) {
            return released;
        }

        while (!queue.isEmpty()) {
            released.add(queue.removeFirst());
        }

        int remaining = getBufferedCount(uid) - released.size();

        if (remaining <= 0) {
            bufferedCountByUid.remove(uid);
        } else {
            bufferedCountByUid.put(uid, remaining
            );
        }

        if (byAlias.isEmpty()) {
            bufferedByUidAndAlias.remove(uid);
        }

        return released;
    }

    public int getBufferedCount(int uid) {

        Integer count = bufferedCountByUid.get(uid);
        return count == null ? 0 : count.intValue();
    }

    public int getBufferedCount(int uid, String alias) {

        requireAlias(alias);
        LinkedHashMap<String, ArrayDeque<JsonNode>> byAlias = bufferedByUidAndAlias.get(uid);

        if (byAlias == null) {
            return 0;
        }

        ArrayDeque<JsonNode> queue = byAlias.get(alias.trim());

        return queue == null ? 0 : queue.size();
    }

    private static void requireAlias(String alias) {

        if (alias == null || alias.trim().isEmpty()) {
            throw new IllegalArgumentException("OnePass alias must not be blank");
        }
    }

    public boolean isSealed(int uid, String alias) {

        requireAlias(alias);
        Set<String> sealed = sealedAliasesByUid.get(uid);

        return sealed != null && sealed.contains(alias.trim());
    }

    /**
     * Permanently closes the current alias for this worker.
     * After this call, another tuple for this alias is considered a protocol ordering error.
     */
    public void sealAlias(int uid, String alias) {

        requireAlias(alias);

        String normalizedAlias = alias.trim();
        String currentAlias = allowedAliasByUid.get(uid);

        if (!normalizedAlias.equals(currentAlias)) {

            throw new IllegalStateException("Cannot seal OnePass alias " + normalizedAlias + " for uid=" + uid +
                    " because current allowed alias is " + currentAlias);
        }

        Set<String> sealed = sealedAliasesByUid.get(uid);

        if (sealed == null) {
            sealed = new HashSet<String>();
            sealedAliasesByUid.put(uid, sealed);
        }

        sealed.add(normalizedAlias);
    }

    public int getMaxBufferedTuplesPerUid() {
        return maxBufferedTuplesPerUid;
    }

    public void clear(int uid) {

        allowedAliasByUid.remove(uid);
        bufferedByUidAndAlias.remove(uid);
        bufferedCountByUid.remove(uid);
        sealedAliasesByUid.remove(uid);
    }
}