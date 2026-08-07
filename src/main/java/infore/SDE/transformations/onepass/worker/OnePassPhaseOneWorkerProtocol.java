package infore.SDE.transformations.onepass.worker;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Worker-local state machine for the OnePass Phase 1 feedback protocol.
 *
 * This class:
 *
 *   - assembles BEGIN / CHUNK / COMMIT messages;
 *   - validates the reconstructed state;
 *   - remembers ready and installed states;
 *   - gates START_NEXT_ALIAS / START_PHASE_2 on requiredStateRef.
 *
 * It does not:
 *
 *   - install state into OnePassSamplerSdeSynopsis;
 *   - emit Estimation messages;
 *   - access Kafka;
 *   - affect non-OnePass synopses.
 */
public final class OnePassPhaseOneWorkerProtocol
        implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final ObjectMapper MAPPER =
            new ObjectMapper();

    public static final String TYPE_GLOBAL_STATE_BEGIN = "GLOBAL_STATE_BEGIN";
    public static final String TYPE_GLOBAL_STATE_CHUNK = "GLOBAL_STATE_CHUNK";
    public static final String TYPE_GLOBAL_STATE_COMMIT = "GLOBAL_STATE_COMMIT";
    public static final String COMMAND_START_NEXT_ALIAS = "START_NEXT_ALIAS";
    public static final String COMMAND_START_PHASE_2 = "START_PHASE_2";
    public static final String STATE_TYPE_GLOBAL_PHASE1_INDEX = "GLOBAL_PHASE1_INDEX";

    private final Map<String, StateAssembly> assembliesByRef = new HashMap<String, StateAssembly>();
    private final Map<String, JsonNode> readyStatesByRef = new HashMap<String, JsonNode>();
    private final Map<Integer, String> installedStateRefByUid = new HashMap<Integer, String>();
    private final Map<Integer, Transition> pendingTransitionByUid = new HashMap<Integer, Transition>();
    private final Map<Integer, Transition> activeTransitionByUid = new HashMap<Integer, Transition>();

    /**
     * Accepts BEGIN, CHUNK, or COMMIT.
     *
     * @return a complete validated state when this message completes an
     *         assembly; otherwise null.
     */
    public JsonNode acceptStateMessage(JsonNode payload) {
        requirePayload(payload);

        String type = textField(payload, "type", "");

        if (!TYPE_GLOBAL_STATE_BEGIN.equals(type) && !TYPE_GLOBAL_STATE_CHUNK.equals(type)
                && !TYPE_GLOBAL_STATE_COMMIT.equals(type)) {

            throw new IllegalArgumentException("Unsupported OnePass state message type: " + type);
        }

        String stateType = textField(payload, "stateType", "");

        if (!STATE_TYPE_GLOBAL_PHASE1_INDEX.equals(stateType)) {
            throw new IllegalArgumentException("OnePassPhaseOneWorkerProtocol only supports stateType="
                            + STATE_TYPE_GLOBAL_PHASE1_INDEX + ", received=" + stateType);
        }

        String stateRef = requiredTextField(payload, "stateRef");

        if (readyStatesByRef.containsKey(stateRef)) {
            // Duplicate delivery after successful assembly is idempotent.
            return null;
        }

        StateAssembly assembly = assembliesByRef.get(stateRef);

        if (assembly == null) {
            assembly = new StateAssembly(stateRef);

            assembliesByRef.put(stateRef, assembly);
        }

        if (TYPE_GLOBAL_STATE_BEGIN.equals(type)) {
            assembly.acceptBegin(payload);
        } else if (TYPE_GLOBAL_STATE_CHUNK.equals(type)) {
            assembly.acceptChunk(payload);
        } else {
            assembly.acceptCommit(payload);
        }

        JsonNode complete = assembly.tryAssemble();

        if (complete != null) {
            assembliesByRef.remove(stateRef);

            readyStatesByRef.put(
                    stateRef,
                    complete
            );

            return complete;
        }

        return null;
    }

    /**
     * Returns a complete but not-yet-installed state for a UID.
     *
     * This is used when state messages arrive before the OnePass synopsis
     * has been created locally.
     */
    public JsonNode getReadyStateForUid(int uid) {
        for (JsonNode state : readyStatesByRef.values()) {
            if (intField(state, "uid", -1) == uid) {
                return state;
            }
        }

        return null;
    }

    /**
     * Marks a previously assembled state as installed.
     *
     * If a matching transition was already received, that transition
     * becomes active immediately.
     */
    public void markInstalled(int uid, String stateRef) {

        if (stateRef == null
                || stateRef.trim().isEmpty()) {

            throw new IllegalArgumentException(
                    "stateRef must not be blank"
            );
        }

        installedStateRefByUid.put(
                uid,
                stateRef
        );

        readyStatesByRef.remove(stateRef);

        Transition pending =
                pendingTransitionByUid.get(uid);

        if (pending != null
                && stateRef.equals(
                pending.getRequiredStateRef()
        )) {

            pendingTransitionByUid.remove(uid);

            activeTransitionByUid.put(
                    uid,
                    pending
            );
        }
    }

    /**
     * Accepts START_NEXT_ALIAS or START_PHASE_2.
     *
     * @return true when the transition is activated immediately;
     *         false when it is stored pending installation.
     */
    public boolean acceptTransition(
            JsonNode payload) {

        requirePayload(payload);

        String command =
                textField(payload, "type", "");

        if (!COMMAND_START_NEXT_ALIAS.equals(command)
                && !COMMAND_START_PHASE_2.equals(command)) {

            throw new IllegalArgumentException(
                    "Unsupported Phase 1 transition command: "
                            + command
            );
        }

        int uid =
                intField(payload, "uid", -1);

        if (uid < 0) {
            throw new IllegalArgumentException(
                    "Transition is missing a valid uid: "
                            + payload
            );
        }

        String requiredStateRef =
                requiredTextField(
                        payload,
                        "requiredStateRef"
                );

        String nextAlias =
                requiredTextField(
                        payload,
                        "nextAlias"
                );

        Transition transition =
                new Transition(
                        uid,
                        command,
                        nextAlias,
                        requiredStateRef
                );

        String installedStateRef =
                installedStateRefByUid.get(uid);

        if (requiredStateRef.equals(installedStateRef)) {
            pendingTransitionByUid.remove(uid);

            activeTransitionByUid.put(
                    uid,
                    transition
            );

            return true;
        }

        pendingTransitionByUid.put(
                uid,
                transition
        );

        return false;
    }

    public boolean isStateInstalled(
            int uid,
            String stateRef) {

        if (stateRef == null) {
            return false;
        }

        return stateRef.equals(
                installedStateRefByUid.get(uid)
        );
    }

    public String getInstalledStateRef(int uid) {

        return installedStateRefByUid.get(uid);
    }

    public Transition getPendingTransition(int uid) {

        return pendingTransitionByUid.get(uid);
    }

    public Transition getActiveTransition(int uid) {

        return activeTransitionByUid.get(uid);
    }

    /**
     * Removes and returns the currently activated transition for this UID.
     *
     * SDEcoFlatMap consumes the transition exactly once when it changes the
     * tuple gate to the new alias and releases buffered tuples.
     */
    public Transition consumeActiveTransition(int uid) {
        return activeTransitionByUid.remove(uid);
    }

    public boolean hasActiveTransition(int uid) {

        return activeTransitionByUid.containsKey(uid);
    }

    private static void requirePayload(JsonNode payload) {

        if (payload == null || payload.isNull() || !payload.isObject()) {

            throw new IllegalArgumentException("OnePass protocol payload must be a JSON object");
        }
    }

    private static String requiredTextField(JsonNode node, String fieldName) {

        String value = textField(node, fieldName, "");

        if (value == null || value.trim().isEmpty()) {

            throw new IllegalArgumentException("Missing required field '" + fieldName + "' in payload " + node);
        }

        return value;
    }

    private static String textField(
            JsonNode node,
            String fieldName,
            String defaultValue) {

        if (node == null || node.isNull()) {
            return defaultValue;
        }

        JsonNode field =
                node.get(fieldName);

        if (field == null || field.isNull()) {
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

        if (node == null || node.isNull()) {
            return defaultValue;
        }

        JsonNode field =
                node.get(fieldName);

        if (field == null || field.isNull()) {
            return defaultValue;
        }

        return field.asInt(defaultValue);
    }

    private static String sha256Hex(
            String value) {

        try {
            MessageDigest digest =
                    MessageDigest.getInstance(
                            "SHA-256"
                    );

            byte[] hash =
                    digest.digest(
                            value.getBytes(
                                    StandardCharsets.UTF_8
                            )
                    );

            StringBuilder out =
                    new StringBuilder(
                            hash.length * 2
                    );

            for (byte b : hash) {
                out.append(
                        String.format(
                                "%02x",
                                b & 0xff
                        )
                );
            }

            return out.toString();

        } catch (Exception exception) {
            throw new IllegalStateException(
                    "Could not calculate SHA-256 checksum",
                    exception
            );
        }
    }

    /** Immutable activated/pending transition metadata. */
    public static final class Transition
            implements Serializable {

        private static final long serialVersionUID = 1L;

        private final int uid;
        private final String command;
        private final String nextAlias;
        private final String requiredStateRef;

        private Transition(
                int uid,
                String command,
                String nextAlias,
                String requiredStateRef) {

            this.uid = uid;
            this.command = command;
            this.nextAlias = nextAlias;
            this.requiredStateRef =
                    requiredStateRef;
        }

        public int getUid() {
            return uid;
        }

        public String getCommand() {
            return command;
        }

        public String getNextAlias() {
            return nextAlias;
        }

        public String getRequiredStateRef() {
            return requiredStateRef;
        }

        @Override
        public String toString() {
            return "Transition{"
                    + "uid="
                    + uid
                    + ", command='"
                    + command
                    + '\''
                    + ", nextAlias='"
                    + nextAlias
                    + '\''
                    + ", requiredStateRef='"
                    + requiredStateRef
                    + '\''
                    + '}';
        }
    }

    /** Assembly for one stateRef. */
    private static final class StateAssembly
            implements Serializable {

        private static final long serialVersionUID = 1L;

        private final String stateRef;

        private JsonNode beginPayload;
        private JsonNode commitPayload;

        private final Map<Integer, JsonNode> chunksById =
                new LinkedHashMap<Integer, JsonNode>();

        private StateAssembly(
                String stateRef) {

            this.stateRef = stateRef;
        }

        private void acceptBegin(
                JsonNode payload) {

            if (beginPayload != null
                    && !beginPayload.equals(payload)) {

                throw new IllegalStateException(
                        "Conflicting GLOBAL_STATE_BEGIN for stateRef="
                                + stateRef
                );
            }

            beginPayload =
                    payload.deepCopy();
        }

        private void acceptChunk(
                JsonNode payload) {

            int chunkId =
                    intField(
                            payload,
                            "chunkId",
                            -1
                    );

            int chunkCount =
                    intField(
                            payload,
                            "chunkCount",
                            -1
                    );

            if (chunkId < 0
                    || chunkCount <= 0
                    || chunkId >= chunkCount) {

                throw new IllegalArgumentException(
                        "Invalid chunk metadata for stateRef="
                                + stateRef
                                + ": "
                                + payload
                );
            }

            JsonNode existing =
                    chunksById.get(chunkId);

            if (existing != null
                    && !existing.equals(payload)) {

                throw new IllegalStateException(
                        "Conflicting duplicate chunk "
                                + chunkId
                                + " for stateRef="
                                + stateRef
                );
            }

            chunksById.put(
                    chunkId,
                    payload.deepCopy()
            );
        }

        private void acceptCommit(
                JsonNode payload) {

            if (commitPayload != null
                    && !commitPayload.equals(payload)) {

                throw new IllegalStateException(
                        "Conflicting GLOBAL_STATE_COMMIT for stateRef="
                                + stateRef
                );
            }

            commitPayload =
                    payload.deepCopy();
        }

        private JsonNode tryAssemble() {
            if (beginPayload == null
                    || commitPayload == null) {

                return null;
            }

            int beginChunkCount =
                    intField(
                            beginPayload,
                            "chunkCount",
                            -1
                    );

            int commitChunkCount =
                    intField(
                            commitPayload,
                            "chunkCount",
                            -1
                    );

            if (beginChunkCount <= 0) {
                throw new IllegalStateException(
                        "BEGIN has invalid chunkCount for stateRef="
                                + stateRef
                );
            }

            if (beginChunkCount
                    != commitChunkCount) {

                throw new IllegalStateException(
                        "BEGIN/COMMIT chunkCount mismatch for stateRef="
                                + stateRef
                                + ": "
                                + beginChunkCount
                                + " vs "
                                + commitChunkCount
                );
            }

            if (chunksById.size()
                    < beginChunkCount) {

                return null;
            }

            ArrayNode entries =
                    MAPPER.createArrayNode();

            for (int chunkId = 0;
                 chunkId < beginChunkCount;
                 chunkId++) {

                JsonNode chunk =
                        chunksById.get(chunkId);

                if (chunk == null) {
                    return null;
                }

                int chunkDeclaredCount =
                        intField(
                                chunk,
                                "chunkCount",
                                -1
                        );

                if (chunkDeclaredCount
                        != beginChunkCount) {

                    throw new IllegalStateException(
                            "Chunk "
                                    + chunkId
                                    + " has chunkCount="
                                    + chunkDeclaredCount
                                    + " but expected "
                                    + beginChunkCount
                                    + " for stateRef="
                                    + stateRef
                    );
                }

                JsonNode chunkEntries =
                        chunk.get("entries");

                if (chunkEntries == null
                        || !chunkEntries.isArray()) {

                    throw new IllegalStateException(
                            "Chunk "
                                    + chunkId
                                    + " has no entries array for stateRef="
                                    + stateRef
                    );
                }

                int declaredEntryCount =
                        intField(
                                chunk,
                                "entryCount",
                                -1
                        );

                if (declaredEntryCount
                        != chunkEntries.size()) {

                    throw new IllegalStateException(
                            "Chunk "
                                    + chunkId
                                    + " entryCount mismatch for stateRef="
                                    + stateRef
                    );
                }

                for (JsonNode entry
                        : chunkEntries) {

                    entries.add(
                            entry.deepCopy()
                    );
                }
            }

            int expectedTotalEntryCount =
                    intField(
                            beginPayload,
                            "totalEntryCount",
                            -1
                    );

            int commitTotalEntryCount =
                    intField(
                            commitPayload,
                            "totalEntryCount",
                            -1
                    );

            if (expectedTotalEntryCount
                    != commitTotalEntryCount) {

                throw new IllegalStateException(
                        "BEGIN/COMMIT totalEntryCount mismatch for stateRef="
                                + stateRef
                );
            }

            if (entries.size()
                    != expectedTotalEntryCount) {

                throw new IllegalStateException(
                        "Reconstructed entry count mismatch for stateRef="
                                + stateRef
                                + ": expected "
                                + expectedTotalEntryCount
                                + ", reconstructed "
                                + entries.size()
                );
            }

            String beginChecksum =
                    requiredTextField(
                            beginPayload,
                            "checksum"
                    );

            String commitChecksum =
                    requiredTextField(
                            commitPayload,
                            "checksum"
                    );

            if (!beginChecksum.equals(
                    commitChecksum)) {

                throw new IllegalStateException(
                        "BEGIN/COMMIT checksum mismatch for stateRef="
                                + stateRef
                );
            }

            String reconstructedChecksum =
                    sha256Hex(
                            entries.toString()
                    );

            if (!beginChecksum.equals(
                    reconstructedChecksum)) {

                throw new IllegalStateException(
                        "Reconstructed checksum mismatch for stateRef="
                                + stateRef
                                + ": expected "
                                + beginChecksum
                                + ", actual "
                                + reconstructedChecksum
                );
            }

            ObjectNode assembled =
                    MAPPER.createObjectNode();

            assembled.put(
                    "type",
                    STATE_TYPE_GLOBAL_PHASE1_INDEX
            );

            assembled.put(
                    "stateRef",
                    stateRef
            );

            copyIfPresent(beginPayload, assembled, "stateType");
            copyIfPresent(beginPayload, assembled, "uid");
            copyIfPresent(beginPayload, assembled, "synopsisID");
            copyIfPresent(beginPayload, assembled, "phase");
            copyIfPresent(beginPayload, assembled, "resultId");
            copyIfPresent(beginPayload, assembled, "queryName");
            copyIfPresent(beginPayload, assembled, "rootAlias");
            copyIfPresent(beginPayload, assembled, "baseKey");
            copyIfPresent(beginPayload, assembled, "expectedWorkers");
            copyIfPresent(beginPayload, assembled, "seenTuplesByAlias");
            copyIfPresent(beginPayload, assembled, "edgeSummaries");
            copyIfPresent(beginPayload, assembled, "activeAlias");
            copyIfPresent(beginPayload, assembled, "activeEdgeId");

            assembled.put(
                    "chunkCount",
                    beginChunkCount
            );

            assembled.put(
                    "totalEntryCount",
                    entries.size()
            );

            assembled.put(
                    "checksum",
                    reconstructedChecksum
            );

            assembled.set(
                    "entries",
                    entries
            );

            return assembled;
        }

        private static void copyIfPresent(
                JsonNode source,
                ObjectNode target,
                String fieldName) {

            JsonNode value =
                    source.get(fieldName);

            if (value != null
                    && !value.isNull()) {

                target.set(
                        fieldName,
                        value.deepCopy()
                );
            }
        }
    }
}