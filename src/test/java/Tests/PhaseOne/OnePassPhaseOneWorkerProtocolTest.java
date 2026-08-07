package Tests.PhaseOne;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import infore.SDE.transformations.onepass.worker.OnePassPhaseOneWorkerProtocol;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class OnePassPhaseOneWorkerProtocolTest {

    private static final ObjectMapper MAPPER =
            new ObjectMapper();

    @Test
    public void shouldAssembleInstallAndActivatePendingTransition()
            throws Exception {

        OnePassPhaseOneWorkerProtocol protocol =
                new OnePassPhaseOneWorkerProtocol();

        ProtocolMessages messages =
                protocolMessages();

        assertNull(
                protocol.acceptStateMessage(
                        messages.begin
                )
        );

        assertNull(
                protocol.acceptStateMessage(
                        messages.chunk0
                )
        );

        // Transition arrives before COMMIT/state installation.
        boolean activatedImmediately =
                protocol.acceptTransition(
                        messages.transition
                );

        assertFalse(activatedImmediately);
        assertNotNull(protocol.getPendingTransition(123));
        assertNull(protocol.getActiveTransition(123));

        assertNull(
                protocol.acceptStateMessage(
                        messages.chunk1
                )
        );

        JsonNode assembled =
                protocol.acceptStateMessage(
                        messages.commit
                );

        assertNotNull(assembled);

        assertEquals(
                "GLOBAL_PHASE1_INDEX",
                assembled.get("type").asText()
        );

        assertEquals(
                3,
                assembled.get("entries").size()
        );

        assertEquals(
                "l<->o",
                assembled.get("entries")
                        .get(0)
                        .get("edgeId")
                        .asText()
        );

        assertFalse(protocol.hasActiveTransition(123));

        // SDEcoFlatMap installs the state and then calls markInstalled.
        protocol.markInstalled(
                123,
                "123_PHASE1_l_GLOBAL_STATE"
        );

        assertTrue(
                protocol.isStateInstalled(
                        123,
                        "123_PHASE1_l_GLOBAL_STATE"
                )
        );

        assertNull(protocol.getPendingTransition(123));

        OnePassPhaseOneWorkerProtocol.Transition active =
                protocol.getActiveTransition(123);

        assertNotNull(active);

        assertEquals(
                "START_NEXT_ALIAS",
                active.getCommand()
        );

        assertEquals(
                "o",
                active.getNextAlias()
        );

        assertEquals(
                "123_PHASE1_l_GLOBAL_STATE",
                active.getRequiredStateRef()
        );
    }

    @Test
    public void shouldActivateTransitionImmediatelyWhenStateAlreadyInstalled()
            throws Exception {

        OnePassPhaseOneWorkerProtocol protocol =
                new OnePassPhaseOneWorkerProtocol();

        ProtocolMessages messages =
                protocolMessages();

        protocol.acceptStateMessage(messages.begin);
        protocol.acceptStateMessage(messages.chunk0);
        protocol.acceptStateMessage(messages.chunk1);

        JsonNode assembled =
                protocol.acceptStateMessage(messages.commit);

        assertNotNull(assembled);

        protocol.markInstalled(
                123,
                "123_PHASE1_l_GLOBAL_STATE"
        );

        boolean activated =
                protocol.acceptTransition(messages.transition);

        assertTrue(activated);
        assertNotNull(protocol.getActiveTransition(123));
    }

    @Test
    public void shouldAcceptIdenticalDuplicateChunk()
            throws Exception {

        OnePassPhaseOneWorkerProtocol protocol =
                new OnePassPhaseOneWorkerProtocol();

        ProtocolMessages messages =
                protocolMessages();

        protocol.acceptStateMessage(messages.begin);
        protocol.acceptStateMessage(messages.chunk0);

        // Identical Kafka retry/re-delivery is idempotent.
        protocol.acceptStateMessage(messages.chunk0.deepCopy());

        protocol.acceptStateMessage(messages.chunk1);

        JsonNode assembled =
                protocol.acceptStateMessage(messages.commit);

        assertNotNull(assembled);

        assertEquals(
                3,
                assembled.get("entries").size()
        );
    }

    @Test(expected = IllegalStateException.class)
    public void shouldRejectChecksumMismatch()
            throws Exception {

        OnePassPhaseOneWorkerProtocol protocol =
                new OnePassPhaseOneWorkerProtocol();

        ProtocolMessages messages =
                protocolMessages();

        ObjectNode invalidCommit =
                messages.commit.deepCopy();

        invalidCommit.put(
                "checksum",
                "invalid-checksum"
        );

        protocol.acceptStateMessage(messages.begin);
        protocol.acceptStateMessage(messages.chunk0);
        protocol.acceptStateMessage(messages.chunk1);
        protocol.acceptStateMessage(invalidCommit);
    }

    @Test
    public void shouldRetainReadyStateUntilSynopsisCanInstallIt()
            throws Exception {

        OnePassPhaseOneWorkerProtocol protocol =
                new OnePassPhaseOneWorkerProtocol();

        ProtocolMessages messages =
                protocolMessages();

        protocol.acceptStateMessage(messages.begin);
        protocol.acceptStateMessage(messages.chunk0);
        protocol.acceptStateMessage(messages.chunk1);

        JsonNode assembled =
                protocol.acceptStateMessage(messages.commit);

        assertNotNull(assembled);

        JsonNode retained =
                protocol.getReadyStateForUid(123);

        assertNotNull(retained);

        assertEquals(
                "123_PHASE1_l_GLOBAL_STATE",
                retained.get("stateRef").asText()
        );

        protocol.markInstalled(
                123,
                "123_PHASE1_l_GLOBAL_STATE"
        );

        assertNull(protocol.getReadyStateForUid(123));
    }

    private static ProtocolMessages protocolMessages()
            throws Exception {

        ArrayNode allEntries =
                MAPPER.createArrayNode();

        allEntries.add(entry("l<->o", "100", 10.0d));
        allEntries.add(entry("l<->o", "200", 20.0d));
        allEntries.add(entry("l<->o", "300", 30.0d));

        String checksum =
                sha256Hex(allEntries.toString());

        ObjectNode begin =
                baseMessage("GLOBAL_STATE_BEGIN");

        begin.put("chunkCount", 2);
        begin.put("totalEntryCount", 3);
        begin.put("checksum", checksum);

        begin.putObject("seenTuplesByAlias")
                .put("l", 3);

        begin.putObject("edgeSummaries")
                .putObject("l<->o")
                .put("numberOfKeys", 3);

        ObjectNode chunk0 =
                baseMessage("GLOBAL_STATE_CHUNK");

        chunk0.put("chunkId", 0);
        chunk0.put("chunkCount", 2);
        chunk0.put("entryCount", 2);

        ArrayNode chunk0Entries =
                chunk0.putArray("entries");

        chunk0Entries.add(allEntries.get(0).deepCopy());
        chunk0Entries.add(allEntries.get(1).deepCopy());

        ObjectNode chunk1 =
                baseMessage("GLOBAL_STATE_CHUNK");

        chunk1.put("chunkId", 1);
        chunk1.put("chunkCount", 2);
        chunk1.put("entryCount", 1);

        chunk1.putArray("entries")
                .add(allEntries.get(2).deepCopy());

        ObjectNode commit =
                baseMessage("GLOBAL_STATE_COMMIT");

        commit.put("chunkCount", 2);
        commit.put("totalEntryCount", 3);
        commit.put("checksum", checksum);
        commit.put("nextCommand", "START_NEXT_ALIAS");
        commit.put("nextAlias", "o");
        commit.put(
                "requiredStateRef",
                "123_PHASE1_l_GLOBAL_STATE"
        );

        ObjectNode transition =
                MAPPER.createObjectNode();

        transition.put("type", "START_NEXT_ALIAS");
        transition.put("uid", 123);
        transition.put("baseKey", "onepass-phase1-123");
        transition.put("nextAlias", "o");
        transition.put(
                "requiredStateRef",
                "123_PHASE1_l_GLOBAL_STATE"
        );

        return new ProtocolMessages(
                begin,
                chunk0,
                chunk1,
                commit,
                transition
        );
    }

    private static ObjectNode baseMessage(
            String type) {

        ObjectNode payload =
                MAPPER.createObjectNode();

        payload.put("type", type);
        payload.put("stateType", "GLOBAL_PHASE1_INDEX");
        payload.put("uid", 123);
        payload.put("synopsisID", 30);
        payload.put("phase", "PHASE1");
        payload.put("resultId", "PHASE1_l_123");
        payload.put("stateRef", "123_PHASE1_l_GLOBAL_STATE");
        payload.put("queryName", "wq3_alias");
        payload.put("rootAlias", "c");
        payload.put("baseKey", "onepass-phase1-123");
        payload.put("expectedWorkers", 4);
        payload.put("activeAlias", "l");
        payload.put("activeEdgeId", "l<->o");

        return payload;
    }

    private static ObjectNode entry(
            String edgeId,
            String joinKey,
            double globalWeight) {

        ObjectNode entry =
                MAPPER.createObjectNode();

        entry.put("edgeId", edgeId);
        entry.put("joinKey", joinKey);
        entry.put("globalWeight", globalWeight);

        return entry;
    }

    private static String sha256Hex(
            String value) throws Exception {

        MessageDigest digest =
                MessageDigest.getInstance("SHA-256");

        byte[] hash =
                digest.digest(
                        value.getBytes(StandardCharsets.UTF_8)
                );

        StringBuilder out =
                new StringBuilder(hash.length * 2);

        for (byte b : hash) {
            out.append(
                    String.format("%02x", b & 0xff)
            );
        }

        return out.toString();
    }

    private static final class ProtocolMessages {

        private final ObjectNode begin;
        private final ObjectNode chunk0;
        private final ObjectNode chunk1;
        private final ObjectNode commit;
        private final ObjectNode transition;

        private ProtocolMessages(
                ObjectNode begin,
                ObjectNode chunk0,
                ObjectNode chunk1,
                ObjectNode commit,
                ObjectNode transition) {

            this.begin = begin;
            this.chunk0 = chunk0;
            this.chunk1 = chunk1;
            this.commit = commit;
            this.transition = transition;
        }
    }
}