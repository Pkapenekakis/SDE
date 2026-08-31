package Tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import infore.SDE.messages.Datapoint;
import infore.SDE.messages.Request;
import infore.SDE.transformations.onepass.OnePassDataRouterCoFlatMap;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class OnePassEndAliasRouterTest {

    private static final ObjectMapper MAPPER =
            new ObjectMapper();

    @Test
    public void shouldBroadcastEndAliasToEveryLogicalWorker()
            throws Exception {

        OnePassDataRouterCoFlatMap router =
                new OnePassDataRouterCoFlatMap();

        DatapointCollector out =
                new DatapointCollector();

        Request add =
                new Request(
                        "onepass-phase1-123",
                        1,
                        30,
                        123,
                        "stream",
                        new String[] {
                                "ONEPASS_SQL"
                        },
                        4
                );

        /*
         * Registers baseKey -> parallelism=4.
         */
        router.flatMap2(
                add,
                out
        );

        ObjectNode marker =
                MAPPER.createObjectNode();

        marker.put(
                "type",
                "END_ALIAS"
        );

        marker.put(
                "synopsisID",
                30
        );

        marker.put(
                "uid",
                123
        );

        marker.put(
                "phase",
                "PHASE1"
        );

        marker.put(
                "alias",
                "l"
        );

        marker.put(
                "resultId",
                "PHASE1_l_123"
        );

        marker.put(
                "expectedWorkers",
                4
        );

        marker.put(
                "nextCommand",
                "START_NEXT_ALIAS"
        );

        marker.put(
                "nextAlias",
                "o"
        );

        Datapoint endAlias =
                new Datapoint(
                        "onepass-phase1-123",
                        "stream",
                        marker
                );

        router.flatMap1(
                endAlias,
                out
        );

        assertEquals(
                4,
                out.values.size()
        );

        Set<String> keys =
                new HashSet<String>();

        for (Datapoint value
                : out.values) {

            keys.add(
                    value.getKey()
            );

            assertEquals(
                    "END_ALIAS",
                    value.getValues()
                            .get("type")
                            .asText()
            );
        }

        assertTrue(
                keys.contains(
                        "onepass-phase1-123_4_KEYED_0"
                )
        );

        assertTrue(
                keys.contains(
                        "onepass-phase1-123_4_KEYED_1"
                )
        );

        assertTrue(
                keys.contains(
                        "onepass-phase1-123_4_KEYED_2"
                )
        );

        assertTrue(
                keys.contains(
                        "onepass-phase1-123_4_KEYED_3"
                )
        );
    }

    private static final class DatapointCollector
            implements Collector<Datapoint> {

        private final List<Datapoint> values =
                new ArrayList<Datapoint>();

        @Override
        public void collect(
                Datapoint record) {

            values.add(record);
        }

        @Override
        public void close() {
        }
    }
}