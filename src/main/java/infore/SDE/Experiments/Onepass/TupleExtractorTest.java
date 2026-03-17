package infore.SDE.Experiments.Onepass;

import infore.SDE.synopses.OnePassSampler.OnePassTuple;
import infore.SDE.transformations.onepass.OnePassTupleExtractor;

public class TupleExtractorTest {

    public static void main(String[] args) {
        testValidOrdersTuple();
        testValidLineitemTuple();
        testMissingTableField();
        testInvalidJson();
        testMissingFieldAccess();

        System.out.println("\nAll OnePassTuple / OnePassTupleExtractor smoke tests passed.");
    }

    private static void testValidOrdersTuple() {
        String json = "{\"table\":\"orders\",\"o_orderkey\":1,\"o_custkey\":42}";

        OnePassTuple tuple = OnePassTupleExtractor.extract(json);

        assert tuple != null : "Tuple should not be null";
        assert "orders".equals(tuple.getTable()) : "Expected table=orders";
        assert tuple.hasField("o_orderkey") : "Expected o_orderkey field";
        assert tuple.hasField("o_custkey") : "Expected o_custkey field";

        Long orderKey = tuple.getFieldAsLong("o_orderkey");
        Long custKey = tuple.getFieldAsLong("o_custkey");

        assert orderKey != null && orderKey == 1L : "Expected o_orderkey=1";
        assert custKey != null && custKey == 42L : "Expected o_custkey=42";

        System.out.println("testValidOrdersTuple passed");
    }

    private static void testValidLineitemTuple() {
        String json = "{\"table\":\"lineitem\",\"l_orderkey\":10,\"l_partkey\":1552,\"l_linenumber\":3}";

        OnePassTuple tuple = OnePassTupleExtractor.extract(json);

        assert tuple != null : "Tuple should not be null";
        assert "lineitem".equals(tuple.getTable()) : "Expected table=lineitem";

        Long orderKey = tuple.getFieldAsLong("l_orderkey");
        Long partKey = tuple.getFieldAsLong("l_partkey");
        Integer lineNumber = tuple.getFieldAsInt("l_linenumber");

        assert orderKey != null && orderKey == 10L : "Expected l_orderkey=10";
        assert partKey != null && partKey == 1552L : "Expected l_partkey=1552";
        assert lineNumber != null && lineNumber == 3 : "Expected l_linenumber=3";

        System.out.println("testValidLineitemTuple passed");
    }

    private static void testMissingTableField() {
        String json = "{\"o_orderkey\":1,\"o_custkey\":42}";

        boolean failed = false;
        try {
            OnePassTupleExtractor.extract(json);
        } catch (IllegalArgumentException e) {
            failed = true;
            System.out.println("testMissingTableField passed: " + e.getMessage());
        }

        assert failed : "Expected extractor to fail when 'table' is missing";
    }

    private static void testInvalidJson() {
        String json = "{\"table\":\"orders\",\"o_orderkey\":1,"; // broken JSON

        boolean failed = false;
        try {
            OnePassTupleExtractor.extract(json);
        } catch (IllegalArgumentException e) {
            failed = true;
            System.out.println("testInvalidJson passed: " + e.getMessage());
        }

        assert failed : "Expected extractor to fail on invalid JSON";
    }

    private static void testMissingFieldAccess() {
        String json = "{\"table\":\"customer\",\"c_custkey\":7,\"c_nationkey\":15}";

        OnePassTuple tuple = OnePassTupleExtractor.extract(json);

        assert tuple != null : "Tuple should not be null";
        assert !tuple.hasField("c_phone") : "Did not expect c_phone field";
        assert tuple.getField("c_phone") == null : "Expected null for missing raw field";
        assert tuple.getFieldAsText("c_phone") == null : "Expected null for missing text field";
        assert tuple.getFieldAsLong("c_phone") == null : "Expected null for missing long field";

        System.out.println("testMissingFieldAccess passed");
    }
}