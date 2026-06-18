package infore.SDE.transformations.onepass.sql;

import infore.SDE.messages.Onepass.DatasetConfig;
import infore.SDE.messages.Onepass.JoinEdgeSpec;
import infore.SDE.messages.Onepass.OnePassParams;
import infore.SDE.messages.Onepass.OutputSpec;
import infore.SDE.messages.Onepass.RelationSpec;
import infore.SDE.messages.Onepass.WeightSpec;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class OnePassSqlCompiler {

    private OnePassSqlCompiler() {
    }

    public static OnePassParams compile(String sql) {
        OnePassSqlRequest request = OnePassSqlParser.parse(sql);
        OnePassCatalog catalog = OnePassQueryCatalogLoader.load(request.getCatalogRef());

        if (catalog.getQueries() == null ||
                !catalog.getQueries().containsKey(request.getQueryName())) {
            throw new IllegalArgumentException(
                    "Query '" + request.getQueryName() +
                            "' does not exist in catalog '" + request.getCatalogRef() + "'"
            );
        }

        OnePassCatalog.CatalogQuery query =
                catalog.getQueries().get(request.getQueryName());

        OnePassParams params = new OnePassParams();

        params.setQueryName(request.getQueryName());
        params.setMainTable(firstNonBlank(request.getRootAlias(), query.getDefaultRoot()));

        DatasetConfig dataset = new DatasetConfig();

        if (catalog.getDataset() != null) {
            dataset.setName(catalog.getDataset().getName());
        } else {
            dataset.setName("unknown");
        }

        dataset.setDbConfig(request.getCatalogRef());
        dataset.setScaleFactor(request.getScaleFactor());
        dataset.setSeed(request.getSeed());

        params.setDataset(dataset);

        params.setRelations(copyRelations(query.getRelations()));
        params.setJoins(copyJoins(query.getJoins()));
        params.setWeight(buildWeightSpec(query, request.getWeightOverride()));
        params.setOutput(buildOutputSpec(query, request.getSampleSize()));

        return params;
    }

    private static WeightSpec buildWeightSpec(OnePassCatalog.CatalogQuery query,
                                              String weightOverride) {
        WeightSpec weight = new WeightSpec();

        /*
         * If no WEIGHTED BY override is supplied, use the catalog-defined
         * alias-specific weights.
         */
        if (isBlank(weightOverride)) {
            weight.setExpression("1");
            weight.setWeightsByAlias(copyStringMap(query.getDefaultWeightsByAlias()));
            weight.setVariables(new ArrayList<String>());
            return weight;
        }

        /*
         * First implementation:
         *
         * - A plain WEIGHTED BY expression becomes a global fallback expression.
         * - Alias-specific defaults are still preferred when present.
         *
         * In the next iteration we can add a stricter decomposer for:
         *   WEIGHTED BY orders:o_totalprice, lineitem:l_extendedprice
         */
        weight.setExpression(weightOverride.trim());
        weight.setWeightsByAlias(new LinkedHashMap<String, String>());
        weight.setVariables(extractSimpleVariables(weightOverride));

        return weight;
    }

    private static OutputSpec buildOutputSpec(OnePassCatalog.CatalogQuery query,
                                              int sampleSize) {
        OutputSpec output = new OutputSpec();

        output.setSampleSize(sampleSize);
        output.setProjection(copyStringList(query.getProjection()));

        return output;
    }

    private static List<RelationSpec> copyRelations(List<RelationSpec> source) {
        List<RelationSpec> out = new ArrayList<RelationSpec>();

        if (source == null) {
            return out;
        }

        for (RelationSpec relation : source) {
            RelationSpec copy = new RelationSpec();
            copy.setAlias(relation.getAlias());
            copy.setTable(relation.getTable());
            out.add(copy);
        }

        return out;
    }

    private static List<JoinEdgeSpec> copyJoins(List<JoinEdgeSpec> source) {
        List<JoinEdgeSpec> out = new ArrayList<JoinEdgeSpec>();

        if (source == null) {
            return out;
        }

        for (JoinEdgeSpec join : source) {
            JoinEdgeSpec copy = new JoinEdgeSpec();
            copy.setLeftAlias(join.getLeftAlias());
            copy.setLeftField(join.getLeftField());
            copy.setRightAlias(join.getRightAlias());
            copy.setRightField(join.getRightField());
            out.add(copy);
        }

        return out;
    }

    private static Map<String, String> copyStringMap(Map<String, String> source) {
        Map<String, String> out = new LinkedHashMap<String, String>();

        if (source != null) {
            out.putAll(source);
        }

        return out;
    }

    private static List<String> copyStringList(List<String> source) {
        List<String> out = new ArrayList<String>();

        if (source != null) {
            out.addAll(source);
        }

        return out;
    }

    private static List<String> extractSimpleVariables(String expression) {
        List<String> out = new ArrayList<String>();

        if (expression == null) {
            return out;
        }

        String[] tokens = expression.split("[^A-Za-z0-9_\\.]+");

        for (String token : tokens) {
            if (isBlank(token)) {
                continue;
            }

            if (isNumeric(token)) {
                continue;
            }

            String normalized = token;

            int dot = normalized.indexOf('.');
            if (dot >= 0 && dot + 1 < normalized.length()) {
                normalized = normalized.substring(dot + 1);
            }

            if (!out.contains(normalized)) {
                out.add(normalized);
            }
        }

        return out;
    }

    private static boolean isNumeric(String value) {
        try {
            Double.parseDouble(value);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private static String firstNonBlank(String first, String second) {
        return isBlank(first) ? second : first;
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}