package infore.SDE.transformations.onepass.sql;

import infore.SDE.messages.Onepass.DatasetConfig;
import infore.SDE.messages.Onepass.JoinEdgeSpec;
import infore.SDE.messages.Onepass.OnePassParams;
import infore.SDE.messages.Onepass.OutputSpec;
import infore.SDE.messages.Onepass.RelationSpec;
import infore.SDE.messages.Onepass.WeightSpec;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class OnePassSqlCompiler {

    private static final Pattern IDENTIFIER =
            Pattern.compile("\\b[A-Za-z_][A-Za-z0-9_]*\\b");

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

        params.setWeight(
                buildWeightSpec(
                        query,
                        request.getWeightOverride(),
                        catalog
                )
        );

        params.setOutput(buildOutputSpec(query, request, catalog));

        return params;
    }

    private static WeightSpec buildWeightSpec(OnePassCatalog.CatalogQuery query,
                                              String weightOverride,
                                              OnePassCatalog catalog) {
        WeightSpec weight = new WeightSpec();

        /*
         * Default fallback should be 1.
         *
         * In the new design, alias-specific weights are the real source of
         * tuple weights. The global expression is only a backward-compatible
         * fallback.
         */
        weight.setExpression("1");
        weight.setVariables(new ArrayList<String>());

        /*
         * Start from catalog defaults.
         */
        Map<String, String> weightsByAlias =
                copyStringMap(query.getDefaultWeightsByAlias());

        /*
         * SQL WEIGHTED BY overrides catalog defaults.
         *
         * Example:
         *
         * WEIGHTED BY (
         *     c.c_acctbal
         *     * o.o_totalprice
         *     * (l.l_extendedprice * (1 - l.l_discount))
         * )
         *
         * becomes:
         *
         * c -> c_acctbal
         * o -> o_totalprice
         * l -> l_extendedprice * (1 - l_discount)
         */
        if (!isBlank(weightOverride)) {
            Set<String> validAliases = buildAliasSet(query);

            weightsByAlias =
                    OnePassWeightedByParser.parseWeightsByAlias(
                            weightOverride,
                            validAliases
                    );

            validateWeightExpressions(weightsByAlias, query, catalog);
        } else {
            validateWeightExpressions(weightsByAlias, query, catalog);
        }

        weight.setWeightsByAlias(weightsByAlias);

        return weight;
    }

    private static OutputSpec buildOutputSpec(OnePassCatalog.CatalogQuery query,
                                              OnePassSqlRequest request,
                                              OnePassCatalog catalog) {
        OutputSpec output = new OutputSpec();

        output.setSampleSize(request.getSampleSize());

        List<String> projection = resolveProjection(query, request);

        validateProjection(projection, query, catalog);

        output.setProjection(projection);

        return output;
    }

    private static List<String> resolveProjection(OnePassCatalog.CatalogQuery query,
                                                  OnePassSqlRequest request) {
        List<String> sqlProjection = request.getProjection();

        /*
         * If SQL uses SELECT *, use the catalog projection.
         */
        if (isStarProjection(sqlProjection)) {
            return copyStringList(query.getProjection());
        }

        /*
         * Otherwise, use the explicit SQL projection.
         */
        return copyStringList(sqlProjection);
    }

    private static boolean isStarProjection(List<String> projection) {
        if (projection == null || projection.isEmpty()) {
            return true;
        }

        return projection.size() == 1 && "*".equals(projection.get(0).trim());
    }

    private static void validateProjection(List<String> projection,
                                           OnePassCatalog.CatalogQuery query,
                                           OnePassCatalog catalog) {
        if (projection == null || projection.isEmpty()) {
            throw new IllegalArgumentException(
                    "Projection is empty. Use SELECT * or at least one alias.field expression."
            );
        }

        Map<String, String> tableByAlias = buildTableByAlias(query);

        for (String item : projection) {
            validateProjectionItem(item, tableByAlias, catalog);
        }
    }

    private static Map<String, String> buildTableByAlias(OnePassCatalog.CatalogQuery query) {
        Map<String, String> tableByAlias = new LinkedHashMap<String, String>();

        if (query.getRelations() == null) {
            return tableByAlias;
        }

        for (RelationSpec relation : query.getRelations()) {
            if (relation == null) {
                continue;
            }

            tableByAlias.put(relation.getAlias(), relation.getTable());
        }

        return tableByAlias;
    }

    private static Set<String> buildAliasSet(OnePassCatalog.CatalogQuery query) {
        return new LinkedHashSet<String>(buildTableByAlias(query).keySet());
    }

    private static void validateProjectionItem(String item,
                                               Map<String, String> tableByAlias,
                                               OnePassCatalog catalog) {
        if (isBlank(item)) {
            throw new IllegalArgumentException("Projection contains a blank item");
        }

        String trimmed = item.trim();

        if ("*".equals(trimmed)) {
            return;
        }

        int dot = trimmed.indexOf('.');

        if (dot <= 0 || dot == trimmed.length() - 1 || trimmed.indexOf('.', dot + 1) >= 0) {
            throw new IllegalArgumentException(
                    "Invalid projection item '" + trimmed + "'. Expected format alias.field, for example c.c_custkey."
            );
        }

        String alias = trimmed.substring(0, dot);
        String field = trimmed.substring(dot + 1);

        String tableName = tableByAlias.get(alias);

        if (tableName == null) {
            throw new IllegalArgumentException(
                    "Projection item '" + trimmed + "' uses unknown alias '" + alias + "'. " +
                            "Available aliases are: " + tableByAlias.keySet()
            );
        }

        validateFieldExists(alias, field, tableName, catalog, "Projection item '" + trimmed + "'");
    }

    private static void validateWeightExpressions(Map<String, String> weightsByAlias,
                                                  OnePassCatalog.CatalogQuery query,
                                                  OnePassCatalog catalog) {
        if (weightsByAlias == null || weightsByAlias.isEmpty()) {
            return;
        }

        Map<String, String> tableByAlias = buildTableByAlias(query);

        for (Map.Entry<String, String> entry : weightsByAlias.entrySet()) {
            String alias = entry.getKey();
            String expression = entry.getValue();

            if (isBlank(alias)) {
                throw new IllegalArgumentException(
                        "Weight expression contains a blank alias"
                );
            }

            if (isBlank(expression)) {
                throw new IllegalArgumentException(
                        "Weight expression for alias '" + alias + "' is blank"
                );
            }

            String tableName = tableByAlias.get(alias);

            if (tableName == null) {
                throw new IllegalArgumentException(
                        "Weight expression uses unknown alias '" + alias + "'. " +
                                "Available aliases are: " + tableByAlias.keySet()
                );
            }

            validateLocalWeightExpressionFields(alias, tableName, expression, catalog);
        }
    }

    private static void validateLocalWeightExpressionFields(String alias,
                                                            String tableName,
                                                            String expression,
                                                            OnePassCatalog catalog) {
        Matcher matcher = IDENTIFIER.matcher(expression);

        while (matcher.find()) {
            String token = matcher.group();

            /*
             * All identifiers inside alias-local expressions must be fields.
             *
             * Example:
             *   l_extendedprice * (1 - l_discount)
             */
            validateFieldExists(
                    alias,
                    token,
                    tableName,
                    catalog,
                    "Weight expression for alias '" + alias + "': " + expression
            );
        }
    }

    private static void validateFieldExists(String alias,
                                            String field,
                                            String tableName,
                                            OnePassCatalog catalog,
                                            String context) {
        if (catalog.getDataset() == null || catalog.getDataset().getTables() == null) {
            throw new IllegalArgumentException("Catalog dataset tables are missing");
        }

        OnePassCatalog.CatalogTable table =
                catalog.getDataset().getTables().get(tableName);

        if (table == null) {
            throw new IllegalArgumentException(
                    "Alias '" + alias + "' refers to table '" + tableName +
                            "', but that table is not defined in the catalog dataset."
            );
        }

        List<String> columns = table.getColumns();

        if (columns == null || !columns.contains(field)) {
            throw new IllegalArgumentException(
                    context + " uses unknown field '" + field +
                            "' for alias '" + alias + "' / table '" + tableName + "'. " +
                            "Available columns are: " + columns
            );
        }
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

    private static String firstNonBlank(String first, String second) {
        return isBlank(first) ? second : first;
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}