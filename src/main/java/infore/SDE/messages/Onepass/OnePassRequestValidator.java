package infore.SDE.messages.Onepass;

import java.util.HashSet;
import java.util.Set;

public final class OnePassRequestValidator {

    private OnePassRequestValidator() {
    }

    public static void validate(OnePassParams params) {
        if (params == null) {
            throw new IllegalArgumentException("OnePassStarParams is null");
        }

        if (isBlank(params.getQueryName())) {
            throw new IllegalArgumentException("queryName is required");
        }

        if (isBlank(params.getMainTable())) {
            throw new IllegalArgumentException("mainTable is required");
        }

        if (params.getDataset() == null) {
            throw new IllegalArgumentException("dataset is required");
        }

        if (params.getRelations() == null || params.getRelations().isEmpty()) {
            throw new IllegalArgumentException("relations must not be empty");
        }

        if (params.getJoins() == null || params.getJoins().isEmpty()) {
            throw new IllegalArgumentException("joins must not be empty");
        }

        if (params.getWeight() == null || isBlank(params.getWeight().getExpression())) {
            throw new IllegalArgumentException("weight.expression is required");
        }

        if (params.getOutput() == null) {
            throw new IllegalArgumentException("output is required");
        }

        if (params.getOutput().getSampleSize() <= 0) {
            throw new IllegalArgumentException("output.sampleSize must be > 0");
        }

        Set<String> aliases = new HashSet<>();
        boolean mainFound = false;

        for (RelationSpec relation : params.getRelations()) {
            if (relation == null) {
                throw new IllegalArgumentException("relations contains null entry");
            }
            if (isBlank(relation.getTable())) {
                throw new IllegalArgumentException("relation.table is required");
            }
            if (isBlank(relation.getAlias())) {
                throw new IllegalArgumentException("relation.alias is required");
            }
            if (!aliases.add(relation.getAlias())) {
                throw new IllegalArgumentException("Duplicate relation alias: " + relation.getAlias());
            }
            if (params.getMainTable().equals(relation.getAlias())) {
                mainFound = true;
            }
        }

        if (!mainFound) {
            throw new IllegalArgumentException("mainTable does not match any relation table");
        }

        for (JoinEdgeSpec join : params.getJoins()) {
            if (join == null) {
                throw new IllegalArgumentException("joins contains null entry");
            }
            if (isBlank(join.getLeftAlias()) || isBlank(join.getLeftField())
                    || isBlank(join.getRightAlias()) || isBlank(join.getRightField())) {
                throw new IllegalArgumentException("Each join must define leftAlias, leftField, rightAlias, rightField");
            }

            if (!aliases.contains(join.getLeftAlias())) {
                throw new IllegalArgumentException("Join references unknown alias: " + join.getLeftAlias());
            }
            if (!aliases.contains(join.getRightAlias())) {
                throw new IllegalArgumentException("Join references unknown alias: " + join.getRightAlias());
            }
        }
    }

    private static boolean isBlank(String s) {
        return s == null || s.trim().isEmpty();
    }

    /* JSON request should be looking like this
        * {
      "StreamID": "tpch_sf1_qy",
      "requestID": 1,
      "synopsisID": 30,
      "UID": 1001,
      "streamID": "tpch",
      "param": ["onepass-star"],
      "noOfP": 1,
      "parameters": {
        "onePassParams": {
          "queryName": "QY",
          "mainTable": "l1",
          "dataset": {
            "name": "tpch",
            "dbConfig": "tpch.json",
            "scaleFactor": 1,
            "seed": "test123"
          },
          "relations": [
            { "table": "lineitem", "alias": "l1" },
            { "table": "orders",   "alias": "o1" },
            { "table": "customer", "alias": "c1" },
            { "table": "lineitem", "alias": "l2" },
            { "table": "orders",   "alias": "o2" },
            { "table": "customer", "alias": "c2" },
            { "table": "supplier", "alias": "s" }
          ],
          "joins": [
            { "leftAlias": "l1", "leftField": "l_orderkey",  "rightAlias": "o1", "rightField": "o_orderkey" },
            { "leftAlias": "o1", "leftField": "o_custkey",   "rightAlias": "c1", "rightField": "c_custkey" },
            { "leftAlias": "l1", "leftField": "l_partkey",   "rightAlias": "l2", "rightField": "l_partkey" },
            { "leftAlias": "l2", "leftField": "l_orderkey",  "rightAlias": "o2", "rightField": "o_orderkey" },
            { "leftAlias": "o2", "leftField": "o_custkey",   "rightAlias": "c2", "rightField": "c_custkey" },
            { "leftAlias": "c1", "leftField": "c_nationkey", "rightAlias": "s",  "rightField": "s_nationkey" },
            { "leftAlias": "s",  "leftField": "s_nationkey", "rightAlias": "c2", "rightField": "c_nationkey" }
          ],
          "weight": {
            "expression": "((e1*(1-d1))*t1*(e2*(1-d2))*t2)",
            "variables": ["e1", "d1", "t1", "e2", "d2", "t2"]
          },
          "output": {
            "sampleSize": 1000000,
            "projection": [
              "l1.l_linenumber",
              "o1.o_orderkey",
              "c1.c_custkey",
              "l2.l_linenumber",
              "o2.o_orderkey",
              "s.s_suppkey",
              "c2.c_custkey",
              "l2.l_partkey",
              "c1.c_nationkey",
              "c2.c_nationkey"
            ]
          }
        }
      }
    }
    *
    * */
}