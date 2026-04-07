package infore.SDE.synopses.OnePassSampler.PhaseOne;

import infore.SDE.messages.Onepass.*;

import java.util.Arrays;

public class TestOnePassFixtures {

    private TestOnePassFixtures() { }

    public static OnePassParams simpleABCDPlan() {
        OnePassParams p = new OnePassParams();
        p.setQueryName("test-abcd");
        p.setMainTable("A");

        RelationSpec a = new RelationSpec(); a.setAlias("A"); a.setTable("A");
        RelationSpec b = new RelationSpec(); b.setAlias("B"); b.setTable("B");
        RelationSpec c = new RelationSpec(); c.setAlias("C"); c.setTable("C");
        RelationSpec d = new RelationSpec(); d.setAlias("D"); d.setTable("D");
        p.setRelations(Arrays.asList(a, b, c, d));

        JoinEdgeSpec ab = new JoinEdgeSpec();
        ab.setLeftAlias("A"); ab.setLeftField("ab_key");
        ab.setRightAlias("B"); ab.setRightField("ab_key");

        JoinEdgeSpec bc = new JoinEdgeSpec();
        bc.setLeftAlias("B"); bc.setLeftField("bc_key");
        bc.setRightAlias("C"); bc.setRightField("bc_key");

        JoinEdgeSpec cd = new JoinEdgeSpec();
        cd.setLeftAlias("C"); cd.setLeftField("cd_key");
        cd.setRightAlias("D"); cd.setRightField("cd_key");

        p.setJoins(Arrays.asList(ab, bc, cd));

        WeightSpec w = new WeightSpec();
        w.setExpression("weight");
        w.setVariables(Arrays.asList("weight"));
        p.setWeight(w);

        OutputSpec out = new OutputSpec();
        out.setSampleSize(10);
        p.setOutput(out);

        DatasetConfig datasetConfig = new DatasetConfig();
        p.setDataset(datasetConfig);

        return p;
    }

    public static OnePassParams cyclicABCPlan() {
        OnePassParams p = new OnePassParams();
        p.setQueryName("test-cycle");
        p.setMainTable("A");

        RelationSpec a = new RelationSpec(); a.setAlias("A"); a.setTable("A");
        RelationSpec b = new RelationSpec(); b.setAlias("B"); b.setTable("B");
        RelationSpec c = new RelationSpec(); c.setAlias("C"); c.setTable("C");
        p.setRelations(Arrays.asList(a, b, c));

        JoinEdgeSpec ab = new JoinEdgeSpec();
        ab.setLeftAlias("A"); ab.setLeftField("ab");
        ab.setRightAlias("B"); ab.setRightField("ab");

        JoinEdgeSpec bc = new JoinEdgeSpec();
        bc.setLeftAlias("B"); bc.setLeftField("bc");
        bc.setRightAlias("C"); bc.setRightField("bc");

        JoinEdgeSpec ca = new JoinEdgeSpec();
        ca.setLeftAlias("C"); ca.setLeftField("ca");
        ca.setRightAlias("A"); ca.setRightField("ca");

        p.setJoins(Arrays.asList(ab, bc, ca));

        WeightSpec w = new WeightSpec();
        w.setExpression("weight");
        p.setWeight(w);

        OutputSpec out = new OutputSpec();
        out.setSampleSize(10);
        p.setOutput(out);

        DatasetConfig datasetConfig = new DatasetConfig();
        p.setDataset(datasetConfig);

        return p;
    }
}