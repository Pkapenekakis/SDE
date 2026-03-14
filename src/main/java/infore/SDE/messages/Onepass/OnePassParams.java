package infore.SDE.messages.Onepass;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.ArrayList;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class OnePassParams {

    private String queryName;
    private String mainTable;
    private DatasetConfig dataset;
    private List<RelationSpec> relations = new ArrayList<>();
    private List<JoinEdgeSpec> joins = new ArrayList<>();
    private WeightSpec weight;
    private OutputSpec output;

    public OnePassParams() {
    }

    public String getQueryName() {
        return queryName;
    }

    public void setQueryName(String queryName) {
        this.queryName = queryName;
    }

    public String getMainTable() {
        return mainTable;
    }

    public void setMainTable(String mainTable) {
        this.mainTable = mainTable;
    }

    public DatasetConfig getDataset() {
        return dataset;
    }

    public void setDataset(DatasetConfig dataset) {
        this.dataset = dataset;
    }

    public List<RelationSpec> getRelations() {
        return relations;
    }

    public void setRelations(List<RelationSpec> relations) {
        this.relations = relations;
    }

    public List<JoinEdgeSpec> getJoins() {
        return joins;
    }

    public void setJoins(List<JoinEdgeSpec> joins) {
        this.joins = joins;
    }

    public WeightSpec getWeight() {
        return weight;
    }

    public void setWeight(WeightSpec weight) {
        this.weight = weight;
    }

    public OutputSpec getOutput() {
        return output;
    }

    public void setOutput(OutputSpec output) {
        this.output = output;
    }
}