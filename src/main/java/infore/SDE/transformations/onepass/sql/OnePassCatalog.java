package infore.SDE.transformations.onepass.sql;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import infore.SDE.messages.Onepass.JoinEdgeSpec;
import infore.SDE.messages.Onepass.RelationSpec;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class OnePassCatalog {

    private CatalogDataset dataset;
    private Map<String, CatalogQuery> queries = new LinkedHashMap<String, CatalogQuery>();

    public CatalogDataset getDataset() {
        return dataset;
    }

    public void setDataset(CatalogDataset dataset) {
        this.dataset = dataset;
    }

    public Map<String, CatalogQuery> getQueries() {
        return queries;
    }

    public void setQueries(Map<String, CatalogQuery> queries) {
        this.queries = queries;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class CatalogDataset {
        private String name;
        private int defaultScaleFactor = 1;
        private Map<String, CatalogTable> tables = new LinkedHashMap<String, CatalogTable>();

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getDefaultScaleFactor() {
            return defaultScaleFactor;
        }

        public void setDefaultScaleFactor(int defaultScaleFactor) {
            this.defaultScaleFactor = defaultScaleFactor;
        }

        public Map<String, CatalogTable> getTables() {
            return tables;
        }

        public void setTables(Map<String, CatalogTable> tables) {
            this.tables = tables;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class CatalogTable {
        private String file;
        private String separator = "|";
        private List<String> columns = new ArrayList<String>();

        public String getFile() {
            return file;
        }

        public void setFile(String file) {
            this.file = file;
        }

        public String getSeparator() {
            return separator;
        }

        public void setSeparator(String separator) {
            this.separator = separator;
        }

        public List<String> getColumns() {
            return columns;
        }

        public void setColumns(List<String> columns) {
            this.columns = columns;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class CatalogQuery {
        private String description;
        private String defaultRoot;
        private List<RelationSpec> relations = new ArrayList<RelationSpec>();
        private List<JoinEdgeSpec> joins = new ArrayList<JoinEdgeSpec>();
        private List<String> projection = new ArrayList<String>();
        private Map<String, String> defaultWeightsByAlias =
                new LinkedHashMap<String, String>();

        public String getDescription() {
            return description;
        }

        public void setDescription(String description) {
            this.description = description;
        }

        public String getDefaultRoot() {
            return defaultRoot;
        }

        public void setDefaultRoot(String defaultRoot) {
            this.defaultRoot = defaultRoot;
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

        public List<String> getProjection() {
            return projection;
        }

        public void setProjection(List<String> projection) {
            this.projection = projection;
        }

        public Map<String, String> getDefaultWeightsByAlias() {
            return defaultWeightsByAlias;
        }

        public void setDefaultWeightsByAlias(Map<String, String> defaultWeightsByAlias) {
            this.defaultWeightsByAlias = defaultWeightsByAlias;
        }
    }
}