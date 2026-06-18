package infore.SDE.transformations.onepass.sql;

public final class OnePassSqlRequest {

    private final String queryName;
    private final String rootAlias;
    private final String weightOverride;
    private final int sampleSize;
    private final String catalogRef;
    private final String seed;
    private final int scaleFactor;

    public OnePassSqlRequest(String queryName,
                             String rootAlias,
                             String weightOverride,
                             int sampleSize,
                             String catalogRef,
                             String seed,
                             int scaleFactor) {
        this.queryName = queryName;
        this.rootAlias = rootAlias;
        this.weightOverride = weightOverride;
        this.sampleSize = sampleSize;
        this.catalogRef = catalogRef;
        this.seed = seed;
        this.scaleFactor = scaleFactor;
    }

    public String getQueryName() {
        return queryName;
    }

    public String getRootAlias() {
        return rootAlias;
    }

    public String getWeightOverride() {
        return weightOverride;
    }

    public int getSampleSize() {
        return sampleSize;
    }

    public String getCatalogRef() {
        return catalogRef;
    }

    public String getSeed() {
        return seed;
    }

    public int getScaleFactor() {
        return scaleFactor;
    }
}