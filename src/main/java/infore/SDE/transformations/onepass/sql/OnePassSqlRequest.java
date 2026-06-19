package infore.SDE.transformations.onepass.sql;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class OnePassSqlRequest {

    private final List<String> projection;
    private final String queryName;
    private final String rootAlias;
    private final String weightOverride;
    private final int sampleSize;
    private final String catalogRef;
    private final String seed;
    private final int scaleFactor;

    /*
     * Backwards-compatible constructor.
     * If older code creates OnePassSqlRequest without projection,
     * it behaves like SELECT *.
     */
    public OnePassSqlRequest(String queryName,
                             String rootAlias,
                             String weightOverride,
                             int sampleSize,
                             String catalogRef,
                             String seed,
                             int scaleFactor) {
        this(
                Collections.singletonList("*"),
                queryName,
                rootAlias,
                weightOverride,
                sampleSize,
                catalogRef,
                seed,
                scaleFactor
        );
    }

    public OnePassSqlRequest(List<String> projection,
                             String queryName,
                             String rootAlias,
                             String weightOverride,
                             int sampleSize,
                             String catalogRef,
                             String seed,
                             int scaleFactor) {

        if (projection == null || projection.isEmpty()) {
            this.projection = Collections.singletonList("*");
        } else {
            this.projection = Collections.unmodifiableList(
                    new ArrayList<String>(projection)
            );
        }

        this.queryName = queryName;
        this.rootAlias = rootAlias;
        this.weightOverride = weightOverride;
        this.sampleSize = sampleSize;
        this.catalogRef = catalogRef;
        this.seed = seed;
        this.scaleFactor = scaleFactor;
    }

    public List<String> getProjection() {
        return projection;
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