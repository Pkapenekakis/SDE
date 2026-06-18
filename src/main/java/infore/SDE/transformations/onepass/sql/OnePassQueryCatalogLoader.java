package infore.SDE.transformations.onepass.sql;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.InputStream;

public final class OnePassQueryCatalogLoader {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private OnePassQueryCatalogLoader() {
    }

    public static OnePassCatalog load(String catalogRef) {
        if (isBlank(catalogRef)) {
            catalogRef = "tpch-onepass-catalog.json";
        }

        try {
            File directFile = new File(catalogRef);
            if (directFile.exists()) {
                return MAPPER.readValue(directFile, OnePassCatalog.class);
            }

            File relativeFile = new File("src/main/resources/onepass", catalogRef);
            if (relativeFile.exists()) {
                return MAPPER.readValue(relativeFile, OnePassCatalog.class);
            }

            String resourcePath = catalogRef.startsWith("onepass/")
                    ? catalogRef
                    : "onepass/" + catalogRef;

            InputStream in =
                    Thread.currentThread().getContextClassLoader().getResourceAsStream(resourcePath);

            if (in == null) {
                throw new IllegalArgumentException(
                        "Could not find OnePass catalog: " + catalogRef +
                                ". Tried direct file, src/main/resources/onepass, and classpath resource " +
                                resourcePath
                );
            }

            try {
                return MAPPER.readValue(in, OnePassCatalog.class);
            } finally {
                in.close();
            }
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Failed to load OnePass catalog '" + catalogRef + "': " + e.getMessage(),
                    e
            );
        }
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}