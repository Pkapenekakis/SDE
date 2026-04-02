package infore.SDE.synopses.OnePassSampler.PhaseOne;

import com.fasterxml.jackson.databind.JsonNode;
import infore.SDE.synopses.OnePassSampler.OnePassTuple;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Canonical join key representation for single-field and composite joins.
 * Stored as ordered string components so that parent-side and child-side
 * lookups can match on the same logical key.
 */
public final class JoinValue implements Serializable {

    private static final long serialVersionUID = 1L;

    private final List<String> parts;

    public JoinValue(List<String> parts) {
        if (parts == null || parts.isEmpty()) {
            throw new IllegalArgumentException("JoinValue parts must not be null or empty");
        }
        this.parts = Collections.unmodifiableList(new ArrayList<String>(parts));
    }

    public static JoinValue ofSingle(String value) {
        return new JoinValue(Collections.singletonList(value));
    }

    //Extracts the join key from a tuple using the field names defined by the compiled plan
    public static JoinValue fromTuple(OnePassTuple tuple, List<String> fieldNames) {
        if (tuple == null) {
            throw new IllegalArgumentException("tuple must not be null");
        }
        if (fieldNames == null || fieldNames.isEmpty()) {
            throw new IllegalArgumentException("fieldNames must not be null or empty");
        }

        List<String> values = new ArrayList<String>(fieldNames.size());
        for (String field : fieldNames) {
            JsonNode node = tuple.getField(field);
            if (node == null || node.isNull()) {
                throw new IllegalArgumentException(
                        "Missing required join field '" + field + "' in tuple: " + tuple);
            }
            values.add(node.asText());
        }
        return new JoinValue(values);
    }

    public List<String> getParts() {
        return parts;
    }

    @Override
    public String toString() {
        if (parts.size() == 1) {
            return parts.get(0);
        }
        return String.join("|", parts);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof JoinValue)) {
            return false;
        }
        JoinValue other = (JoinValue) o;
        return Objects.equals(parts, other.parts);
    }

    @Override
    public int hashCode() {
        return Objects.hash(parts);
    }
}