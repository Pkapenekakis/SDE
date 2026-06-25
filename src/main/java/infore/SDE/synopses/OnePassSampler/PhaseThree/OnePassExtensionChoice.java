package infore.SDE.synopses.OnePassSampler.PhaseThree;

import infore.SDE.synopses.OnePassSampler.OnePassTuple;

import java.io.Serializable;
import java.util.Random;

/**
 * One-pass weighted choice for one sample instance and one child alias.
 *
 * If matching candidates x1, x2, ..., xk are replayed from the side stream,
 * this keeps exactly one selected tuple with probability proportional to
 * candidateWeight.
 */
public final class OnePassExtensionChoice implements Serializable {

    private static final long serialVersionUID = 1L;

    private double cumulativeWeight;
    private long candidatesSeen;
    private OnePassTuple selectedTuple;
    private double selectedWeight;

    public OnePassExtensionChoice() {
        this.cumulativeWeight = 0.0d;
        this.candidatesSeen = 0L;
        this.selectedTuple = null;
        this.selectedWeight = 0.0d;
    }

    public void consider(OnePassTuple tuple, double candidateWeight, Random random) {
        if (tuple == null) {
            throw new IllegalArgumentException("tuple must not be null");
        }

        if (random == null) {
            throw new IllegalArgumentException("random must not be null");
        }

        validatePositiveFinite(candidateWeight, "candidateWeight");

        double newCumulativeWeight = checkedAdd(cumulativeWeight, candidateWeight);

        /*
         * Weighted reservoir for one selected item:
         *
         * Replace current selection with probability:
         *
         *     candidateWeight / newCumulativeWeight
         */
        double replaceProbability = candidateWeight / newCumulativeWeight;

        if (selectedTuple == null || random.nextDouble() < replaceProbability) {
            selectedTuple = tuple;
            selectedWeight = candidateWeight;
        }

        cumulativeWeight = newCumulativeWeight;
        candidatesSeen++;
    }

    public boolean hasSelection() {
        return selectedTuple != null;
    }

    public OnePassTuple getSelectedTuple() {
        return selectedTuple;
    }

    public double getSelectedWeight() {
        return selectedWeight;
    }

    public double getCumulativeWeight() {
        return cumulativeWeight;
    }

    public long getCandidatesSeen() {
        return candidatesSeen;
    }

    private static void validatePositiveFinite(double value,String label) {
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            throw new IllegalArgumentException(label + " must be finite: " + value);
        }

        if (value <= 0.0d) {
            throw new IllegalArgumentException(label + " must be positive: " + value);
        }
    }

    private static double checkedAdd(double left, double right) {
        double result = left + right;

        if (Double.isNaN(result) || Double.isInfinite(result)) {
            throw new IllegalArgumentException("Invalid cumulative weight: " + left + " + " + right + " = " + result);
        }

        return result;
    }
}