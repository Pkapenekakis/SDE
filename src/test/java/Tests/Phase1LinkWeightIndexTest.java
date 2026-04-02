package Tests;

import infore.SDE.synopses.OnePassSampler.PhaseOne.JoinValue;
import infore.SDE.synopses.OnePassSampler.PhaseOne.Phase1LinkWeightIndex;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;

public class Phase1LinkWeightIndexTest {

    @Test
    public void testAddAndLookup() {
        Phase1LinkWeightIndex index = new Phase1LinkWeightIndex("B<->C");

        JoinValue k1 = new JoinValue(Arrays.asList("k1"));
        JoinValue k2 = new JoinValue(Arrays.asList("k2"));

        index.add(k1, 5.0);
        index.add(k1, 8.0);
        index.add(k2, 1.0);

        assertEquals(13.0, index.getOrZero(k1), 1e-9);
        assertEquals(1.0, index.getOrZero(k2), 1e-9);
        assertEquals(0.0, index.getOrZero(new JoinValue(Arrays.asList("k3"))), 1e-9);
    }

    @Test
    public void testMerge() {
        Phase1LinkWeightIndex left = new Phase1LinkWeightIndex("B<->C");
        Phase1LinkWeightIndex right = new Phase1LinkWeightIndex("B<->C");

        JoinValue k1 = new JoinValue(Arrays.asList("k1"));
        JoinValue k2 = new JoinValue(Arrays.asList("k2"));

        left.add(k1, 10.0);
        right.add(k1, 4.0);
        right.add(k2, 7.0);

        left.mergeFrom(right);

        assertEquals(14.0, left.getOrZero(k1), 1e-9);
        assertEquals(7.0, left.getOrZero(k2), 1e-9);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWrongEdgeMergeFails() {
        Phase1LinkWeightIndex left = new Phase1LinkWeightIndex("B<->C");
        Phase1LinkWeightIndex right = new Phase1LinkWeightIndex("A<->B");

        left.mergeFrom(right);
    }
}