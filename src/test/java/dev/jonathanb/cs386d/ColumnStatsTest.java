package dev.jonathanb.cs386d;

import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class ColumnStatsTest {
    @Test
    public void testJoinNoFrequent() {
        ColumnStats left = new ColumnStats(0, 5, Map.of());
        ColumnStats right = new ColumnStats(0, 10, Map.of());
        assertEquals(1, left.semijoinSelectivityAgainst(right), 0.001);
        assertEquals(0.5, right.semijoinSelectivityAgainst(left), 0.001);
        // Say there is one entry of each value. Left has 1-5, right has 1-10.
        // Cross product gives 50 tuples, of which 5 match. So it should be 0.1.
        assertEquals(0.1, left.joinSelectivityAgainst(right), 0.001);
        assertEquals(0.1, right.joinSelectivityAgainst(left), 0.001);
    }

    @Test
    public void testBothFrequentOnly() {
        ColumnStats left = new ColumnStats(0, 3,
                Map.of(new HistogramValue("A"), 0.5, new HistogramValue("B"), 0.2, new HistogramValue("C"), 0.3));
        ColumnStats right = new ColumnStats(0, 3,
                Map.of(new HistogramValue("B"), 0.25, new HistogramValue("C"), 0.35, new HistogramValue("D"), 0.4));
        assertEquals(0.5, left.semijoinSelectivityAgainst(right), 0.001);
        assertEquals(0.6, right.semijoinSelectivityAgainst(left), 0.001);

        assertEquals(0.2 * 0.25 + 0.3 * 0.35, left.joinSelectivityAgainst(right), 0.001);
        assertEquals(0.2 * 0.25 + 0.3 * 0.35, right.joinSelectivityAgainst(left), 0.001);
    }

    @Test
    public void testJoinEqualMix() {
        ColumnStats left = new ColumnStats(0, 4,
                Map.of(new HistogramValue("A"), 0.4, new HistogramValue("B"), 0.5));
        ColumnStats right = new ColumnStats(0, 4,
                Map.of(new HistogramValue("B"), 0.25, new HistogramValue("C"), 0.3));
        // Both are 1 because each side has 4 elements.
        assertEquals(1, left.semijoinSelectivityAgainst(right), 0.001);
        assertEquals(1, right.semijoinSelectivityAgainst(left), 0.001);

        // A gets 0.4 * 0.45 / 2
        // B gets 0.5 * 0.25
        // C gets 0.3 * 0.1 / 2
        // D gets 0.45 / 2 * 0.1 / 2
        assertEquals(0.4 * 0.45 / 2 + 0.5 * 0.25 + 0.3 * 0.1 / 2 + 0.45 / 2 * 0.1 * 2, left.joinSelectivityAgainst(right), 0.001);
        assertEquals(0.4 * 0.45 / 2 + 0.5 * 0.25 + 0.3 * 0.1 / 2 + 0.45 / 2 * 0.1 * 2, right.joinSelectivityAgainst(left), 0.001);
    }

    @Test
    public void testJoinUnEqualMix() {
        ColumnStats left = new ColumnStats(0, 10,
                Map.of(new HistogramValue("A"), 0.4, new HistogramValue("B"), 0.5));
        ColumnStats right = new ColumnStats(0, 4,
                Map.of(new HistogramValue("B"), 0.25, new HistogramValue("C"), 0.3));

        // Half from the match with B, so the other half is a 3 versus 9 join.
        assertEquals(0.5 + (3.0 / 9 / 2), left.semijoinSelectivityAgainst(right), 0.001);
        assertEquals(1, right.semijoinSelectivityAgainst(left), 0.001);

        // A gets 0.4 * (0.45 / 2) * (3/9) = left.value * right.fractionUnmapped / right.nDistinctUnmapped * left.nDistinct / right.nDistinct
        // B gets 0.5 * 0.25
        // C gets (0.1 / 8) * 0.3 = right.value * left.fractionUnmapped / left.nDistinctUnmapped
        // Rest gets (0.1 / 8) * (0.45 / 2) * 2 = 0.1 * 0.45 / 8 = left.fractionUnmapped * right.fractionUnmapped / max(left.distinctUnmapped, right.distinctUnmapped)
        assertEquals(0.4 * (0.45 / 2) * (3.0 / 9)
                + 0.5 * 0.25
                + (0.1 / 8) * 0.3
                + 0.1 * 0.45 / 8,
                left.joinSelectivityAgainst(right), 0.001);
        assertEquals(0.4 * (0.45 / 2) * (3.0 / 9)
                + 0.5 * 0.25
                + (0.1 / 8) * 0.3
                + 0.1 * 0.45 / 8, right.joinSelectivityAgainst(left), 0.001);
    }

    @Test
    public void testJoinOneFrequentOnly() {
        ColumnStats left = new ColumnStats(0, 3,
                Map.of(new HistogramValue("A"), 0.4, new HistogramValue("B"), 0.35, new HistogramValue("C"), 0.25));
        ColumnStats right = new ColumnStats(0, 10,
                Map.of(new HistogramValue("B"), 0.25, new HistogramValue("C"), 0.1, new HistogramValue("D"), 0.1));
        // 0.6 because B and C match, A does not
        assertEquals(0.6, left.semijoinSelectivityAgainst(right), 0.001);
        // 0.35 from histogram exact match, 0.65 * 1/8 from rest
        assertEquals(0.35 + 0.65 / 8, right.semijoinSelectivityAgainst(left), 0.001);

        // B is 0.35 * 0.25, C is 0.25 * 0.1, A is 0.4 * 0.55 (the leftover part)
        assertEquals(0.35 * 0.25 + 0.25 * 0.1 + 0.4 * 0.55 / 7, left.joinSelectivityAgainst(right), 0.00001);
    }

    @Test
    public void testJoin2x2() {
        ColumnStats left = new ColumnStats(0, 2, Map.of(new HistogramValue("A"), 0.5));
        ColumnStats right = new ColumnStats(0, 2, Map.of(new HistogramValue("B"), 0.5));
        // Since we assume maximum overlap, this effectively means both are half A and half B.
        // Full semijoin selectivity, and half join selectivity (keep AA and BB, not AB or BA).
        assertEquals(1, left.semijoinSelectivityAgainst(right), 0.001);
        assertEquals(1, right.semijoinSelectivityAgainst(left), 0.001);
        assertEquals(0.5, left.joinSelectivityAgainst(right), 0.001);
        assertEquals(0.5, right.joinSelectivityAgainst(left), 0.001);
    }
}
