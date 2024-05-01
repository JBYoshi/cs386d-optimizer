package dev.jonathanb.cs386d;

import org.junit.Test;

import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class ColumnStatsTest {
    @Test
    public void testJoinNoFrequent() {
        ColumnStats left = new ColumnStats(0, 5, Map.of());
        ColumnStats right = new ColumnStats(0, 10, Map.of());
        assertEquals(1, left.semijoin(right).selectivity(), 0.001);
        assertEquals(0.5, right.semijoin(left).selectivity(), 0.001);
        // Say there is one entry of each value. Left has 1-5, right has 1-10.
        // Cross product gives 50 tuples, of which 5 match. So it should be 0.1.
        assertEquals(0.1, left.join(right).selectivity(), 0.001);
        assertEquals(0.1, right.join(left).selectivity(), 0.001);

        assertEquals(new ColumnStats(0, 5, Map.of()), left.join(right).newStats());
    }

    @Test
    public void testBothFrequentOnly() {
        ColumnStats left = new ColumnStats(0, 3,
                Map.of(new HistogramValue("A"), 0.5, new HistogramValue("B"), 0.2, new HistogramValue("C"), 0.3));
        ColumnStats right = new ColumnStats(0, 3,
                Map.of(new HistogramValue("B"), 0.25, new HistogramValue("C"), 0.35, new HistogramValue("D"), 0.4));
        assertEquals(0.5, left.semijoin(right).selectivity(), 0.001);
        assertEquals(0.6, right.semijoin(left).selectivity(), 0.001);

        assertEquals(0.2 * 0.25 + 0.3 * 0.35, left.join(right).selectivity(), 0.001);
        assertEquals(0.2 * 0.25 + 0.3 * 0.35, right.join(left).selectivity(), 0.001);

        ColumnStats joinStats = left.join(right).newStats();
        assertEquals(2, joinStats.nDistinct());
        assertEquals(0, joinStats.fractionNull(), 0.001);
        assertEquals(0.2 * 0.25 / (0.2 * 0.25 + 0.3 * 0.35), joinStats.mostCommon().get(new HistogramValue("B")), 0.001);
        assertEquals(0.3 * 0.35 / (0.2 * 0.25 + 0.3 * 0.35), joinStats.mostCommon().get(new HistogramValue("C")), 0.001);
        assertEquals(Set.of(new HistogramValue("B"), new HistogramValue("C")), joinStats.mostCommon().keySet());
    }

    @Test
    public void testJoinEqualMix() {
        ColumnStats left = new ColumnStats(0, 4,
                Map.of(new HistogramValue("A"), 0.4, new HistogramValue("B"), 0.5));
        ColumnStats right = new ColumnStats(0, 4,
                Map.of(new HistogramValue("B"), 0.25, new HistogramValue("C"), 0.3));
        // Both are 1 because each side has 4 elements.
        assertEquals(1, left.semijoin(right).selectivity(), 0.001);
        assertEquals(1, right.semijoin(left).selectivity(), 0.001);

        // A gets 0.4 * 0.45 / 2
        // B gets 0.5 * 0.25
        // C gets 0.3 * 0.1 / 2
        // D gets 0.45 / 2 * 0.1 / 2
        assertEquals(0.4 * 0.45 / 2 + 0.5 * 0.25 + 0.3 * 0.1 / 2 + 0.45 / 2 * 0.1 * 2, left.join(right).selectivity(), 0.001);
        assertEquals(0.4 * 0.45 / 2 + 0.5 * 0.25 + 0.3 * 0.1 / 2 + 0.45 / 2 * 0.1 * 2, right.join(left).selectivity(), 0.001);
    }

    @Test
    public void testJoinUnEqualMix() {
        ColumnStats left = new ColumnStats(0, 10,
                Map.of(new HistogramValue("A"), 0.4, new HistogramValue("B"), 0.5));
        ColumnStats right = new ColumnStats(0, 4,
                Map.of(new HistogramValue("B"), 0.25, new HistogramValue("C"), 0.3));

        // Half from the match with B, so the other half is a 3 versus 9 join.
        assertEquals(0.5 + (3.0 / 9 / 2), left.semijoin(right).selectivity(), 0.001);
        assertEquals(1, right.semijoin(left).selectivity(), 0.001);

        // A gets 0.4 * (0.45 / 2) * (3/9) = left.value * right.fractionUnmapped / right.nDistinctUnmapped * left.nDistinct / right.nDistinct
        // B gets 0.5 * 0.25
        // C gets (0.1 / 8) * 0.3 = right.value * left.fractionUnmapped / left.nDistinctUnmapped
        // Rest gets (0.1 / 8) * (0.45 / 2) * 2 = 0.1 * 0.45 / 8 = left.fractionUnmapped * right.fractionUnmapped / max(left.distinctUnmapped, right.distinctUnmapped)
        assertEquals(0.4 * (0.45 / 2) * (3.0 / 9)
                + 0.5 * 0.25
                + (0.1 / 8) * 0.3
                + 0.1 * 0.45 / 8,
                left.join(right).selectivity(), 0.001);
        assertEquals(0.4 * (0.45 / 2) * (3.0 / 9)
                + 0.5 * 0.25
                + (0.1 / 8) * 0.3
                + 0.1 * 0.45 / 8, right.join(left).selectivity(), 0.001);
    }

    @Test
    public void testJoinOneFrequentOnly() {
        ColumnStats left = new ColumnStats(0, 3,
                Map.of(new HistogramValue("A"), 0.4, new HistogramValue("B"), 0.35, new HistogramValue("C"), 0.25));
        ColumnStats right = new ColumnStats(0, 10,
                Map.of(new HistogramValue("B"), 0.25, new HistogramValue("C"), 0.1, new HistogramValue("D"), 0.1));
        // 0.6 because B and C match, A does not
        assertEquals(0.6, left.semijoin(right).selectivity(), 0.001);
        // 0.35 from histogram exact match, 0.65 * 1/8 from rest
        assertEquals(0.35 + 0.65 / 8, right.semijoin(left).selectivity(), 0.001);

        // B is 0.35 * 0.25, C is 0.25 * 0.1, A is 0.4 * 0.55 (the leftover part)
        assertEquals(0.35 * 0.25 + 0.25 * 0.1 + 0.4 * 0.55 / 7, left.join(right).selectivity(), 0.00001);
    }

    @Test
    public void testJoin2x2() {
        ColumnStats left = new ColumnStats(0, 2, Map.of(new HistogramValue("A"), 0.5));
        ColumnStats right = new ColumnStats(0, 2, Map.of(new HistogramValue("B"), 0.5));
        // Since we assume maximum overlap, this effectively means both are half A and half B.
        // Full semijoin selectivity, and half join selectivity (keep AA and BB, not AB or BA).
        assertEquals(1, left.semijoin(right).selectivity(), 0.001);
        assertEquals(1, right.semijoin(left).selectivity(), 0.001);
        assertEquals(0.5, left.join(right).selectivity(), 0.001);
        assertEquals(0.5, right.join(left).selectivity(), 0.001);

        ColumnStats expectedJoinStats = new ColumnStats(0, 2, Map.of());
        assertEquals(expectedJoinStats, left.join(right).newStats());
        assertEquals(expectedJoinStats, right.join(left).newStats());
    }

    @Test
    public void testJoin3x3() {
        ColumnStats left = new ColumnStats(0, 3, Map.of(new HistogramValue("A"), 0.333, new HistogramValue("B"), 0.333));
        ColumnStats right = new ColumnStats(0, 3, Map.of(new HistogramValue("B"), 0.333, new HistogramValue("C"), 0.333));
        // Since we assume maximum overlap, this effectively means both are half A and half B.
        // Full semijoin selectivity, and half join selectivity (keep AA and BB, not AB or BA).
        assertEquals(1, left.semijoin(right).selectivity(), 0.001);
        assertEquals(1, right.semijoin(left).selectivity(), 0.001);
        assertEquals(0.3333, left.join(right).selectivity(), 0.001);
        assertEquals(0.3333, right.join(left).selectivity(), 0.001);

        ColumnStats joinStats = left.join(right).newStats();
        assertEquals(0, joinStats.fractionNull(), 0.001);
        assertEquals(3, joinStats.nDistinct());
        assertEquals(0.3333, joinStats.mostCommon().get(new HistogramValue("B")), 0.001);
        assertEquals(Set.of(new HistogramValue("B")), joinStats.mostCommon().keySet());
    }


}
