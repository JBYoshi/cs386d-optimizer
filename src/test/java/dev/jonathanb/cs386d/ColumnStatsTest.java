package dev.jonathanb.cs386d;

import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class ColumnStatsTest {
    @Test
    public void testSemijoinSelectivityNoFrequent() {
        ColumnStats left = new ColumnStats(0, 5, Map.of());
        ColumnStats right = new ColumnStats(0, 10, Map.of());
        assertEquals(1, left.semijoinSelectivityAgainst(right), 0.001);
        assertEquals(0.5, right.semijoinSelectivityAgainst(left), 0.001);
    }

    @Test
    public void testSemijoinSelectivityBothFrequentOnly() {
        ColumnStats left = new ColumnStats(0, 3,
                Map.of(new HistogramValue("A"), 0.5, new HistogramValue("B"), 0.2, new HistogramValue("C"), 0.3));
        ColumnStats right = new ColumnStats(0, 3,
                Map.of(new HistogramValue("B"), 0.25, new HistogramValue("C"), 0.35, new HistogramValue("D"), 0.4));
        assertEquals(0.5, left.semijoinSelectivityAgainst(right), 0.001);
        assertEquals(0.6, right.semijoinSelectivityAgainst(left), 0.001);
    }

    @Test
    public void testSemijoinSelectivityEqualMix() {
        ColumnStats left = new ColumnStats(0, 4,
                Map.of(new HistogramValue("A"), 0.4, new HistogramValue("B"), 0.5));
        ColumnStats right = new ColumnStats(0, 4,
                Map.of(new HistogramValue("B"), 0.25, new HistogramValue("C"), 0.3));
        // Both are 1 because each side has 4 elements.
        assertEquals(1, left.semijoinSelectivityAgainst(right), 0.001);
        assertEquals(1, right.semijoinSelectivityAgainst(left), 0.001);
    }

    @Test
    public void testSemijoinSelectivityUnEqualMix() {
        ColumnStats left = new ColumnStats(0, 10,
                Map.of(new HistogramValue("A"), 0.4, new HistogramValue("B"), 0.5));
        ColumnStats right = new ColumnStats(0, 4,
                Map.of(new HistogramValue("B"), 0.25, new HistogramValue("C"), 0.3));

        // Half from the match with B, so the other half is a 3 versus 9 join.
        assertEquals(0.5 + (3.0 / 9 / 2), left.semijoinSelectivityAgainst(right), 0.001);
        assertEquals(1, right.semijoinSelectivityAgainst(left), 0.001);
    }

    @Test
    public void testSemijoinSelectivityOneFrequentOnly() {
        ColumnStats left = new ColumnStats(0, 3,
                Map.of(new HistogramValue("A"), 0.4, new HistogramValue("B"), 0.35, new HistogramValue("C"), 0.25));
        ColumnStats right = new ColumnStats(0, 10,
                Map.of(new HistogramValue("B"), 0.25, new HistogramValue("C"), 0.1, new HistogramValue("D"), 0.1));
        // 0.6 because B and C match, A does not
        assertEquals(0.6, left.semijoinSelectivityAgainst(right), 0.001);
        // 0.35 from histogram exact match, 0.65 * 1/8 from rest
        assertEquals(0.35 + 0.65 / 8, right.semijoinSelectivityAgainst(left), 0.001);
    }
}
