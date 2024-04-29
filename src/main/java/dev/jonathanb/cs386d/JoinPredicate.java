package dev.jonathanb.cs386d;

import java.util.*;
import java.util.stream.Collectors;

// This optimizer only handles conjunctions of equality predicates.
// The benchmark system applies all other predicates using views.
public record JoinPredicate(Column a, Column b) {

    @Override
    public String toString() {
        return a + " = " + b;
    }

    public RelationStats apply(RelationStats stats) {
        ColumnStats aStats = stats.columnStats().get(a);
        ColumnStats bStats = stats.columnStats().get(b);

        // reminder: joins ignore nulls
        double count = 0;

        Map<HistogramValue, Double> mostCommonCounts = new HashMap<>();
        for (Map.Entry<HistogramValue, Double> aElem : aStats.mostCommon().entrySet()) {
            if (bStats.mostCommon().containsKey(aElem.getKey())) {
                double elemCount = aElem.getValue() * bStats.mostCommon().get(aElem.getKey()) * count;
                count += elemCount;
                mostCommonCounts.put(aElem.getKey(), elemCount);
            }
        }
        double nDistinct = mostCommonCounts.size();

        // Idea 1:
        // Split both sides into ranges (for now ignore most common elements)
        // For each range in A, find the overlapping ranges in B
        // P[A range & B range] = P[A range] * P[B range] ?

        // Idea 2:
        // Get cross product of ranges
        // Discard any that don't overlap
        // Each double-range has f^2 fraction of the items
        // ...

        // Textbook model assumes that whichever has fewer distinct values has all of those show up.

//        TreeSet<HistogramValue> histogramAnchorsSet = new TreeSet<>();
//        for (HistogramRange aRange : aStats.histogram()) {
//            histogramAnchorsSet.add(aRange.lowerBound());
//            histogramAnchorsSet.add(aRange.upperBound());
//        }
//        for (HistogramRange bRange : bStats.histogram()) {
//            histogramAnchorsSet.add(bRange.lowerBound());
//            histogramAnchorsSet.add(bRange.upperBound());
//        }
//        List<HistogramRange> aRangesSplit = splitHistogram(histogramAnchorsSet, aStats.histogram());
//        List<HistogramRange> bRangesSplit = splitHistogram(histogramAnchorsSet, bStats.histogram());
//        // TODO: merge the two ranges, then re-weight them.

        final double countFinal = count;
        ColumnStats mergedStats = new ColumnStats(0, nDistinct,
                mostCommonCounts.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, elem -> elem.getValue() / countFinal)));

        Map<Column, ColumnStats> updatedColumns = new HashMap<>();
        for (Map.Entry<Column, ColumnStats> entry : stats.columnStats().entrySet()) {
            if (entry.getKey().equals(a) || entry.getKey().equals(b)) {
                updatedColumns.put(entry.getKey(), mergedStats);
            } else {
                updatedColumns.put(entry.getKey(), entry.getValue());
            }
        }
        return new RelationStats(count, updatedColumns);
    }

//    private static List<HistogramRange> splitHistogram(TreeSet<HistogramValue> values, Collection<HistogramRange> ranges) {
//        Iterator<HistogramValue> valuesIt = values.iterator();
//        Iterator<HistogramRange> rangesIt = ranges.iterator();
//        HistogramValue prevValue = valuesIt.next();
//        HistogramRange currRange = rangesIt.next();
//        List<HistogramRange> split = new ArrayList<>();
//        while (prevValue.compareTo(currRange.lowerBound()) < 0) {
//            prevValue = valuesIt.next();
//        }
//        while (valuesIt.hasNext()) {
//            HistogramValue nextValue = valuesIt.next();
//            while (nextValue.compareTo(currRange.upperBound()) > 0) {
//                split.add(currRange.subRange(prevValue, currRange.upperBound()));
//                prevValue = currRange.upperBound();
//                if (!rangesIt.hasNext()) {
//                    return split;
//                }
//                currRange = rangesIt.next();
//            }
//            split.add(currRange.subRange(prevValue, nextValue));
//            prevValue = nextValue;
//        }
//        return split;
//    }
}
