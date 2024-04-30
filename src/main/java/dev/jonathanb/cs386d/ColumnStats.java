package dev.jonathanb.cs386d;

import java.util.HashMap;
import java.util.Map;

// Nulls excluded in other fields
// histogram excludes mostCommon
public record ColumnStats(double fractionNull, long nDistinct, Map<HistogramValue, Double> mostCommon/*, List<HistogramRange> histogram*/) {
    public double semijoinSelectivityAgainst(ColumnStats join) {
        double totalFrequencyOfOverlap = 0;
        double unmappedFraction = 1 - fractionNull;
        long unmappedNDistinctMine = nDistinct, unmappedNDistinctTheirs = join.nDistinct;

        // If a histogram entry from here is in the other relation, then that section is automatically kept.
        for (Map.Entry<HistogramValue, Double> myEntry : mostCommon.entrySet()) {
            if (join.mostCommon.containsKey(myEntry.getKey())) {
                totalFrequencyOfOverlap += myEntry.getValue();
                unmappedFraction -= myEntry.getValue();
                unmappedNDistinctMine--;
                unmappedNDistinctTheirs--;
            }
        }

        // The rest (if there is any) follows the original model.
        double semijoinSelectivityLeftoverPart = 0;
        if (unmappedNDistinctMine > 0 && mostCommon.size() < nDistinct) {
            semijoinSelectivityLeftoverPart = unmappedFraction * Math.min((double) unmappedNDistinctTheirs / unmappedNDistinctMine, 1);
        }

        return totalFrequencyOfOverlap + semijoinSelectivityLeftoverPart;
    }

    public ColumnStats semijoinStatsAgainst(ColumnStats join) {
        double discardedFraction = fractionNull;
        Map<HistogramValue, Double> newMostCommon = new HashMap<>();
        long discardedDistinct = 0;
        for (Map.Entry<HistogramValue, Double> myEntry : mostCommon.entrySet()) {
            if (join.mostCommon.containsKey(myEntry.getKey())) {
                newMostCommon.put(myEntry.getKey(), myEntry.getValue());
            } else {
                discardedFraction += myEntry.getValue();
                discardedDistinct++;
            }
        }

        for (Map.Entry<HistogramValue, Double> entry : newMostCommon.entrySet()) {
            // Note that this element was not discarded.
            // New elem count = old elem count
            // New frequency * new total = old frequency * old total
            // New total = old total * (1 - discarded fraction)
            // New frequency * old total * (1 - discarded fraction) = old frequency * old total
            // New frequency * (1 - discarded fraction) = old frequency
            entry.setValue(entry.getValue() / (1 - discardedFraction));
        }
        return new ColumnStats(0, nDistinct - discardedDistinct, newMostCommon);
    }
}
