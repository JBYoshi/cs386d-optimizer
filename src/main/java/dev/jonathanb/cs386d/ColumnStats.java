package dev.jonathanb.cs386d;

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
}
