package dev.jonathanb.cs386d;

import java.util.Map;

// Nulls excluded in other fields
// histogram excludes mostCommon
public record ColumnStats(double fractionNull, double nDistinct, Map<HistogramValue, Double> mostCommon/*, List<HistogramRange> histogram*/) {
    double fractionNotCommon() {
        double frac = 1 - fractionNull;
        if (mostCommon != null) {
            frac -= mostCommon.values().stream().mapToDouble(Double::doubleValue).sum();
        }
        return frac;
    }

//    double fractionInEachBucket() {
//        return fractionInHistogram() / (histogram.size() - 1);
//    }

    double nDistinctNotCommon() {
        if (mostCommon != null) {
            return nDistinct - mostCommon.size();
        }
        return nDistinct;
    }

//    double nDistinctInEachBucket() {
//        return nDistinctInHistogram() / (histogram.size() - 1);
//    }

    double semijoinSelectivityAgainst(ColumnStats join) {
        double totalFrequencyOfOverlap = 0;
        double unmappedFraction = 1;
        double nDistinctMine = nDistinct, nDistinctTheirs = join.nDistinct;
        for (Map.Entry<HistogramValue, Double> myEntry : mostCommon.entrySet()) {
            Double theirFreq = join.mostCommon.get(myEntry.getKey());
            if (theirFreq != null) {
                totalFrequencyOfOverlap += theirFreq;
                unmappedFraction -= myEntry.getValue();
                nDistinctMine--;
                nDistinctTheirs--;
            }
        }

        // TODO: check that I got this right
        double semijoinSelectivityLeftoverPart = unmappedFraction / Math.min(nDistinctMine, nDistinctTheirs);

        return totalFrequencyOfOverlap + semijoinSelectivityLeftoverPart;
    }
}
