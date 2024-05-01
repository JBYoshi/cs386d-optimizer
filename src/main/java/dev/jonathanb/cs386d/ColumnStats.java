package dev.jonathanb.cs386d;

import java.util.HashMap;
import java.util.Map;

// Nulls excluded in other fields
// histogram excludes mostCommon
public record ColumnStats(double fractionNull, long nDistinct, Map<HistogramValue, Double> mostCommon/*, List<HistogramRange> histogram*/) {
    public double fractionUnmapped() {
        return 1 - fractionNull - mostCommon.values().stream().mapToDouble(Double::doubleValue).sum();
    }

    public long nDistinctUnmapped() {
        return nDistinct - mostCommon.size();
    }

    public double semijoinSelectivityAgainst(ColumnStats join) {
        double sharedFractionMine = 0;
        double unsharedFractionMine = 1 - fractionNull;
        long unsharedDistinctMine = nDistinct, unsharedDistinctTheirs = join.nDistinct;

        // If a histogram entry from here is in the other relation, then that section is automatically kept.
        for (Map.Entry<HistogramValue, Double> myEntry : mostCommon.entrySet()) {
            if (join.mostCommon.containsKey(myEntry.getKey())) {
                sharedFractionMine += myEntry.getValue();
                unsharedFractionMine -= myEntry.getValue();
                unsharedDistinctMine--;
                unsharedDistinctTheirs--;
            }
        }

        // The rest (if there is any) follows the original model.
        double semijoinSelectivityLeftoverPart = 0;
        if (unsharedDistinctMine > 0 && nDistinctUnmapped() > 0) { // TODO: one of these is redundant but I'm missing the brainpower to figure out which.
            semijoinSelectivityLeftoverPart = unsharedFractionMine * Math.min((double) unsharedDistinctTheirs / unsharedDistinctMine, 1);
        }

        return sharedFractionMine + semijoinSelectivityLeftoverPart;
    }

    public ColumnStats semijoinStatsAgainst(ColumnStats join) {
        double unsharedFractionMine = 1 - fractionNull;
        Map<HistogramValue, Double> newMostCommon = new HashMap<>();
        for (Map.Entry<HistogramValue, Double> myEntry : mostCommon.entrySet()) {
            if (join.mostCommon.containsKey(myEntry.getKey())) {
                newMostCommon.put(myEntry.getKey(), myEntry.getValue());
            } else {
                unsharedFractionMine -= myEntry.getValue();
            }
        }

        for (Map.Entry<HistogramValue, Double> entry : newMostCommon.entrySet()) {
            // Note that this element was not discarded.
            // New elem count = old elem count
            // New frequency * new total = old frequency * old total
            // New total = old total * (1 - discarded fraction)
            // New frequency * old total * (1 - discarded fraction) = old frequency * old total
            // New frequency * (1 - discarded fraction) = old frequency
            entry.setValue(entry.getValue() / unsharedFractionMine);
        }

        long newNDistinct;
        if (nDistinct == mostCommon.size()) {
            newNDistinct = newMostCommon.size();
        } else {
            newNDistinct = Math.min(nDistinct, join.nDistinct);
        }
        return new ColumnStats(0, newNDistinct, newMostCommon);
    }

    public double joinSelectivityAgainst(ColumnStats other) {
        double sharedFractionMine = 0, sharedFractionTheirs = 0;
        double unsharedFractionMine = 1 - fractionNull, unsharedFractionTheirs = 1 - fractionNull;
        double fractionUnmappedMySide = 1 - fractionNull, fractionUnmappedTheirSide = 1 - fractionNull;
        long unsharedDistinctMine = nDistinct, unsharedDistinctTheirs = other.nDistinct;
        long nShared = 0;
        double selectivity = 0;

        // First consider values that exist in both histograms.
        for (Map.Entry<HistogramValue, Double> myEntry : mostCommon.entrySet()) {
            if (other.mostCommon.containsKey(myEntry.getKey())) {
                sharedFractionMine += myEntry.getValue();
                sharedFractionTheirs += other.mostCommon.get(myEntry.getKey());
                unsharedFractionMine -= myEntry.getValue();
                unsharedFractionTheirs -= other.mostCommon.get(myEntry.getKey());
                selectivity += myEntry.getValue() * other.mostCommon.get(myEntry.getKey());
                unsharedDistinctMine--;
                unsharedDistinctTheirs--;
                nShared++;
            }
        }

        // Next consider values that exist in exactly one histogram.
        // For values in one histogram, contributes (value * other.fractionUnmapped / other.nDistinctUnmapped) * (probability that this element is in the join).
        // Semijoin sets probability to min((double) unsharedDistinctTheirs / unsharedDistinctMine, 1).
        if (other.nDistinctUnmapped() > 0) {
            for (Map.Entry<HistogramValue, Double> myEntry : mostCommon.entrySet()) {
                if (!other.mostCommon.containsKey(myEntry.getKey())) {
                    double probabilityThatThisExists = Math.min((double) unsharedDistinctTheirs / unsharedDistinctMine, 1);
                    selectivity += myEntry.getValue() * other.fractionUnmapped() / other.nDistinctUnmapped() * probabilityThatThisExists;
                }
            }
        }
        if (nDistinctUnmapped() > 0) {
            for (Map.Entry<HistogramValue, Double> theirEntry : other.mostCommon.entrySet()) {
                if (!mostCommon.containsKey(theirEntry.getKey())) {
                    double probabilityThatThisExists = Math.min((double) unsharedDistinctMine / unsharedDistinctTheirs, 1);
                    selectivity += theirEntry.getValue() * fractionUnmapped() / nDistinctUnmapped() * probabilityThatThisExists;
                }
            }
        }

        // Finally consider values that are not in either histogram.
        long numExplicitValues = mostCommon.size() + other.mostCommon.size() - nShared;
        long numImplicitValuesSelf = nDistinct - numExplicitValues;
        long numImplicitValuesOther = other.nDistinct - numExplicitValues;
        if (numImplicitValuesSelf > 0 && numImplicitValuesOther > 0) {
            selectivity += fractionUnmapped() * other.fractionUnmapped() / Math.max(numImplicitValuesSelf, numImplicitValuesOther);
        }

        return selectivity;
    }
}
