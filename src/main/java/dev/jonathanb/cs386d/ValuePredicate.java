package dev.jonathanb.cs386d;

import java.util.*;
import java.util.stream.Collectors;

public abstract class ValuePredicate {
    private final Column column;

    protected ValuePredicate(Column column) {
        this.column = column;
    }

    public Column getColumn() {
        return this.column;
    }

    public abstract ColumnSelectivity getSelectivity(ColumnStats stats);

    public static class Equality extends ValuePredicate {
        private final Set<HistogramValue> values;
        private final boolean invert;

        public Equality(Column column, Set<HistogramValue> values, boolean invert) {
            super(column);
            this.values = values;
            this.invert = invert;
        }

        @Override
        public ColumnSelectivity getSelectivity(ColumnStats stats) {
            double fractionKept = 0;
            for (HistogramValue value : values) {
                // Assuming all values exist in the base relation.
                fractionKept += stats.estimatedFrequencyAssumingExists(value);
            }

            if (invert) {
                fractionKept = 1 - fractionKept;

                Map<HistogramValue, Double> newMostCommon = new HashMap<>();
                for (Map.Entry<HistogramValue, Double> entry : stats.mostCommon().entrySet()) {
                    if (!values.contains(entry.getKey())) {
                        newMostCommon.put(entry.getKey(), entry.getValue() / fractionKept);
                    }
                }

                return new ColumnSelectivity(fractionKept, new ColumnStats(stats.fractionNull() / fractionKept, stats.nDistinct() - values.size(), newMostCommon, List.of()));
            } else {
                Map<HistogramValue, Double> newMostCommon = new HashMap<>();
                for (HistogramValue value : values) {
                    // I'm including all of the values here to make it more explicit.
                    newMostCommon.put(value, stats.estimatedFrequencyAssumingExists(value) / fractionKept);
                }
                return new ColumnSelectivity(fractionKept, new ColumnStats(0, values.size(), newMostCommon, List.of()));
            }
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append(getColumn());
            if (values.size() == 1) {
                builder.append(invert ? " <> " : " = ");
                builder.append(values.iterator().next());
            } else {
                builder.append(invert ? " NOT IN (" : " IN (");
                builder.append(values.stream().map(Object::toString).collect(Collectors.joining(", ")));
                builder.append(")");
            }
            return builder.toString();
        }
    }

    public static class Inequality extends ValuePredicate {
        private final HistogramValue threshold;
        private final boolean lessThan, equal, greaterThan;

        public Inequality(Column column, HistogramValue threshold, boolean lessThan, boolean equal, boolean greaterThan) {
            super(column);
            this.threshold = threshold;
            this.lessThan = lessThan;
            this.equal = equal;
            this.greaterThan = greaterThan;
        }

        private boolean accept(HistogramValue value) {
            int comparison = value.compareTo(threshold);
            if (comparison < 0 && lessThan) return true;
            if (comparison > 0 && greaterThan) return true;
            if (comparison == 0 && equal) return true;
            return false;
        }

        private boolean accept(HistogramRange range) {
            if (lessThan && range.lowerBound().compareTo(threshold) < 0) {
                return true;
            }
            if (greaterThan && range.upperBound().compareTo(threshold) > 0) {
                return true;
            }
            if (equal && range.contains(threshold)) {
                return true;
            }
            return false;
        }

        @Override
        public ColumnSelectivity getSelectivity(ColumnStats stats) {
            double fractionKept = 0;
            double newDistinct = 0;

            for (HistogramRange range : stats.histogram()) {
                // Allow any range that partially overlaps
                if (accept(range)) {
                    fractionKept += range.fractionOfElements();
                    newDistinct += range.numDistinct();
                    // (Note if you decide to carry over the histogram ranges: the fraction of elements field needs to be changed)
                }
            }

            for (Map.Entry<HistogramValue, Double> entry : stats.mostCommon().entrySet()) {
                if (accept(entry.getKey())) {
                    fractionKept += entry.getValue();
                    newDistinct++;
                }
            }

            Map<HistogramValue, Double> newMostCommon = new HashMap<>();
            for (Map.Entry<HistogramValue, Double> entry : stats.mostCommon().entrySet()) {
                if (accept(entry.getKey())) {
                    newMostCommon.put(entry.getKey(), entry.getValue() / fractionKept);
                }
            }
            return new ColumnSelectivity(fractionKept, new ColumnStats(0, Math.round(newDistinct), newMostCommon, List.of()));
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append(getColumn());
            builder.append(" ");
            if (lessThan) builder.append('<');
            if (greaterThan) builder.append('>');
            if (equal) builder.append('=');
            builder.append(" ");
            builder.append(threshold);
            return builder.toString();
        }
    }

    public static class Null extends ValuePredicate {
        private final boolean invert;

        public Null(Column column, boolean invert) {
            super(column);
            this.invert = invert;
        }

        @Override
        public ColumnSelectivity getSelectivity(ColumnStats stats) {
            if (invert) {
                return new ColumnSelectivity(stats.fractionNull(), new ColumnStats(1, 0, Map.of(), List.of()));
            }
            Map<HistogramValue, Double> newMostCommon = new HashMap<>();
            for (Map.Entry<HistogramValue, Double> entry : stats.mostCommon().entrySet()) {
                newMostCommon.put(entry.getKey(), entry.getValue() / stats.fractionNull());
            }
            return new ColumnSelectivity(1 - stats.fractionNull(), new ColumnStats(0, stats.nDistinct(), newMostCommon, List.of()));
        }
        
        @Override
        public String toString() {
            return getColumn() + (invert ? " IS NOT NULL" : " IS NULL");
        }
    }
}
