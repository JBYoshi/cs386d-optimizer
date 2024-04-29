package dev.jonathanb.cs386d;

import java.math.MathContext;
import java.util.ArrayList;
import java.util.List;

public record HistogramRange(HistogramValue lowerBound, HistogramValue upperBound, double numDistinct, double fractionOfElements) {
    public List<HistogramRange> split(List<HistogramValue> values) {
        if (values.isEmpty()) return List.of(this);

        List<HistogramRange> out = new ArrayList<>();
        out.add(subRange(lowerBound, values.get(0)));
        for (int i = 0; i < values.size() - 1; i++) {
            HistogramValue left = values.get(i);
            HistogramValue right = values.get(i + 1);
            out.add(subRange(left, right));
        }
        out.add(subRange(values.get(values.size() - 1), upperBound));
        return out;
    }

    public HistogramRange subRange(HistogramValue lower, HistogramValue upper) {
        if (lower.compareTo(this.lowerBound) < 0 || upper.compareTo(this.upperBound) > 0 || lower.compareTo(upper) > 0) {
            throw new IllegalArgumentException();
        }
        if (this.lowerBound.equals(this.upperBound)) {
            return this;
        }
        double portionOfRange = (upper.numeric().subtract(lower.numeric())).divide(upperBound.numeric().subtract(lowerBound.numeric()), MathContext.DECIMAL64).doubleValue();
        return new HistogramRange(lower, upper, numDistinct * portionOfRange, fractionOfElements * portionOfRange);
    }

    public static List<HistogramRange> makeRange(List<HistogramValue> values, double totalNumDistinct, double totalFractionOfElements) {
        List<HistogramRange> out = new ArrayList<>();
        for (int i = 0; i < values.size() - 1; i++) {
            HistogramValue lower = values.get(i);
            HistogramValue upper = values.get(i + 1);
            out.add(new HistogramRange(lower, upper, totalNumDistinct / (values.size()) - 1, totalFractionOfElements / (values.size() - 1)));
        }
        return out;
    }

    public boolean contains(HistogramValue key) {
        return key.compareTo(lowerBound) >= 0 && key.compareTo(upperBound) <= 0;
    }

    public boolean overlaps(HistogramRange other) {
        return other.upperBound.compareTo(this.lowerBound) >= 0 && other.lowerBound.compareTo(this.upperBound) <= 0;
    }
}
