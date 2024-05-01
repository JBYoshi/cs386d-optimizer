package dev.jonathanb.cs386d;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;

public record HistogramValue(Object obj, BigDecimal numeric) implements Comparable<HistogramValue> {
    public HistogramValue(Object obj) {
        this(obj, typeToNumber(obj));
    }

    @Override
    public int compareTo(HistogramValue o) {
        return numeric.compareTo(o.numeric);
    }

    private static final BigDecimal CHAR_SIZE = BigDecimal.valueOf(100000);

    private static BigDecimal typeToNumber(Object o) {
        if (o instanceof Number) {
            if (o instanceof BigDecimal) {
                return (BigDecimal) o;
            }
            if (o instanceof BigInteger) {
                return new BigDecimal((BigInteger) o);
            }
            if (o instanceof Long) {
                return new BigDecimal((Long) o);
            }
            return BigDecimal.valueOf(((Number) o).doubleValue());
        }
        if (o instanceof String) {
            BigDecimal value = BigDecimal.ZERO;
            BigDecimal frac = BigDecimal.ONE;
            for (char c : ((String) o).toCharArray()) {
                frac = frac.divide(CHAR_SIZE, RoundingMode.HALF_UP);
                value = value.add(frac.multiply(BigDecimal.valueOf(c).add(BigDecimal.ONE)));
            }
            return value;
        }
        throw new UnsupportedOperationException("Cannot convert " + o.getClass() + " to a numeric equivalent");
    }
}
