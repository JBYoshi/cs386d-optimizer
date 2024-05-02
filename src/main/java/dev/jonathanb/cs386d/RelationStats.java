package dev.jonathanb.cs386d;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public record RelationStats(double numRows, Map<Column, ColumnStats> columnStats) {
    public RelationStats applySelect(ColumnSelectivity selectivity, Collection<Column> columns) {
        double count = numRows() * selectivity.selectivity();
        Map<Column, ColumnStats> updatedColumns = new HashMap<>();
        for (Map.Entry<Column, ColumnStats> entry : columnStats().entrySet()) {
            if (columns.contains(entry.getKey())) {
                updatedColumns.put(entry.getKey(), selectivity.newStats());
            } else if (entry.getValue().nDistinct() > count) {
                // Common for ID fields; these are generally unique and so the number of unique values will also decrease proportionally.
                ColumnStats decreasedStats = new ColumnStats(entry.getValue().fractionNull(),
                        Math.round(entry.getValue().nDistinct() * selectivity.selectivity()),
                        entry.getValue().mostCommon(),
                        entry.getValue().histogram()
                );
                updatedColumns.put(entry.getKey(), decreasedStats);
            } else {
                updatedColumns.put(entry.getKey(), entry.getValue());
            }
        }
        return new RelationStats(count, updatedColumns);
    }
}
