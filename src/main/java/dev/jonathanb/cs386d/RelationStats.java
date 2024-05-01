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
            } else {
                updatedColumns.put(entry.getKey(), entry.getValue());
            }
        }
        return new RelationStats(count, updatedColumns);
    }
}
