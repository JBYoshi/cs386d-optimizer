package dev.jonathanb.cs386d;

import java.util.Map;

public record RelationStats(double numRows, Map<Column, ColumnStats> columnStats) {
}
