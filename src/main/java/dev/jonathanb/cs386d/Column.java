package dev.jonathanb.cs386d;

public record Column(TableRef table, String columnName) {
    @Override
    public String toString() {
        return table + "." + columnName;
    }

    public String toShortString() {
        if (table.alias() == null) return table.baseTable().tableName() + "." + columnName;
        return table.alias() + "." + columnName;
    }
}
