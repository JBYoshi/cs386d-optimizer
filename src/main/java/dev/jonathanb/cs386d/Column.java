package dev.jonathanb.cs386d;

public record Column(TableRef table, String columnName) {
    @Override
    public String toString() {
        return table + "." + columnName;
    }
}
