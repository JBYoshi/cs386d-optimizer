package dev.jonathanb.cs386d;

public record Table(String schemaName, String tableName) {
    @Override
    public String toString() {
        return schemaName + "." + tableName;
    }
}
