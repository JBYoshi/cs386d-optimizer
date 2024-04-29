package dev.jonathanb.cs386d;

public record TableRef(String alias, Table baseTable) {
    @Override
    public String toString() {
        if (alias.equals(baseTable.tableName())) return baseTable.toString();
        return "(" + baseTable + " AS " + alias + ")";
    }
}
