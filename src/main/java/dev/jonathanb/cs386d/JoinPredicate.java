package dev.jonathanb.cs386d;

import java.util.*;

// This optimizer only handles conjunctions of equality predicates.
// The benchmark system applies all other predicates using views.
public record JoinPredicate(Column a, Column b) {

    @Override
    public String toString() {
        return a + " = " + b;
    }

    public RelationStats apply(RelationStats stats, Collection<JoinPredicate> existingPredicates) {
        ColumnStats aStats = stats.columnStats().get(a);
        ColumnStats bStats = stats.columnStats().get(b);
        if (aStats == null || bStats == null) {
            throw new IllegalArgumentException("Missing column for " + this + " in " + stats);
        }

        Set<Column> equalColumns = new HashSet<>();
        equalColumns.add(a);
        equalColumns.add(b);
        while (true) {
            boolean changed = false;
            for (JoinPredicate pred : existingPredicates) {
                if (equalColumns.contains(pred.a) ^ equalColumns.contains(pred.b)) {
                    equalColumns.add(pred.a);
                    equalColumns.add(pred.b);
                    changed = true;
                }
            }
            if (!changed) {
                break;
            }
        }

        return stats.applySelect(aStats.join(bStats), equalColumns);
    }
}
