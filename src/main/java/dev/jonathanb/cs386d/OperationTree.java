package dev.jonathanb.cs386d;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class OperationTree {
    private final RelationStats stats;
    private final Set<TableRef> tables;
    private final double totalCost;

    protected OperationTree(RelationStats stats, Set<TableRef> tables, double totalCost) {
        this.stats = stats;
        this.tables = tables;
        this.totalCost = totalCost;
    }

    public RelationStats getStats() {
        return stats;
    }

    public Set<TableRef> getTablesSet() {
        return tables;
    }

    public double getTotalCost() {
        return totalCost;
    }

    public final String toString() {
        // TODO format properly
        StringBuilder builder = new StringBuilder();
        toString(builder, 0);
        return builder.toString();
    }

    protected abstract void toString(StringBuilder builder, int depth);

    public static class TableScan extends OperationTree {
        private final TableRef table;
        public TableScan(RelationStats stats, TableRef table) {
            super(stats, Set.of(table), 0);
            this.table = table;
        }

        public TableRef getTable() {
            return table;
        }

        @Override
        protected void toString(StringBuilder builder, int depth) {
            builder.append("|".repeat(depth));
            builder.append("TableScan(").append(this.table).append(")");
        }
    }

    public static class Join extends OperationTree {
        private final OperationTree leftTree, rightTree;
        private final Set<JoinPredicate> predicates;
        private final TableRef leftTable, rightTable;

        public Join(RelationStats stats, OperationTree leftTree, OperationTree rightTree, TableRef leftTable, TableRef rightTable, Set<JoinPredicate> predicates) {
            super(stats, Stream.concat(leftTree.tables.stream(), rightTree.tables.stream()).collect(Collectors.toSet()),
                    leftTree.totalCost + rightTree.totalCost);
            this.leftTree = leftTree;
            this.rightTree = rightTree;
            this.leftTable = leftTable;
            this.rightTable = rightTable;
            this.predicates = predicates;
        }

        public OperationTree getLeftTree() {
            return leftTree;
        }

        public OperationTree getRightTree() {
            return rightTree;
        }

        @Override
        protected void toString(StringBuilder builder, int depth) {
            builder.append("|".repeat(depth));
            builder.append("Join((");
            builder.append(leftTable);
            builder.append(", ");
            builder.append(rightTable);
            builder.append(") on [");
            builder.append(predicates.stream().map(JoinPredicate::toString).collect(Collectors.joining(", ")));
            builder.append("])\n");
            leftTree.toString(builder, depth + 1);
            builder.append("\n");
            rightTree.toString(builder, depth + 1);
        }
    }
}
