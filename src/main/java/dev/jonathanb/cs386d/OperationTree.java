package dev.jonathanb.cs386d;

import java.util.HashSet;
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

    public final Set<JoinPredicate> collectJoins() {
        Set<JoinPredicate> joins = new HashSet<>();
        collectJoins(joins);
        return joins;
    }

    protected abstract void collectJoins(Set<JoinPredicate> joins);

    protected abstract void toString(StringBuilder builder, int depth);

    public static class TableScan extends OperationTree {
        private final TableRef table;
        private Set<ValuePredicate> predicates;
        public TableScan(RelationStats stats, TableRef table, Set<ValuePredicate> valuePredicates) {
            super(applyPredicates(stats, valuePredicates), Set.of(table), stats.numRows());
            this.table = table;
            this.predicates = valuePredicates;
        }

        private static RelationStats applyPredicates(RelationStats stats, Set<ValuePredicate> valuePredicates) {
            for (ValuePredicate predicate : valuePredicates) {
                stats = stats.applySelect(predicate.getSelectivity(stats.columnStats().get(predicate.getColumn())), Set.of(predicate.getColumn()));
            }
            return stats;
        }

        public TableRef getTable() {
            return table;
        }

        @Override
        protected void toString(StringBuilder builder, int depth) {
            builder.append("|".repeat(depth));
            builder.append("TableScan(").append(this.table).append(" WHERE ").append(this.predicates).append("); rows = ").append(getStats().numRows()).append(", cumulative cost = ").append(getTotalCost());
        }

        @Override
        protected void collectJoins(Set<JoinPredicate> joins) {
        }
    }

    public static class Join extends OperationTree {
        private final OperationTree leftTree, rightTree;
        private final Set<JoinPredicate> predicates;
        private final TableRef leftTable, rightTable;

        public Join(RelationStats stats, OperationTree leftTree, OperationTree rightTree, TableRef leftTable, TableRef rightTable, Set<JoinPredicate> predicates) {
            super(stats, Stream.concat(leftTree.tables.stream(), rightTree.tables.stream()).collect(Collectors.toSet()),
                    leftTree.totalCost + rightTree.totalCost + stats.numRows());
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
            builder.append("]); rows = ");
            builder.append(getStats().numRows());
            builder.append(", cumulative cost = ");
            builder.append(getTotalCost());
            builder.append("\n");
            leftTree.toString(builder, depth + 1);
            builder.append("\n");
            rightTree.toString(builder, depth + 1);
        }

        @Override
        protected void collectJoins(Set<JoinPredicate> joins) {
            joins.addAll(predicates);
            leftTree.collectJoins(joins);
            rightTree.collectJoins(joins);
        }

        public TableRef getLeftTable() {
            return leftTable;
        }

        public TableRef getRightTable() {
            return rightTable;
        }
    }
}
