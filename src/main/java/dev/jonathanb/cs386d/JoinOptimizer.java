package dev.jonathanb.cs386d;

import java.util.*;
import java.util.stream.Collectors;

public class JoinOptimizer {
    public OperationTree optimize(Map<TableRef, RelationStats> baseRelations, Set<JoinPredicate> predicates, Set<ValuePredicate> valuePredicates) {
        applyPredicates(baseRelations, valuePredicates);

        Map<Set<TableRef>, OperationTree> baseOps = new HashMap<>();
        for (Map.Entry<TableRef, RelationStats> baseRelation : baseRelations.entrySet()) {
            OperationTree tree = new OperationTree.TableScan(baseRelation.getValue(), baseRelation.getKey(), valuePredicates.stream().filter(x -> x.getColumn().table().equals(baseRelation.getKey())).collect(Collectors.toSet()));
            baseOps.put(tree.getTablesSet(), tree);
        }
        Map<Set<TableRef>, OperationTree> prevOps = new HashMap<>(baseOps);
        Map<Set<TableRef>, OperationTree> allOps = new HashMap<>(baseOps);

        Map<Set<TableRef>, Set<JoinPredicate>> predicatesByEdge = new HashMap<>();
        Map<TableRef, Set<JoinPredicate>> predicatesByVertex = new HashMap<>(); // TODO currently unused
        for (JoinPredicate predicate : predicates) {
            predicatesByEdge.computeIfAbsent(Set.of(predicate.a().table(), predicate.b().table()), x -> new HashSet<>())
                    .add(predicate);
            predicatesByVertex.computeIfAbsent(predicate.a().table(), x -> new HashSet<>()).add(predicate);
            predicatesByVertex.computeIfAbsent(predicate.b().table(), x -> new HashSet<>()).add(predicate);
        }

        for (int count = 2; count <= baseRelations.size(); count++) {
            Map<Set<TableRef>, OperationTree> nextOps = new HashMap<>();

            for (OperationTree left : prevOps.values()) {
                for (TableRef rightTable : baseRelations.keySet()) {
                    if (left.getTablesSet().contains(rightTable)) continue;
                    OperationTree right = baseOps.get(Set.of(rightTable));

                    for (TableRef leftTable : left.getTablesSet()) {
                        Set<JoinPredicate> relevantPredicates = predicatesByEdge.get(Set.of(leftTable, rightTable));
                        if (relevantPredicates == null) {
                            continue;
                        }

                        OperationTree newTree = computeJoin(left, right, leftTable, rightTable, relevantPredicates);
                        nextOps.merge(newTree.getTablesSet(), newTree, (a, b) -> {
                            if (a.getTotalCost() <= b.getTotalCost()) return a;
                            else return b;
                        });
                        allOps.merge(newTree.getTablesSet(), newTree, (a, b) -> {
                            if (a.getTotalCost() <= b.getTotalCost()) return a;
                            else return b;
                        });
                    }
                }
            }

            prevOps = nextOps;
        }
        System.out.println(rounds); // TODO debugging only

        return prevOps.get(new LinkedHashSet<>(baseRelations.keySet()));
    }

    public OperationTree testSpecific(List<List<TableRef>> order, Map<TableRef, RelationStats> baseRelations, Set<JoinPredicate> predicates, Set<ValuePredicate> valuePredicates) {
        applyPredicates(baseRelations, valuePredicates);

        Map<TableRef, OperationTree> baseOps = new HashMap<>();
        for (Map.Entry<TableRef, RelationStats> baseRelation : baseRelations.entrySet()) {
            OperationTree tree = new OperationTree.TableScan(baseRelation.getValue(), baseRelation.getKey(), valuePredicates.stream().filter(x -> x.getColumn().table().equals(baseRelation.getKey())).collect(Collectors.toSet()));
            baseOps.put(baseRelation.getKey(), tree);
        }

        OperationTree curr = baseOps.get(order.get(0).get(0));
        for (int i = 1; i < order.size(); i++) {
            List<TableRef> command = order.get(i);
            OperationTree right = baseOps.get(command.get(1));
            Set<JoinPredicate> relevantPredicates = predicates.stream().filter(p -> Set.of(p.a().table(), p.b().table()).equals(Set.copyOf(command))).collect(Collectors.toSet());
            curr = computeJoin(curr, right, command.get(0), command.get(1), relevantPredicates);
        }

        return curr;
    }

    private static void applyPredicates(Map<TableRef, RelationStats> baseRelations, Set<ValuePredicate> valuePredicates) {
        for (ValuePredicate valuePredicate : valuePredicates) {
            baseRelations.computeIfPresent(valuePredicate.getColumn().table(), (table, stats) -> valuePredicate.apply(stats));
        }
    }

    private int rounds = 0;

    private OperationTree computeJoin(OperationTree left, OperationTree right, TableRef leftTable, TableRef rightTable, Set<JoinPredicate> relevantPredicates) {
        rounds++;
        double numRows = left.getStats().numRows() * right.getStats().numRows();
        Map<Column, ColumnStats> baseColumnStats = new HashMap<>();
        baseColumnStats.putAll(left.getStats().columnStats());
        baseColumnStats.putAll(right.getStats().columnStats());
        RelationStats stats = new RelationStats(numRows, baseColumnStats);

        Set<JoinPredicate> allPredicates = new HashSet<>(left.collectJoins());
        allPredicates.addAll(right.collectJoins());
        for (JoinPredicate predicate : relevantPredicates) {
            if (predicate.a().table().equals(rightTable)) {
                left = left.pushSemijoin(predicate.b(), stats.columnStats().get(predicate.b()).semijoin(stats.columnStats().get(predicate.a())));
            } else {
                left = left.pushSemijoin(predicate.a(), stats.columnStats().get(predicate.a()).semijoin(stats.columnStats().get(predicate.b())));
            }
            stats = predicate.apply(stats, allPredicates);
            allPredicates.add(predicate);
        }

        return new OperationTree.Join(stats, left, right, leftTable, rightTable, relevantPredicates);
    }
}
