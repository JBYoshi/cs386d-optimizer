package dev.jonathanb.cs386d;

import java.util.*;
import java.util.stream.Collectors;

public class JoinOptimizer {
    public OperationTree optimize(Map<TableRef, RelationStats> baseRelations, Set<JoinPredicate> predicates, Set<ValuePredicate> valuePredicates) {
        applyPredicates(baseRelations, predicates, valuePredicates);

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

                        RelationStats newRelationStats = computeJoinCost(left, right, relevantPredicates);
                        OperationTree newTree = new OperationTree.Join(newRelationStats, left, right, leftTable, rightTable, relevantPredicates);
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

    private static void applyPredicates(Map<TableRef, RelationStats> baseRelations, Set<JoinPredicate> predicates, Set<ValuePredicate> valuePredicates) {
        for (ValuePredicate valuePredicate : valuePredicates) {
            baseRelations.computeIfPresent(valuePredicate.getColumn().table(), (table, stats) -> valuePredicate.apply(stats));
        }

        for (int i = 0; i < predicates.size(); i++) {
            for (JoinPredicate predicate : predicates) {
                RelationStats aStatsBefore = baseRelations.get(predicate.a().table());
                RelationStats bStatsBefore = baseRelations.get(predicate.b().table());
                RelationStats aStatsAfter = aStatsBefore.applySelect(aStatsBefore.columnStats().get(predicate.a()).semijoin(bStatsBefore.columnStats().get(predicate.b())), Set.of(predicate.a()));
                RelationStats bStatsAfter = bStatsBefore.applySelect(bStatsBefore.columnStats().get(predicate.b()).semijoin(aStatsBefore.columnStats().get(predicate.a())), Set.of(predicate.b()));
                baseRelations.put(predicate.a().table(), aStatsAfter);
                baseRelations.put(predicate.b().table(), bStatsAfter);
            }
        }
    }

    private int rounds = 0;

    private RelationStats computeJoinCost(OperationTree left, OperationTree right, Set<JoinPredicate> relevantPredicates) {
        rounds++;
        double numRows = left.getStats().numRows() * right.getStats().numRows();
        Map<Column, ColumnStats> baseColumnStats = new HashMap<>();
        baseColumnStats.putAll(left.getStats().columnStats());
        baseColumnStats.putAll(right.getStats().columnStats());
        RelationStats stats = new RelationStats(numRows, baseColumnStats);

        Set<JoinPredicate> allPredicates = new HashSet<>(left.collectJoins());
        allPredicates.addAll(right.collectJoins());
        for (JoinPredicate predicate : relevantPredicates) {
            stats = predicate.apply(stats, allPredicates);
            allPredicates.add(predicate);
        }

        return stats;
    }
}
