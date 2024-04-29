package dev.jonathanb.cs386d;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public record BenchmarkQuery(Set<TableRef> relations, Set<JoinPredicate> predicates) {
    private static final Pattern COLUMN_PATTERN = Pattern.compile("^([a-zA-Z_][a-zA-Z0-9_]*)\\.([a-zA-Z_][a-zA-Z0-9_]*)$");
    private static final Pattern NUMBER_SUFFIX_PATTERN = Pattern.compile("[0-9]+$");

    @Override
    public String toString() {
        String pseudoSql = "SELECT (...) FROM " + relations.stream().map(TableRef::toString).collect(Collectors.joining(",\n  "));
        if (!predicates.isEmpty()) {
            pseudoSql += "\nWHERE " + predicates.stream().map(JoinPredicate::toString).collect(Collectors.joining("\n  AND "));
        }
        return pseudoSql + ";";
    }

    /**
     * Parses a query from the original gregrahn/join-order-benchmark list into an analyzable form compatible with the TreeTracker benchmark.
     * <p>
     * The TreeTracker benchmark system doesn't support single-column filters (like x > 3). It only supports conjunctive equalities between two
     * tables. Filters on a specific base table are handled using views. This function is specifically designed to parse those queries.
     * @param prefix The prefix used for query-specific views in the benchmark system (for example, q10a_).
     * @param query The query text from the original gregrahn/join-order-benchmark list.
     * @return A query configuration.
     */
    public static BenchmarkQuery parseFromString(String prefix, String query) {
        // The benchmark system doesn't support single-column filters (like x > 3). Instead, it creates views that have those filters applied.
        // I've optimized this parser to specifically work with that case.

        Scanner scanner = new Scanner(query.replace(",", " , ").replace(";", " ; "));
        while (!scanner.next().equals("FROM")) continue;

        Set<String> originalTableNames = new HashSet<>();
        Map<String, String> aliasesToOriginalTableNames = new HashMap<>();
        Set<String> duplicateTableNames = new HashSet<>();
        while (true) {
            String originalTableName = scanner.next();
            if (originalTableNames.contains(originalTableName)) {
                duplicateTableNames.add(originalTableName);
            }
            originalTableNames.add(originalTableName);
            String end = scanner.next();
            if (end.equals("AS")) {
                String alias = scanner.next();
                aliasesToOriginalTableNames.put(alias, originalTableName);
                end = scanner.next();
            }
            if (end.equals(",")) {
                continue;
            } else if (end.equals("WHERE")) {
                break;
            } else {
                throw new IllegalArgumentException("Got unexpected token " + end + " in FROM section");
            }
        }

        Set<PartialJoinPredicate> predicatesUsingOriginalTables = new HashSet<>();
        Set<String> aliasesWithNonJoinConstraints = new HashSet<>();
        while (true) {
            String left = scanner.next();
            boolean isParen = left.startsWith("(");
            if (isParen) {
                left = left.substring(1);
            }
            Matcher leftMatcher = COLUMN_PATTERN.matcher(left);
            if (!leftMatcher.matches()) {
                throw new IllegalArgumentException("Got unexpected left column name " + left + " in WHERE section");
            }
            String operator = scanner.next();
            Matcher right = COLUMN_PATTERN.matcher(scanner.next());
            String end;
            if (operator.equals("=") && right.matches()) {
                if (isParen) {
                    throw new IllegalArgumentException("Unsupported equality constraint in parentheses");
                }
                predicatesUsingOriginalTables.add(new PartialJoinPredicate(
                        leftMatcher.group(1), leftMatcher.group(2),
                        right.group(1), right.group(2)
                ));
                end = scanner.next();
            } else {
                aliasesWithNonJoinConstraints.add(leftMatcher.group(1));
                if (operator.equals("BETWEEN")) {
                    // Skip the AND that corresponds to the BETWEEN
                    do {
                        end = scanner.next();
                    } while (!end.equals("AND"));
                }
                do {
                    end = scanner.next();
                } while (!end.equals("AND") && !end.equals(";"));
            }
            if (end.equals("AND")) {
                continue;
            }
            if (end.equals(";")) {
                break;
            }
            throw new IllegalArgumentException("Got unexpected token " + end + " in WHERE section following two-column equality test");
        }

        Function<String, TableRef> aliasToBenchmarkTable = alias -> {
            String table = aliasesToOriginalTableNames.getOrDefault(alias, alias);
            if (aliasesWithNonJoinConstraints.contains(alias)) {
                String suffix = "";
                if (duplicateTableNames.contains(table)) {
                    Matcher suffixMatcher = NUMBER_SUFFIX_PATTERN.matcher(alias);
                    suffixMatcher.find();
                    suffix = suffixMatcher.group(0);
                }
                String finalTableName = prefix + table + suffix;
                if (finalTableName.equals("q28a_movie_info_idx")) {
                    finalTableName = "q28a_movie_info_idx2";
                } else if (finalTableName.equals("q29a_info_type3")) {
                    finalTableName = "q29a_info_type2";
                }
                return new TableRef(alias, new Table("imdb", finalTableName));
            }
            return new TableRef(alias, new Table("imdb_int", table));
        };

        return new BenchmarkQuery(aliasesToOriginalTableNames.keySet().stream().map(aliasToBenchmarkTable).collect(Collectors.toSet()),
                predicatesUsingOriginalTables.stream().map(pred -> new JoinPredicate(
                        new Column(aliasToBenchmarkTable.apply(pred.leftAlias), pred.leftColumn),
                        new Column(aliasToBenchmarkTable.apply(pred.rightAlias), pred.rightColumn)
                )).collect(Collectors.toSet()));
    }

    public static BenchmarkQuery loadFromBenchmark(String queryName) throws IOException {
        String joinOrderBenchmarkFolder = System.getProperty("join-order-benchmark.path");
        if (joinOrderBenchmarkFolder == null) {
            throw new IOException("Missing system property join-order-benchmark.path");
        }
        return parseFromString("q" + queryName + "_", Files.readString(Path.of(joinOrderBenchmarkFolder, queryName + ".sql")));
    }

    private record PartialJoinPredicate(String leftAlias, String leftColumn, String rightAlias, String rightColumn) {}
}
