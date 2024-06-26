package dev.jonathanb.cs386d;

import org.postgresql.core.BaseConnection;
import org.postgresql.core.TypeInfo;
import org.postgresql.jdbc.TypeInfoCache;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;

public class Main {
    public static void main(String[] args) throws SQLException, IOException {
        Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/");
//        test(BenchmarkQuery.loadFromBenchmark("28a"), conn);
//        test(BenchmarkQuery.loadFromBenchmark("29a"), conn);
//        test(BenchmarkQuery.loadFromBenchmark("10a"), conn);
//        test(BenchmarkQuery.loadFromBenchmark("27a"), conn);
//        test(BenchmarkQuery.loadFromBenchmark("5a"), conn);

        generate("10a", conn);

        BenchmarkQuery query = BenchmarkQuery.loadFromBenchmark("10a");
        Map<TableRef, RelationStats> stats = fetchStats(query.relations(), conn);
        OperationTree myTree = new JoinOptimizer().optimize(stats, query.predicates(), query.valuePredicates());
        System.out.println(TTJBenchmarkWriter.writeTree(query, "q10a_", myTree, conn));
        System.out.println("My tree (outermost is final result):\n" + myTree);
        TableRef ci = new TableRef("ci", new Table("imdb", "cast_info"));
        TableRef chn = new TableRef("chn", new Table("imdb", "char_name"));
        TableRef t = new TableRef("t", new Table("imdb", "title"));
        TableRef rt = new TableRef("rt", new Table("imdb", "role_type"));
        TableRef mc = new TableRef("mc", new Table("imdb", "movie_companies"));
        TableRef ct = new TableRef("ct", new Table("imdb", "company_type"));
        TableRef cn = new TableRef("cn", new Table("imdb", "company_name"));

        System.out.println("\nSample tree:\n" + new JoinOptimizer().testSpecific(
                List.of(
                        List.of(mc),
                        List.of(mc, cn),
                        List.of(mc, ci),
                        List.of(ci, rt),
                        List.of(mc, ct),
                        List.of(mc, t),
                        List.of(ci, chn)
                ),
                stats, query.predicates(), query.valuePredicates()));
    }

    public static void test(BenchmarkQuery query, Connection conn) throws SQLException {
        System.out.println(query);
        System.out.println("Fetching stats...");
        Map<TableRef, RelationStats> stats = fetchStats(query.relations(), conn);
        System.out.println("Stats loaded.");
        System.out.println(new JoinOptimizer().optimize(
                stats,
                query.predicates(),
                query.valuePredicates()
        ));
    }

    public static void generate(String queryName, Connection conn) throws IOException, SQLException {
        BenchmarkQuery query = BenchmarkQuery.loadFromBenchmark(queryName);
        Map<TableRef, RelationStats> stats = fetchStats(query.relations(), conn);
        OperationTree tree = new JoinOptimizer().optimize(stats, query.predicates(), query.valuePredicates());
        String json = TTJBenchmarkWriter.writeTree(query, "q" + queryName + "_", tree, conn);

        Path path = Paths.get(".").toAbsolutePath();
        while (!Files.isDirectory(path.resolve("results/job/with_predicates")) && path.getNameCount() > 1) {
            path = path.getParent();
        }
        Files.write(path.resolve("results/job/with_predicates/TTJHP_org.zhu45.treetracker.benchmark.job.q"
                + queryName.replaceAll("[^0-9]", "") + ".Query" + queryName + "FindOptJoinTree_planStatistics.json"), List.of(json));
    }

    private static Map<TableRef, RelationStats> fetchStats(Set<TableRef> relations, Connection conn) throws SQLException {
        Map<TableRef, RelationStats> stats = new HashMap<>();
        for (TableRef relation : relations) {
            RelationStats relationStats = fetchStatsForRelation(conn, relation);
            stats.put(relation, relationStats);
        }
        return stats;
    }

    private static RelationStats fetchStatsForRelation(Connection conn, TableRef relation) throws SQLException {
        PreparedStatement statsStmt = conn.prepareStatement("SELECT * FROM pg_catalog.pg_stats WHERE schemaname = ? AND tablename = ?");
        statsStmt.setString(1, relation.baseTable().schemaName());
        statsStmt.setString(2, relation.baseTable().tableName());
        statsStmt.execute();

        PreparedStatement typesStmt = conn.prepareStatement("SELECT pg_attribute.attname, pg_attribute.atttypid FROM pg_attribute, pg_class, pg_namespace WHERE pg_attribute.attrelid = pg_class.oid AND pg_class.relname = ? AND pg_class.relnamespace = pg_namespace.oid AND pg_namespace.nspname = ?;");
        typesStmt.setString(1, relation.baseTable().tableName());
        typesStmt.setString(2, relation.baseTable().schemaName());
        typesStmt.execute();

        PreparedStatement countStmt = conn.prepareStatement("SELECT reltuples FROM pg_class, pg_namespace WHERE relname = ? AND pg_class.relnamespace = pg_namespace.oid AND pg_namespace.nspname = ?;");
        countStmt.setString(1, relation.baseTable().tableName());
        countStmt.setString(2, relation.baseTable().schemaName());
        countStmt.execute();

        ResultSet typesResults = typesStmt.getResultSet();
        Map<Column, Integer> columnTypes = new HashMap<>();
        while (typesResults.next()) {
            columnTypes.put(new Column(relation, typesResults.getString("attname")), typesResults.getInt("atttypid"));
        }

        ResultSet countResults = countStmt.getResultSet();
        if (!countResults.next()) {
            throw new IllegalArgumentException("Missing count for table " + relation);
        }
        long count = countResults.getLong("reltuples");

        ResultSet resultSet = statsStmt.getResultSet();
        Map<Column, ColumnStats> columns = new HashMap<>();
        while (resultSet.next()) {
            Column col = new Column(relation, resultSet.getString("attname"));

            double nullFrac = resultSet.getDouble("null_frac");
            double nDistinctDouble = resultSet.getDouble("n_distinct");
            long nDistinct;
            if (nDistinctDouble < 0) {
                nDistinct = Math.round(-1 * count * nDistinctDouble);
            } else {
                nDistinct = Math.round(nDistinctDouble);
            }
            Map<HistogramValue, Double> mostCommon = new HashMap<>();
            if (resultSet.getArray("most_common_vals") != null) {
                Object[] mostCommonVals = getGenericArray(conn, resultSet.getArray("most_common_vals"), columnTypes.get(col));
                Number[] mostCommonFreqs = (Number[]) resultSet.getArray("most_common_freqs").getArray();
                for (int i = 0; i < mostCommonVals.length; i++) {
                    mostCommon.put(new HistogramValue(mostCommonVals[i]), mostCommonFreqs[i].doubleValue());
                }
            }
            List<HistogramValue> histogram = new ArrayList<>();
            if (resultSet.getArray("histogram_bounds") != null) {
                Object[] histogramResults = getGenericArray(conn, resultSet.getArray("histogram_bounds"), columnTypes.get(col));
                histogram = Arrays.stream(histogramResults).map(HistogramValue::new).toList();
            }

            long nDistinctInHistogram = nDistinct - mostCommon.size();
            double fractionInHistogram = 1 - nullFrac - mostCommon.values().stream().reduce(0.0, Double::sum);
            columns.put(col, new ColumnStats(nullFrac, nDistinct, mostCommon, HistogramRange.makeRange(histogram, nDistinctInHistogram, fractionInHistogram)));
        }

        return new RelationStats(count, columns);
    }

    @SuppressWarnings("unchecked")
    private static Object[] getGenericArray(Connection conn, Array array, Integer elementType) throws SQLException {
        // This is very hacky but the Postgres driver doesn't support generic arrays currently.
        try {
            TypeInfo typeInfo = ((BaseConnection) conn).getTypeInfo();

            Field arrayOidToDelimiter = TypeInfoCache.class.getDeclaredField("arrayOidToDelimiter");
            arrayOidToDelimiter.setAccessible(true);
            ((Map<Integer, Character>) arrayOidToDelimiter.get(typeInfo)).put(2277, ',');

            Field pgArrayToPgType = TypeInfoCache.class.getDeclaredField("pgArrayToPgType");
            pgArrayToPgType.setAccessible(true);
            ((Map<Integer, Integer>) pgArrayToPgType.get(typeInfo)).put(2277, elementType);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
        return (Object[]) array.getArray();
    }
}