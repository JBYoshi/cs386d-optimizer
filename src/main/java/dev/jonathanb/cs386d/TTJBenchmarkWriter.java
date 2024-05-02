package dev.jonathanb.cs386d;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TTJBenchmarkWriter {
    private final BenchmarkQuery query;
    private final String queryPrefix;
    private final StringBuilder output = new StringBuilder();
    private final List<Table> joinOrder = new ArrayList<>();
    private final Map<Table, List<Table>> joinTree = new HashMap<>();
    private final Connection conn;

    private TTJBenchmarkWriter(BenchmarkQuery query, String queryPrefix, Connection conn) {
        this.query = query;
        this.queryPrefix = queryPrefix;
        this.conn = conn;
    }

    private void buildJoinOrderAndTree(OperationTree tree) {
        if (tree instanceof OperationTree.Join join) {
            buildJoinOrderAndTree(join.getLeftTree());
            OperationTree.TableScan scanRight = (OperationTree.TableScan) join.getRightTree();
            joinOrder.add(translateTable(scanRight.getTable()));
            joinTree.computeIfAbsent(translateTable(join.getLeftTable()), x -> new ArrayList<>())
                    .add(translateTable(join.getRightTable()));
        }
        if (tree instanceof OperationTree.TableScan scan) {
            joinOrder.add(translateTable(scan.getTable()));
        }
    }

    private Table translateTable(TableRef ref) {
        for (ValuePredicate pred : query.valuePredicates()) {
            if (pred.getColumn().table().equals(ref)) {
                return new Table("imdb", queryPrefix + ref.baseTable().tableName());
            }
        }
        return new Table("imdb_int", ref.baseTable().tableName());
    }

    private void printTree(Table current, String prefix) throws SQLException {
        output.append(prefix).append(current.schemaName()).append(".").append(current.tableName()).append("(");
        ResultSet attributes = conn.getMetaData().getColumns(conn.getCatalog(), current.schemaName(), current.tableName(), "%");
        while (attributes.next()) {
            if (attributes.getRow() > 1) output.append(",");
            output.append(attributes.getString("COLUMN_NAME"));
        }
        output.append(")");
        for (Table child : joinTree.getOrDefault(current, List.of())) {
            output.append("\\n");
            printTree(child, prefix + "|");
        }
    }

    private void write(OperationTree tree) throws SQLException {
        buildJoinOrderAndTree(tree);

        output.append("{\n  \"optimalJoinOrdering\" : {\n    \"schemaTableNameList\" : [");
        for (int i = 0; i < joinOrder.size(); i++) {
            if (i > 0) output.append(",");
            output.append(" {\n      \"schemaName\" : \"");
            output.append(joinOrder.get(i).schemaName());
            output.append("\",\n      \"tableName\" : \"");
            output.append(joinOrder.get(i).tableName());
            output.append("\"\n    }");
        }
        output.append(" ]\n  },\n  \"optimalJoinTree\" : \"");
        printTree(joinOrder.get(0), "");
        output.append("\"\n}");
    }

    public static String writeTree(BenchmarkQuery query, String queryPrefix, OperationTree tree, Connection conn) throws SQLException {
        TTJBenchmarkWriter writer = new TTJBenchmarkWriter(query, queryPrefix, conn);
        writer.write(tree);
        return writer.output.toString();
    }
}
