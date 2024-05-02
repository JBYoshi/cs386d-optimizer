package dev.jonathanb.cs386d;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.util.cnfexpression.MultiAndExpression;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public record BenchmarkQuery(Set<TableRef> relations, Set<JoinPredicate> predicates, Set<ValuePredicate> valuePredicates) {
    @Override
    public String toString() {
        String pseudoSql = "SELECT (...) FROM " + relations.stream().map(TableRef::toString).collect(Collectors.joining(",\n  "));
        List<String> whereClauses = Stream.concat(predicates.stream(), valuePredicates.stream()).map(Object::toString).toList();
        if (!whereClauses.isEmpty()) {
            pseudoSql += "\nWHERE " + String.join("\n  AND ", whereClauses);
        }
        return pseudoSql + ";";
    }

    /**
     * Parses a query from the original gregrahn/join-order-benchmark list into an analyzable form.
     * This parser only supports equijoins and single-column comparisons.
     *
     * @param query The query text from the original gregrahn/join-order-benchmark list.
     * @return A query configuration.
     */
    public static BenchmarkQuery parseFromString(String query) {
        try {
            PlainSelect stmt = (PlainSelect) CCJSqlParserUtil.parseStatements(query).get(0);

            Set<TableRef> tables = new HashSet<>();
            Map<String, TableRef> tablesByAlias = new HashMap<>();
            tables.add(toTableRef(stmt.getFromItem(), tablesByAlias));
            List<Expression> conditions = new ArrayList<>();
            for (Join join : stmt.getJoins()) {
                tables.add(toTableRef(join.getFromItem(), tablesByAlias));
                conditions.addAll(join.getOnExpressions());
            }

            conditions.add(stmt.getWhere());

            Set<JoinPredicate> joins = new HashSet<>();
            Set<ValuePredicate> values = new HashSet<>();
            for (Expression condition : conditions) {
                fillPredicates(condition, joins, values, tablesByAlias);
            }

            return new BenchmarkQuery(tables, joins, values);
        } catch (JSQLParserException | ClassCastException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private static void fillPredicates(Expression condition, Set<JoinPredicate> joins, Set<ValuePredicate> values, Map<String, TableRef> tablesByAlias) {
        if (condition instanceof AndExpression and) {
            fillPredicates(and.getLeftExpression(), joins, values, tablesByAlias);
            fillPredicates(and.getRightExpression(), joins, values, tablesByAlias);
        } else if (condition instanceof MultiAndExpression and) {
            for (Expression sub : and.getList()) {
                fillPredicates(sub, joins, values, tablesByAlias);
            }
        } else if (condition instanceof InExpression in) {
            ParenthesedExpressionList<?> contents = in.getRightExpression(ParenthesedExpressionList.class);
            values.add(new ValuePredicate.Equality(toColumn(in.getLeftExpression(), tablesByAlias),
                    contents.stream().map(BenchmarkQuery::toValue).collect(Collectors.toSet()), in.isNot()));
        } else if (condition instanceof ComparisonOperator comp) {
            Column column = toColumn(comp.getLeftExpression(), tablesByAlias);
            if (comp.getRightExpression() instanceof net.sf.jsqlparser.schema.Column) {
                joins.add(new JoinPredicate(column, toColumn(comp.getRightExpression(), tablesByAlias)));
                return;
            }
            HistogramValue value = toValue(comp.getRightExpression());
            if (condition instanceof EqualsTo) {
                values.add(new ValuePredicate.Equality(column, Set.of(value), false));
            } else if (condition instanceof NotEqualsTo) {
                values.add(new ValuePredicate.Equality(column, Set.of(value), true));
            } else if (condition instanceof GreaterThan) {
                values.add(new ValuePredicate.Inequality(column, value, false, false, true));
            } else if (condition instanceof GreaterThanEquals) {
                values.add(new ValuePredicate.Inequality(column, value, false, true, true));
            } else if (condition instanceof MinorThan) {
                values.add(new ValuePredicate.Inequality(column, value, true, false, false));
            } else if (condition instanceof MinorThanEquals) {
                values.add(new ValuePredicate.Inequality(column, value, true, true, false));
            } else {
                throw new IllegalArgumentException(comp.getClass().getName() + ": " + comp);
            }
        } else if (condition instanceof Between bet) {
            if (bet.isNot()) {
                throw new UnsupportedOperationException("not between");
            }
            Column column = toColumn(bet.getLeftExpression(), tablesByAlias);
            HistogramValue lower = toValue(bet.getBetweenExpressionStart());
            HistogramValue upper = toValue(bet.getBetweenExpressionEnd());
            values.add(new ValuePredicate.Inequality(column, lower, true, true, false));
            values.add(new ValuePredicate.Inequality(column, upper, false, true, true));
        } else if (condition instanceof LikeExpression like) {
            values.add(new ValuePredicate.Like(
                    toColumn(like.getLeftExpression(), tablesByAlias),
                    Set.of((String) toValue(like.getRightExpression()).obj()),
                    like.isNot()));
        } else if (condition instanceof Parenthesis) {
            fillPredicates(((Parenthesis) condition).getExpression(), joins, values, tablesByAlias);
        } else if (condition instanceof OrExpression or) {
            if (!(or.getLeftExpression() instanceof LikeExpression left) || !(or.getRightExpression() instanceof LikeExpression right)) {
                throw new UnsupportedOperationException(or.toString());
            }
            if (left.isNot() || right.isNot() || !left.getLeftExpression().toString().equals(right.getLeftExpression().toString())) {
                throw new UnsupportedOperationException(or.toString());
            }
            values.add(new ValuePredicate.Like(
                    toColumn(left.getLeftExpression(), tablesByAlias),
                    Set.of((String) toValue(left.getRightExpression()).obj(), (String) toValue(right.getRightExpression()).obj()),
                    false));
        } else if (condition instanceof IsNullExpression n) {
            values.add(new ValuePredicate.Null(toColumn(n.getLeftExpression(), tablesByAlias), n.isNot()));
        } else {
            throw new IllegalArgumentException(condition.getClass().getName() + ": " + condition);
        }
    }

    private static TableRef toTableRef(FromItem from, Map<String, TableRef> tablesByAlias) {
        net.sf.jsqlparser.schema.Table table = (net.sf.jsqlparser.schema.Table) from;
        Table baseTable;
        String schemaName = table.getSchemaName();
        if (schemaName == null) {
            if (tablesByAlias.containsKey(from.toString())) {
                return tablesByAlias.get(from.toString());
            }
            schemaName = "imdb"; // TODO
        }
        baseTable = new Table(schemaName, table.getName());
        TableRef ref = new TableRef(from.getAlias() == null ? table.getName() : from.getAlias().getName(), baseTable);
        if (table.getAlias() != null) {
            tablesByAlias.put(table.getAlias().getName(), ref);
        }
        return ref;
    }

    private static Column toColumn(Expression expr, Map<String, TableRef> tablesByAlias) {
        net.sf.jsqlparser.schema.Column col = (net.sf.jsqlparser.schema.Column) expr;
        TableRef table = toTableRef(col.getTable(), tablesByAlias);
        String columnName = col.getColumnName();
        String tableName = table.baseTable().tableName();
        // The queries just use the id field for all IDs, but the benchmark uses *_id.
        if (tableName.equals("title") && columnName.equals("id")) columnName = "movie_id";
        if (tableName.equals("keyword") && columnName.equals("id")) columnName = "keyword_id";
        if (tableName.equals("kind_type") && columnName.equals("id")) columnName = "kind_id";
        if (tableName.equals("company_type") && columnName.equals("id")) columnName = "company_type_id";
        if (tableName.equals("company_name") && columnName.equals("id")) columnName = "company_id";
        if (tableName.equals("info_type") && columnName.equals("id")) columnName = "info_type_id";
        if (tableName.equals("comp_cast_type") && columnName.equals("id")) columnName = "subject_id";
        if (tableName.equals("char_name") && columnName.equals("id")) columnName = "person_role_id";
        if (tableName.equals("name") && columnName.equals("id")) columnName = "person_id";
        if (tableName.equals("role_type") && columnName.equals("id")) columnName = "role_id";
        if (tableName.equals("link_type") && columnName.equals("id")) columnName = "link_type_id";
        return new Column(table, columnName);
    }

    private static HistogramValue toValue(Expression expr) {
        if (expr instanceof StringValue str) return new HistogramValue(str.getValue());
        if (expr instanceof LongValue num) return new HistogramValue(num.getValue());
        throw new IllegalArgumentException(expr.getClass().getName() + ": " + expr);
    }

    public static BenchmarkQuery loadFromBenchmark(String queryName) throws IOException {
        String joinOrderBenchmarkFolder = System.getProperty("join-order-benchmark.path");
        if (joinOrderBenchmarkFolder == null) {
            throw new IOException("Missing system property join-order-benchmark.path");
        }
        return parseFromString(Files.readString(Path.of(joinOrderBenchmarkFolder, queryName + ".sql")));
    }
}
