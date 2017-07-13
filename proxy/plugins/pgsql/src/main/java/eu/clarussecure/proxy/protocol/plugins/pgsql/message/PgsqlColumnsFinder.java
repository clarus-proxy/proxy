package eu.clarussecure.proxy.protocol.plugins.pgsql.message;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.Assignment;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.DateTimeLiteralExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.ExtractExpression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.HexValue;
import net.sf.jsqlparser.expression.IntervalExpression;
import net.sf.jsqlparser.expression.JdbcNamedParameter;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.JsonExpression;
import net.sf.jsqlparser.expression.KeepExpression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.MySQLGroupConcat;
import net.sf.jsqlparser.expression.Not;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.NumericBind;
import net.sf.jsqlparser.expression.OracleHierarchicalExpression;
import net.sf.jsqlparser.expression.OracleHint;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.RawStringValue;
import net.sf.jsqlparser.expression.RowConstructor;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeKeyExpression;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.UserVariable;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.WithinGroupExpression;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Modulo;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.ArrayElement;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.FromExpression;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.JsonOperator;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.expression.operators.relational.RegExpMatchOperator;
import net.sf.jsqlparser.expression.operators.relational.RegExpMySQLOperator;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;

/**
 * Find all used tables within an select statement.
 */
public class PgsqlColumnsFinder implements ExpressionVisitor {

    private List<Map.Entry<Expression, Set<Column>>> expressionsWithColumns;
    private Map<Expression, Expression> expressionsToParents;
    private Deque<Expression> currentExpressions;

    public void parse(Expression expression) {
        expressionsWithColumns = new ArrayList<>();
        expressionsToParents = new HashMap<>();
        currentExpressions = new ArrayDeque<>();
        if (expression != null) {
            expression.accept(this);
        }
    }

    public Set<Column> getColumns() {
        return expressionsWithColumns.stream().map(Map.Entry::getValue).flatMap(Set::stream)
                .collect(Collectors.toSet());
    }

    public List<Expression> getExpressionsWithColumns() {
        return expressionsWithColumns.stream().map(Map.Entry::getKey).collect(Collectors.toList());
    }

    public List<Map.Entry<Expression, Set<Column>>> getExpressionsWithColumnsToColumns() {
        return expressionsWithColumns;
    }

    public Map<Expression, Expression> getExpressionsToParents() {
        return expressionsToParents;
    }

    @Override
    public void visit(SubSelect subSelect) {
    }

    @Override
    public void visit(Assignment assignement) {
        visitBinaryExpression(assignement);
    }

    @Override
    public void visit(Addition addition) {
        visitBinaryExpression(addition);
    }

    @Override
    public void visit(AndExpression andExpression) {
        visitBinaryExpression(andExpression);
    }

    @Override
    public void visit(ArrayElement arrayElement) {
        expressionsToParents.put(arrayElement, currentExpressions.peek());
        currentExpressions.push(arrayElement);
        if (arrayElement.getLeftExpression() != null) {
            arrayElement.getLeftExpression().accept(this);
        }
        if (arrayElement.getIndex() != null) {
            arrayElement.getIndex().accept(this);
        }
        currentExpressions.pop();
    }

    @Override
    public void visit(Between between) {
        expressionsToParents.put(between, currentExpressions.peek());
        currentExpressions.push(between);
        if (between.getLeftExpression() != null) {
            between.getLeftExpression().accept(this);
        }
        if (between.getBetweenExpressionStart() != null) {
            between.getBetweenExpressionStart().accept(this);
        }
        if (between.getBetweenExpressionEnd() != null) {
            between.getBetweenExpressionEnd().accept(this);
        }
        currentExpressions.pop();
    }

    @Override
    public void visit(Column column) {
        if (column.getColumnName().charAt(0) == '$'
                && column.getColumnName().substring(1).chars().allMatch(c -> Character.isDigit(c))) {
            return;
        }
        Map.Entry<Expression, Set<Column>> entry = expressionsWithColumns.isEmpty() ? null
                : expressionsWithColumns.get(expressionsWithColumns.size() - 1);
        Expression currentExpression = currentExpressions.peek();
        if (entry == null || entry.getKey() != currentExpression) {
            entry = new SimpleEntry<>(currentExpression, new HashSet<>());
            expressionsWithColumns.add(entry);
        }
        entry.getValue().add(column);
    }

    @Override
    public void visit(Division division) {
        visitBinaryExpression(division);
    }

    @Override
    public void visit(DoubleValue doubleValue) {
    }

    @Override
    public void visit(EqualsTo equalsTo) {
        visitBinaryExpression(equalsTo);
    }

    @Override
    public void visit(IsExpression is) {
        visitBinaryExpression(is);
    }

    @Override
    public void visit(Function function) {
        expressionsToParents.put(function, currentExpressions.peek());
        currentExpressions.push(function);
        if (function.getParameters() != null) {
            for (Expression expression : function.getParameters().getExpressions()) {
                expression.accept(this);
            }
        }
        currentExpressions.pop();
    }

    @Override
    public void visit(GreaterThan greaterThan) {
        visitBinaryExpression(greaterThan);
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        visitBinaryExpression(greaterThanEquals);
    }

    @Override
    public void visit(InExpression inExpression) {
        expressionsToParents.put(inExpression, currentExpressions.peek());
        currentExpressions.push(inExpression);
        if (inExpression.getLeftExpression() != null) {
            inExpression.getLeftExpression().accept(this);
        } else if (inExpression.getLeftItemsList() != null) {
            // TODO ?
        }
        // TODO ?
        //inExpression.getRightItemsList().accept(this);
        currentExpressions.pop();
    }

    @Override
    public void visit(SignedExpression signedExpression) {
        expressionsToParents.put(signedExpression, currentExpressions.peek());
        currentExpressions.push(signedExpression);
        if (signedExpression.getExpression() != null) {
            signedExpression.getExpression().accept(this);
        }
        currentExpressions.pop();
    }

    @Override
    public void visit(IsNullExpression isNullExpression) {
        expressionsToParents.put(isNullExpression, currentExpressions.peek());
        currentExpressions.push(isNullExpression);
        if (isNullExpression.getLeftExpression() != null) {
            isNullExpression.getLeftExpression().accept(this);
        }
        currentExpressions.pop();
    }

    @Override
    public void visit(JdbcParameter jdbcParameter) {
    }

    @Override
    public void visit(LikeExpression likeExpression) {
        visitBinaryExpression(likeExpression);
    }

    @Override
    public void visit(FromExpression fromExpression) {
        visitBinaryExpression(fromExpression);
    }

    @Override
    public void visit(ExistsExpression existsExpression) {
        expressionsToParents.put(existsExpression, currentExpressions.peek());
        currentExpressions.push(existsExpression);
        if (existsExpression.getRightExpression() != null) {
            existsExpression.getRightExpression().accept(this);
        }
        currentExpressions.pop();
    }

    @Override
    public void visit(LongValue longValue) {
    }

    @Override
    public void visit(MinorThan minorThan) {
        visitBinaryExpression(minorThan);
    }

    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        visitBinaryExpression(minorThanEquals);
    }

    @Override
    public void visit(Multiplication multiplication) {
        visitBinaryExpression(multiplication);
    }

    @Override
    public void visit(Not not) {
        expressionsToParents.put(not, currentExpressions.peek());
        currentExpressions.push(not);
        if (not.getExpression() != null) {
            not.getExpression().accept(this);
        }
        currentExpressions.pop();
    }

    @Override
    public void visit(NotEqualsTo notEqualsTo) {
        visitBinaryExpression(notEqualsTo);
    }

    @Override
    public void visit(NullValue nullValue) {
    }

    @Override
    public void visit(OrExpression orExpression) {
        visitBinaryExpression(orExpression);
    }

    @Override
    public void visit(Parenthesis parenthesis) {
        expressionsToParents.put(parenthesis, currentExpressions.peek());
        currentExpressions.push(parenthesis);
        if (parenthesis.getExpression() != null) {
            parenthesis.getExpression().accept(this);
        }
        currentExpressions.pop();
    }

    @Override
    public void visit(StringValue stringValue) {
    }

    @Override
    public void visit(RawStringValue stringValue) {
    }

    @Override
    public void visit(Subtraction subtraction) {
        visitBinaryExpression(subtraction);
    }

    public void visitBinaryExpression(BinaryExpression binaryExpression) {
        expressionsToParents.put(binaryExpression, currentExpressions.peek());
        currentExpressions.push(binaryExpression);
        if (binaryExpression.getLeftExpression() != null) {
            binaryExpression.getLeftExpression().accept(this);
        }
        if (binaryExpression.getLeftExpression() != null) {
            binaryExpression.getRightExpression().accept(this);
        }
        currentExpressions.pop();
    }

    @Override
    public void visit(DateValue dateValue) {
    }

    @Override
    public void visit(TimestampValue timestampValue) {
    }

    @Override
    public void visit(TimeValue timeValue) {
    }

    @Override
    public void visit(CaseExpression caseExpression) {
        expressionsToParents.put(caseExpression, currentExpressions.peek());
        currentExpressions.push(caseExpression);
        if (caseExpression.getSwitchExpression() != null) {
            caseExpression.getSwitchExpression().accept(this);
        }
        if (caseExpression.getWhenClauses() != null) {
            for (Expression expression : caseExpression.getWhenClauses()) {
                expression.accept(this);
            }
        }
        if (caseExpression.getElseExpression() != null) {
            caseExpression.getElseExpression().accept(this);
        }
        currentExpressions.pop();
    }

    @Override
    public void visit(WhenClause whenClause) {
        expressionsToParents.put(whenClause, currentExpressions.peek());
        currentExpressions.push(whenClause);
        if (whenClause.getWhenExpression() != null) {
            whenClause.getWhenExpression().accept(this);
        }
        if (whenClause.getThenExpression() != null) {
            whenClause.getThenExpression().accept(this);
        }
        currentExpressions.pop();
    }

    @Override
    public void visit(AllComparisonExpression allComparisonExpression) {
        // TODO ?
    }

    @Override
    public void visit(AnyComparisonExpression anyComparisonExpression) {
        // TODO ?
    }

    @Override
    public void visit(Concat concat) {
        visitBinaryExpression(concat);
    }

    @Override
    public void visit(Matches matches) {
        visitBinaryExpression(matches);
    }

    @Override
    public void visit(BitwiseAnd bitwiseAnd) {
        visitBinaryExpression(bitwiseAnd);
    }

    @Override
    public void visit(BitwiseOr bitwiseOr) {
        visitBinaryExpression(bitwiseOr);
    }

    @Override
    public void visit(BitwiseXor bitwiseXor) {
        visitBinaryExpression(bitwiseXor);
    }

    @Override
    public void visit(CastExpression cast) {
        expressionsToParents.put(cast, currentExpressions.peek());
        currentExpressions.push(cast);
        cast.getLeftExpression().accept(this);
        currentExpressions.pop();
    }

    @Override
    public void visit(Modulo modulo) {
        visitBinaryExpression(modulo);
    }

    @Override
    public void visit(AnalyticExpression analytic) {
    }

    @Override
    public void visit(ExtractExpression eexpr) {
    }

    @Override
    public void visit(IntervalExpression iexpr) {
    }

    @Override
    public void visit(JdbcNamedParameter jdbcNamedParameter) {
    }

    @Override
    public void visit(OracleHierarchicalExpression oexpr) {
        expressionsToParents.put(oexpr, currentExpressions.peek());
        currentExpressions.push(oexpr);
        if (oexpr.getStartExpression() != null) {
            oexpr.getStartExpression().accept(this);
        }

        if (oexpr.getConnectExpression() != null) {
            oexpr.getConnectExpression().accept(this);
        }
        currentExpressions.pop();
    }

    @Override
    public void visit(RegExpMatchOperator rexpr) {
        visitBinaryExpression(rexpr);
    }

    @Override
    public void visit(RegExpMySQLOperator rexpr) {
        visitBinaryExpression(rexpr);
    }

    @Override
    public void visit(JsonExpression jsonExpr) {
        expressionsToParents.put(jsonExpr, currentExpressions.peek());
        currentExpressions.push(jsonExpr);
        if (jsonExpr.getColumn() != null) {
            jsonExpr.getColumn().accept(this);
        }
        currentExpressions.pop();
    }

    @Override
    public void visit(JsonOperator jsonExpr) {
    }

    @Override
    public void visit(WithinGroupExpression wgexpr) {
        expressionsToParents.put(wgexpr, currentExpressions.peek());
        currentExpressions.push(wgexpr);
        if (wgexpr.getExprList() != null) {
            for (Expression expression : wgexpr.getExprList().getExpressions()) {
                expression.accept(this);
            }
        }
        currentExpressions.pop();
    }

    @Override
    public void visit(UserVariable var) {
    }

    @Override
    public void visit(NumericBind bind) {

    }

    @Override
    public void visit(KeepExpression aexpr) {
        // TODO ?
    }

    @Override
    public void visit(MySQLGroupConcat groupConcat) {
        // TODO ?
    }

    @Override
    public void visit(RowConstructor rowConstructor) {
        expressionsToParents.put(rowConstructor, currentExpressions.peek());
        currentExpressions.push(rowConstructor);
        if (rowConstructor.getExprList() != null) {
            for (Expression expr : rowConstructor.getExprList().getExpressions()) {
                expr.accept(this);
            }
        }
        currentExpressions.pop();
    }

    @Override
    public void visit(HexValue hexValue) {

    }

    @Override
    public void visit(OracleHint hint) {
    }

    @Override
    public void visit(TimeKeyExpression timeKeyExpression) {
    }

    @Override
    public void visit(DateTimeLiteralExpression literal) {
    }
}
