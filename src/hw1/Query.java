package hw1;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.JSQLParserException;

import net.sf.jsqlparser.parser.*;

import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.util.TablesNamesFinder;

public class Query {

	private String q;

	public Query(String q) {
		this.q = q;
	}

	public Relation execute() {
		Statement statement = null;
		try {
			statement = CCJSqlParserUtil.parse(q);
		} catch (JSQLParserException e) {
			System.out.println("Unable to parse query");
			e.printStackTrace();
		}

		Select selectStatement = (Select) statement;
		PlainSelect sb = (PlainSelect) selectStatement.getSelectBody();

		// your code

		ColumnVisitor columnVisitor = new ColumnVisitor();
		TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
		List<String> table1List = tablesNamesFinder.getTableList(statement);

		// get the catalog and table information
		Catalog catalog = Database.getCatalog();
		int tableId = catalog.getTableId(table1List.get(0));
		TupleDesc td = catalog.getTupleDesc(tableId);
		ArrayList<Tuple> tupleList = catalog.getDbFile(tableId).getAllTuples();

		// create the initial relation
		Relation relation = new Relation(tupleList, td);

		// handle JOIN operations
		List<Join> joinList = sb.getJoins();

		if (joinList != null) {
		    for (Join joinItem : joinList) {
		        String[] onExpressionParts = parseJoinExpression(joinItem.getOnExpression().toString());
		        String[] fieldB = getFieldParts(onExpressionParts[1]);
		        String[] fieldA = getFieldParts(onExpressionParts[0]);
		        
		        String rightTableName = joinItem.getRightItem().toString();
		        TupleDesc rightTableDesc = catalog.getTupleDesc(catalog.getTableId(rightTableName));
		        ArrayList<Tuple> rightTableTuples = catalog.getDbFile(catalog.getTableId(rightTableName)).getAllTuples();
		        Relation rightTableRelation = new Relation(rightTableTuples, rightTableDesc);

		        if (!rightTableName.equalsIgnoreCase(fieldB[0])) {
		            swapFieldNames(fieldA, fieldB);
		        }

		        int fieldIndex1 = relation.getDesc().nameToId(fieldA[1]);
		        int fieldIndex2 = rightTableRelation.getDesc().nameToId(fieldB[1]);
		        relation = relation.join(rightTableRelation, fieldIndex1, fieldIndex2);
		    }
		}

		// WHERE conditions
	
		WhereExpressionVisitor whereVisitor = new WhereExpressionVisitor();
		Relation wRelation = relation;
		
		if (sb.getWhere() == null) {
		// handle
		}
		
		else {
			sb.getWhere().accept(whereVisitor);
			wRelation = relation.select(relation.getDesc().nameToId(whereVisitor.getLeft()), whereVisitor.getOp(), whereVisitor.getRight());
		}

		// SELECT ops
		Relation sRelation = wRelation;
		
		ArrayList<Integer> pFields = new ArrayList<>();
		
		List<SelectItem> sList = sb.getSelectItems();
		
		for (SelectItem item : sList) {
		    item.accept(columnVisitor);
		    String selectCol = columnVisitor.isAggregate() ? columnVisitor.getColumn() : item.toString();

		    if (selectCol.equals("*")) {
		        for (int i = 0; i < wRelation.getDesc().numFields(); i++) {
		            pFields.add(i);
		        }
		        break;
		    }

		    int field = (selectCol.equals("*") && columnVisitor.isAggregate()) ? 0 : wRelation.getDesc().nameToId(selectCol);
		    if (!pFields.contains(field)) {
		        pFields.add(field);
		    }
		}

		sRelation = wRelation.project(pFields);

		//agg opt
		boolean shouldGroupBy = sb.getGroupByColumnReferences() != null;
		Relation aggregated;

		if (columnVisitor.isAggregate()) {
		    aggregated = sRelation.aggregate(columnVisitor.getOp(), shouldGroupBy);
		} else {
		    aggregated = sRelation;
		}

		return aggregated;
	
	}

	private String[] parseJoinExpression(String expression) {
	    return expression.split("=");
	}


	private String[] getFieldParts(String field) {
	    return field.trim().split("\\.");
	}

	private void swapFieldNames(String[] fieldA, String[] fieldB) {
	    String temp = fieldA[1];
	    fieldA[1] = fieldB[1];
	    fieldB[1] = temp;
	}
}