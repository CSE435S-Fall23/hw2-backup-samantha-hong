 
package hw1;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import hw1.Field;
import hw1.RelationalOperator;
import hw1.Tuple;
import hw1.TupleDesc;
import hw1.Type;

/**
 * This class provides methods to perform relational algebra operations. It will be used
 * to implement SQL queries.
 * @author Doug Shook
 *
 */
public class Relation {

	private ArrayList<Tuple> tuples;
	private TupleDesc td;
	
	public Relation(ArrayList<Tuple> l, TupleDesc td) {
		//your code here
		this.tuples = l;
		this.td = td;
	}
	
	/**
	 * This method performs a select operation on a relation
	 * @param field number (refer to TupleDesc) of the field to be compared, left side of comparison
	 * @param op the comparison operator
	 * @param operand a constant to be compared against the given column
	 * @return
	 */
	public Relation select(int field, RelationalOperator op, Field operand) {
		//your code here
		 if (isValidSelectionInput(field, op, operand)) {
		        this.tuples = this.tuples.stream()
		            .filter(t -> t.getField(field).compare(op, operand))
		            .collect(Collectors.toCollection(ArrayList::new));
		    }
		    return this;
	}

	private boolean isValidSelectionInput(int field, RelationalOperator op, Field operand) {
	    return field >= 0 && field < td.numFields() && op != null && operand != null;
	}
	
	/**
	 * This method performs a rename operation on a relation
	 * @param fields the field numbers (refer to TupleDesc) of the fields to be renamed
	 * @param names a list of new names. The order of these names is the same as the order of field numbers in the field list
	 * @return
	 */
	public Relation rename(ArrayList<Integer> fields, ArrayList<String> names){
		//your code here
	
		String[] copyFields = td.copyFields();
		
		Type[] copyTypes = td.copyTypes();
		int j = 0;
		
		while(j < fields.size()) {
			if (fields.get(j) != null){
				if (!names.get(j).equals("")) {
					String name = names.get(j);
					copyFields[fields.get(j)] = name;
				}
			}
			++j;
		}
		
		ArrayList<Tuple> createdTuples = tuples;
		
		TupleDesc newTupleDesc = new TupleDesc(copyTypes, copyFields);
		
		for (Tuple t: createdTuples)
			t.setDesc(newTupleDesc);
		
		return new Relation(createdTuples, newTupleDesc);
	}
	
	/**
	 * This method performs a project operation on a relation
	 * @param fields a list of field numbers (refer to TupleDesc) that should be in the result
	 * @return
	 */
	public Relation project(ArrayList<Integer> fields) {
		//your code here
		
		int numFields = fields.size();
	    if (numFields == 0) {
	        return new Relation(new ArrayList<>(), new TupleDesc(new Type[0], new String[0]));
	    }

	    Type[] newTypes = new Type[numFields];
	    String[] newFields = new String[numFields];
	    ArrayList<Tuple> createdTuples = new ArrayList<>();

	    // see if in valid bounds
	    for (int i = 0; i < numFields; i++) {
	        int fieldIndex = fields.get(i);
	        if (fieldIndex >= td.numFields() || fieldIndex >= td.numTypes()) {
	            return new Relation(new ArrayList<>(), td);
	        }
	        newTypes[i] = td.getType(fieldIndex);
	        newFields[i] = td.getFieldName(fieldIndex);
	    }

	    // create new tuples
	    for (Tuple t : tuples) {
	        Tuple newT = new Tuple(new TupleDesc(newTypes, newFields));
	        for (int i = 0; i < numFields; i++) {
	            try {
	                newT.setField(i, t.getField(fields.get(i)));
	            } catch (Exception e) {
	                throw new IndexOutOfBoundsException("Invalid field access");
	            }
	        }
	        createdTuples.add(newT);
	    }

	    return new Relation(createdTuples, new TupleDesc(newTypes, newFields));
	}
	
	/**
	 * This method performs a join between this relation and a second relation.
	 * The resulting relation will contain all of the columns from both of the given relations,
	 * joined using the equality operator (=)
	 * @param other the relation to be joined
	 * @param field1 the field number (refer to TupleDesc) from this relation to be used in the join condition
	 * @param field2 the field number (refer to TupleDesc) from other to be used in the join condition
	 * @return
	 */
	public Relation join(Relation other, int field1, int field2) {
	    TupleDesc td1 = this.td;
	    TupleDesc td2 = other.td;

	    String[] fields1 = td1.copyFields();
	    String[] fields2 = td2.copyFields();

	    Type[] types1 = td1.copyTypes();
	    Type[] types2 = td2.copyTypes();

	    String[] combinedFields = Arrays.copyOf(fields1, fields1.length + fields2.length);
	    System.arraycopy(fields2, 0, combinedFields, fields1.length, fields2.length);

	    Type[] combinedTypes = Arrays.copyOf(types1, types1.length + types2.length);
	    System.arraycopy(types2, 0, combinedTypes, types1.length, types2.length);

	    TupleDesc newTupleDesc = new TupleDesc(combinedTypes, combinedFields);
	    ArrayList<Tuple> createdTuples = new ArrayList<>();

	    for (Tuple t1 : this.tuples) {
	        for (Tuple t2 : other.tuples) {
	            if (t1.getField(field1).compare(RelationalOperator.EQ, t2.getField(field2))) {
	                Tuple newTuple = new Tuple(newTupleDesc);
	                int fieldIndex = 0;
	                for (int i = 0; i < fields1.length; i++) {
	                    newTuple.setField(fieldIndex, t1.getField(i));
	                    fieldIndex++;
	                }
	                for (int i = 0; i < fields2.length; i++) {
	                    newTuple.setField(fieldIndex, t2.getField(i));
	                    fieldIndex++;
	                }
	                createdTuples.add(newTuple);
	            }
	        }
	    }
	    return new Relation(createdTuples, newTupleDesc);
	}

	
	/**
	 * Performs an aggregation operation on a relation. See the lab write up for details.
	 * @param op the aggregation operation to be performed
	 * @param groupBy whether or not a grouping should be performed
	 * @return
	 */
	public Relation aggregate(AggregateOperator op, boolean groupBy) {
		//your code here
		
		Aggregator aggregator = new Aggregator(op, groupBy, td);
		
		for(Tuple t: this.tuples) {
			aggregator.merge(t);
		
		}
		
		ArrayList<Tuple> createdTuples = aggregator.getResults();
		Relation result = new Relation(createdTuples, createdTuples.get(0).getDesc());
		
		return result;
	}
	
	public TupleDesc getDesc() {
		//your code here
		return td;
	}
	
	public ArrayList<Tuple> getTuples() {
		//your code here
		return tuples;
	}
	
	/**
	 * Returns a string representation of this relation. The string representation should
	 * first contain the TupleDesc, followed by each of the tuples in this relation
	 */
	public String toString() {
		//your code here
		//return null;
		StringBuilder stringBuilder = new StringBuilder();
	    stringBuilder.append(td.toString()).append('\n');

	    for (Tuple t : tuples) {
	        stringBuilder.append(t.toString()).append('\n');
	    }

	    return stringBuilder.toString();
	}
}