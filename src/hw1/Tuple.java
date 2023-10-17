 
package hw1;

import java.sql.Types;
import java.util.HashMap;
import java.util.StringJoiner;

/**
 * This class represents a tuple that will contain a single row's worth of information
 * from a table. It also includes information about where it is stored
 * @author Sam Madden modified by Doug Shook
 *
 */
public class Tuple {
	
	/**
	 * Creates a new tuple with the given description
	 * @param t the schema for this tuple
	 */
	private TupleDesc tupleDesc;
	private int pageId;
	private int tupleId;
	private Field[] fieldVals;
	
	public Tuple(TupleDesc t) {
		//your code here
		this.tupleDesc = t;
		this.fieldVals = new Field[t.numFields()];
	}
	
	public TupleDesc getDesc() {
		//your code here
		return tupleDesc;
	}
	
	/**
	 * retrieves the page id where this tuple is stored
	 * @return the page id of this tuple
	 */
	public int getPid() {
		//your code here
		return this.pageId;
	}

	public void setPid(int pid) {
		//your code here
		this.pageId = pid;
	}

	/**
	 * retrieves the tuple (slot) id of this tuple
	 * @return the slot where this tuple is stored
	 */
	public int getId() {
		//your code here
		return tupleId;
	}

	public void setId(int id) {
		//your code here
		this.tupleId = id;
	}
	
	public void setDesc(TupleDesc td) {
		//your code here;
		this.tupleDesc = td;
		this.fieldVals = new Field[td.numFields()];
	}
	
	/**
	 * Stores the given data at the i-th field
	 * @param i the field number to store the data
	 * @param v the data
	 */
	public void setField(int i, Field v) {
		//your code here
		this.fieldVals[i] = v;
	}
	
	public Field getField(int i) {
		//your code here
		return this.fieldVals[i];
	}
	
	/**
	 * Creates a string representation of this tuple that displays its contents.
	 * You should convert the binary data into a readable format (i.e. display the ints in base-10 and convert
	 * the String columns to readable text).
	 */
	public String toString() {
		//your code here
		  StringJoiner sj = new StringJoiner(", ");
		    for (Field fieldValue : fieldVals) {
		        sj.add(fieldValue.toString());
		    }
		    return sj.toString();
	}
}
	