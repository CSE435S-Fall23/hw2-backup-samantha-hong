package hw1;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * A class to perform various aggregations, by accepting one tuple at a time
 * @author Doug Shook
 *
 */

public class Aggregator {
    
    private TupleDesc tupleDesc;
    private ArrayList<Tuple> tupleList = new ArrayList<>();
    private ArrayList<Tuple> comparisonList = new ArrayList<>();
    private boolean isGroupBy;
    private AggregateOperator operator;

    public Aggregator(AggregateOperator o, boolean groupBy, TupleDesc td) {
    	//your code here
        this.operator = o;
        this.isGroupBy = groupBy;
        this.tupleDesc = td;
    }
    
    /**
	 * Merges the given tuple into the current aggregation
	 * @param t the tuple to be aggregated
	 */
    public void merge(Tuple t) {
    	//your code here
    
        if (isGroupBy) {
           
            Type fieldType = t.getDesc().getType(0);
            Field fieldToMerge;
           
            if (fieldType == Type.STRING) {
                fieldToMerge = new StringField(t.getField(0).toByteArray());
            } else {
                fieldToMerge = new IntField(t.getField(0).toByteArray());
            }

            Tuple newTuple = new Tuple(tupleDesc);
            newTuple.setField(0, fieldToMerge);

           
            if (!comparisonList.contains(newTuple)) {
                comparisonList.add(newTuple);
            }
        }

        tupleList.add(t);
    }
    
    /**
	 * Returns the result of the aggregation
	 * @return a list containing the tuples after aggregation
	 */

    public ArrayList<Tuple> getResults() {
        ArrayList<Tuple> result = new ArrayList<>();

        if (operator == AggregateOperator.COUNT) {
            if (!isGroupBy) {
                int count = tupleList.size();
                Tuple newTuple = new Tuple(tupleDesc);
                IntField b = new IntField(count);
                newTuple.setField(0, b);
                result.add(newTuple);
            } else {
                for (int i = 0; i < comparisonList.size(); i++) {
                    int count = countOccurrences(comparisonList.get(i));
                    Tuple newTuple = new Tuple(tupleDesc);
                    newTuple.setField(0, comparisonList.get(i).getField(0));
                    IntField b = new IntField(count);
                    newTuple.setField(1, b);
                    result.add(newTuple);
                }
            }
        } else if (operator == AggregateOperator.SUM) {
            if (isGroupBy) {
                result.addAll(computeSumByGroup());
            } else {
                int sum = computeSum();
                Tuple newTuple = new Tuple(tupleDesc);
                IntField b = new IntField(sum);
                newTuple.setField(0, b);
                result.add(newTuple);
            }
        } else if (operator == AggregateOperator.AVG) {
            if (isGroupBy) {
                result.addAll(computeAvgByGroup());
            } else {
                int avg = computeAvg();
                Tuple newTuple = new Tuple(tupleDesc);
                IntField b = new IntField(avg);
                newTuple.setField(0, b);
                
                result.add(newTuple);
            }
        } else if (operator == AggregateOperator.MAX) {
            result.addAll(computeMax(isGroupBy));
        } else if (operator == AggregateOperator.MIN) {
            result.addAll(computeMin(isGroupBy));
        }

        return result;
    }

    private int countOccurrences(Tuple targetTuple) {
        int count = 0;
        for (Tuple t : tupleList) {
            if (t.equals(targetTuple)) {
                count++;
            }
        }
        return count;
    }

    private ArrayList<Tuple> computeSumByGroup() {
        ArrayList<Tuple> list = new ArrayList<>();
        Type[] types = new Type[]{tupleDesc.getType(0), Type.INT};
        String[] fields = new String[]{tupleDesc.getFieldName(0), "SUM"};
        TupleDesc newTupleDesc = new TupleDesc(types, fields);

        if (tupleDesc.getType(0) == Type.INT && tupleDesc.getType(1) == Type.INT) {
            return sumByGroup(newTupleDesc, Integer.class);
        } else if (tupleDesc.getType(0) == Type.STRING && tupleDesc.getType(1) == Type.INT) {
            return sumByGroup(newTupleDesc, String.class);
        }
        return list;
    }

    private <T> ArrayList<Tuple> sumByGroup(TupleDesc newTupleDesc, Class<T> keyType) {
        Map<T, Integer> name = new HashMap<>();
        ArrayList<Tuple> list = new ArrayList<>();

        for (int i = 0; i < tupleList.size(); i++) {
            T key = getKey(tupleList.get(i), keyType);
            if (name.containsKey(key)) {
                int index = name.get(key);
                Tuple existingTuple = list.get(index);
                IntField existingValue = (IntField) existingTuple.getField(1);
                IntField valueToAdd = (IntField) tupleList.get(i).getField(1);
                int sum = existingValue.getValue() + valueToAdd.getValue();
                existingTuple.setField(1, new IntField(sum));
            } else {
                Tuple newTuple = new Tuple(newTupleDesc);
                newTuple.setField(0, tupleList.get(i).getField(0));
                newTuple.setField(1, tupleList.get(i).getField(1));
                list.add(newTuple);
                name.put(key, list.size() - 1);
            }
        }
        return list;
    }

    private <T> T getKey(Tuple tuple, Class<T> keyType) {
        if (keyType == Integer.class) {
            return keyType.cast(tuple.getField(0).hashCode());
        } else {
            return keyType.cast(tuple.getField(0).toString());
        }
    }

    private int computeSum() {
        int sum = 0;
        for (Tuple t : tupleList) {
            Type type = t.getDesc().getType(0);
            if (type == Type.INT) {
                int value = ((IntField) t.getField(0)).getValue();
                sum += value;
            }
        }
        return sum;
    }

    private ArrayList<Tuple> computeAvgByGroup() {
        ArrayList<Tuple> list = new ArrayList<>();
        for (int i = 0; i < comparisonList.size(); i++) {
            int avg = computeAverage(comparisonList.get(i));
          
            Tuple newTuple = new Tuple(tupleDesc);
            newTuple.setField(0, comparisonList.get(i).getField(0));
            IntField b = new IntField(avg);
            newTuple.setField(1, b);
          
            list.add(newTuple);
        }
        return list;
    }

    private int computeAverage(Tuple targetTuple) {
        int sum = 0;
        int size = 0;
        for (Tuple t : tupleList) {
            if (t.getField(0).equals(targetTuple.getField(0))) {
                IntField intValue = (IntField) t.getField(1);
                sum += intValue.getValue();
                size++;
            }
        }
        return sum / size;
    }

    private int computeAvg() {
        int sum = 0;
        for (Tuple t : tupleList) {
            int temp = t.getField(0).hashCode();
            sum += temp;
        }
        return sum / tupleList.size();
    }

    private ArrayList<Tuple> computeMax(boolean isGroupBy) {
        ArrayList<Tuple> list = new ArrayList<>();
        if (!isGroupBy) {
            Tuple maxTuple = computeMaxOrMin(tupleList, false);
            list.add(maxTuple);
        } else {
            for (int i = 0; i < comparisonList.size(); i++) {
                Tuple maxTuple = computeMaxOrMinByGroup(comparisonList.get(i), tupleList, false);
                list.add(maxTuple);
            }
        }
        return list;
    }

    private Tuple computeMaxOrMin(ArrayList<Tuple> tuples, boolean isMax) {
        int value = isMax ? Integer.MIN_VALUE : Integer.MAX_VALUE;
        Tuple result = null;

        for (Tuple t : tuples) {
            int fieldValue = ((IntField) t.getField(0)).getValue();
            if ((isMax && fieldValue > value) || (!isMax && fieldValue < value)) {
                value = fieldValue;
                result = t;
            }
        }

        return result;
    }

    private Tuple computeMaxOrMinByGroup(Tuple targetTuple, ArrayList<Tuple> tuples, boolean isMax) {
        int value = isMax ? Integer.MIN_VALUE : Integer.MAX_VALUE;
        Tuple result = null;

        for (Tuple t : tuples) {
            if (t.getField(0).equals(targetTuple.getField(0))) {
                int fieldValue = ((IntField) t.getField(1)).getValue();
                if ((isMax && fieldValue > value) || (!isMax && fieldValue < value)) {
                    value = fieldValue;
                    result = t;
                }
            }
        }

        return result;
    }

    private ArrayList<Tuple> computeMin(boolean isGroupBy) {
        ArrayList<Tuple> list = new ArrayList<>();
        if (!isGroupBy) {
            Tuple minTuple = computeMaxOrMin(tupleList, true);
            list.add(minTuple);
        } else {
            for (int i = 0; i < comparisonList.size(); i++) {
                Tuple minTuple = computeMaxOrMinByGroup(comparisonList.get(i), tupleList, true);
                list.add(minTuple);
            }
        }
        return list;
    }

  
}
