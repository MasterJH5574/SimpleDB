package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import simpledb.utils.Pair;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

  private static final long serialVersionUID = 1L;

  /**
   * @see IntegerAggregator@constructor
   */
  private final int gbfield;
  private final Type gbfieldtype;
  private final int afield;
  private final Op what;
  /**
   * The aggregate value for each group. When `gbfield` is `NO_GROUPING`, we let the group value be
   * `null` for all tuples.
   */
  private final Map<Field, Object> groupAggregateValue;
  /**
   * The TupleDesc of the aggregate result
   */
  private final TupleDesc aggregateTd;


  /**
   * Aggregate constructor
   *
   * @param gbfield     the 0-based index of the group-by field in the tuple, or NO_GROUPING if
   *                    there is no grouping
   * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no
   *                    grouping
   * @param afield      the 0-based index of the aggregate field in the tuple
   * @param what        the aggregation operator
   */
  public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
    this.gbfield = gbfield;
    this.gbfieldtype = gbfieldtype;
    this.afield = afield;
    this.what = what;
    this.groupAggregateValue = new HashMap<>();

    if (gbfield == NO_GROUPING) {
      this.aggregateTd = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"aggregateValue"});
    } else {
      this.aggregateTd = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE},
          new String[]{"groupVal", "aggregateValue"});
    }
  }

  /**
   * Get the initial value for an aggregator of operation `what`.
   *
   * @return The initial value required
   */
  private Object getInitialValue() {
    switch (what) {
      case MIN: return Integer.MAX_VALUE;
      case MAX: return Integer.MIN_VALUE;
      case SUM: case COUNT: return 0;
      case AVG: return new Pair<>(0, 0);
    }
    assert false;
    return null;
  }

  /**
   * Aggregate the `aggregateValue` with the new value
   *
   * @param aggregateValue The old aggregate value
   * @param newValue The new value
   * @return The new aggregate value
   */
  private Object aggregate(Object aggregateValue, int newValue) {
    switch (what) {
      case MIN: return Integer.min(((Integer) aggregateValue), newValue);
      case MAX: return Integer.max(((Integer) aggregateValue), newValue);
      case SUM: return ((Integer) aggregateValue) + newValue;
      case COUNT: return ((Integer) aggregateValue) + 1;
      case AVG:
        assert aggregateValue instanceof Pair;
        @SuppressWarnings("unchecked")
        Pair<Integer, Integer> pair = ((Pair<Integer, Integer>) aggregateValue);
        return new Pair<>(pair.first + newValue, pair.second + 1);
    }
    assert false;
    return null;
  }

  /**
   * Merge a new tuple into the aggregate, grouping as indicated in the constructor
   *
   * @param tup the Tuple containing an aggregate field and a group-by field
   */
  public void mergeTupleIntoGroup(Tuple tup) {
    assert tup.getTupleDesc().getFieldType(afield) == Type.INT_TYPE;
    Field gbField;
    if (gbfield != NO_GROUPING) {
      assert tup.getTupleDesc().getFieldType(gbfield) == gbfieldtype;
      gbField = tup.getField(gbfield);
    } else {
      gbField = null;
    }

    Object gbAggregateValue = groupAggregateValue.get(gbField);
    if (gbAggregateValue == null) {
      gbAggregateValue = getInitialValue();
    }
    gbAggregateValue = aggregate(gbAggregateValue, ((IntField) tup.getField(afield)).getValue());
    groupAggregateValue.put(gbField, gbAggregateValue);
  }

  /**
   * Create a DbIterator over group aggregate results.
   *
   * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal) if using group, or a
   * single (aggregateVal) if no grouping. The aggregateVal is determined by the type of aggregate
   * specified in the constructor.
   */
  public DbIterator iterator() {
    assert gbfield != NO_GROUPING || groupAggregateValue.size() == 1
        && groupAggregateValue.keySet().iterator().next() == null;

    ArrayList<Tuple> tuples = new ArrayList<>();
    for (Entry<Field, Object> e : groupAggregateValue.entrySet()) {
      Field groupValue = e.getKey();
      int aggregateValue;
      if (what == Op.AVG) {
        @SuppressWarnings("unchecked")
        Pair<Integer, Integer> pair = ((Pair<Integer, Integer>) e.getValue());
        aggregateValue = pair.first / pair.second;
      } else {
        aggregateValue = ((Integer) e.getValue());
      }

      Tuple tuple = new Tuple(aggregateTd);
      if (gbfield == NO_GROUPING) {
        tuple.setField(0, new IntField(aggregateValue));
      } else {
        tuple.setField(0, groupValue);
        tuple.setField(1, new IntField(aggregateValue));
      }
      tuples.add(tuple);
    }

    return new TupleIterator(aggregateTd, tuples);
  }

}
