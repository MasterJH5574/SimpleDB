package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

  private static final long serialVersionUID = 1L;

  /**
   * @see StringAggregator@constructor
   */
  private final int gbfield;
  private final Type gbfieldtype;
  private final int afield;
  /**
   * The aggregate value for each group. When `gbfield` is `NO_GROUPING`, we let the group value be
   * `null` for all tuples.
   */
  private final Map<Field, Integer> groupAggregateValue;
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
   * @param what        aggregation operator to use -- only supports COUNT
   * @throws IllegalArgumentException if what != COUNT
   */
  public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
    assert what == Op.COUNT;

    this.gbfield = gbfield;
    this.gbfieldtype = gbfieldtype;
    this.afield = afield;
    this.groupAggregateValue = new HashMap<>();

    if (gbfield == NO_GROUPING) {
      this.aggregateTd = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"aggregateValue"});
    } else {
      this.aggregateTd = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE},
          new String[]{"groupVal", "aggregateValue"});
    }
  }

  /**
   * Merge a new tuple into the aggregate, grouping as indicated in the constructor
   *
   * @param tup the Tuple containing an aggregate field and a group-by field
   */
  public void mergeTupleIntoGroup(Tuple tup) {
    assert tup.getTupleDesc().getFieldType(afield) == Type.STRING_TYPE;
    Field gbField;
    if (gbfield != NO_GROUPING) {
      assert tup.getTupleDesc().getFieldType(gbfield) == gbfieldtype;
      gbField = tup.getField(gbfield);
    } else {
      gbField = null;
    }

    Integer gbAggregateValue = groupAggregateValue.get(gbField);
    if (gbAggregateValue == null) {
      gbAggregateValue = 0;
    }
    gbAggregateValue = gbAggregateValue + 1;
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
    for (Entry<Field, Integer> e : groupAggregateValue.entrySet()) {
      Field groupValue = e.getKey();
      int aggregateValue = e.getValue();

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
