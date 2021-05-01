package simpledb;

import java.util.NoSuchElementException;
import simpledb.Aggregator.Op;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max, min). Note that we only
 * support aggregates over a single column, grouped by a single column.
 */
public class Aggregate extends Operator {

  private static final long serialVersionUID = 1L;

  /**
   * @see Aggregate@constructor
   */
  private DbIterator child;
  private final int afield;
  private final int gfield;
  private final Op aop;
  /**
   * The aggregator which aggregates the tuples given by `child`.
   */
  private Aggregator aggregator;
  /**
   * The iterator of the aggregator, which iterates all the tuples of the aggregation.
   */
  private DbIterator aggregatorIterator;

  /**
   * Constructor.
   * <p>
   * Implementation hint: depending on the type of afield, you will want to construct an {@link
   * IntegerAggregator} or {@link StringAggregator} to help you with your implementation of
   * readNext().
   *
   * @param child  The DbIterator that is feeding us tuples.
   * @param afield The column over which we are computing an aggregate.
   * @param gfield The column over which we are grouping the result, or -1 if there is no grouping
   * @param aop    The aggregation operator to use
   */
  public Aggregate(DbIterator child, int afield, int gfield, Op aop) {
    this.child = child;
    this.afield = afield;
    this.gfield = gfield;
    this.aop = aop;
    resetAggregator();
  }

  /**
   * Reset the aggregator
   */
  private void resetAggregator() {
    if (child.getTupleDesc().getFieldType(afield) == Type.INT_TYPE) {
      this.aggregator = new IntegerAggregator(gfield, groupFieldType(), afield, aop);
    } else {
      this.aggregator = new StringAggregator(gfield, groupFieldType(), afield, aop);
    }
  }

  /**
   * @return If this aggregate is accompanied by a groupby, return the groupby field index in the
   * <b>INPUT</b> tuples. If not, return {@link simpledb.Aggregator#NO_GROUPING}
   */
  public int groupField() {
    return gfield;
  }

  /**
   * @return If this aggregate is accompanied by a group by, return the name of the groupby field in
   * the <b>OUTPUT</b> tuples If not, return null;
   */
  public String groupFieldName() {
    return gfield == Aggregator.NO_GROUPING ? null : child.getTupleDesc().getFieldName(gfield);
  }

  /**
   * @return If this aggregate is accompanied by a group by, return the type of the groupby field in
   * the <b>OUTPUT</b> tuples If not, return null;
   */
  private Type groupFieldType() {
    return gfield == Aggregator.NO_GROUPING ? null : child.getTupleDesc().getFieldType(gfield);
  }

  /**
   * @return the aggregate field
   */
  public int aggregateField() {
    return afield;
  }

  /**
   * @return return the name of the aggregate field in the <b>OUTPUT</b> tuples
   */
  public String aggregateFieldName() {
    return child.getTupleDesc().getFieldName(afield);
  }

  /**
   * @return return the aggregate operator
   */
  public Aggregator.Op aggregateOp() {
    return aop;
  }

  public static String nameOfAggregatorOp(Aggregator.Op aop) {
    return aop.toString();
  }

  public void open() throws NoSuchElementException, DbException, TransactionAbortedException {
    super.open();
    resetAggregator();

    child.open();
    while (child.hasNext()) {
      aggregator.mergeTupleIntoGroup(child.next());
    }
    aggregatorIterator = aggregator.iterator();
    aggregatorIterator.open();
  }

  /**
   * Returns the next tuple. If there is a group by field, then the first field is the field by
   * which we are grouping, and the second field is the result of computing the aggregate, If there
   * is no group by field, then the result tuple should contain one field representing the result of
   * the aggregate. Should return null if there are no more tuples.
   */
  protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    return aggregatorIterator.hasNext() ? aggregatorIterator.next() : null;
  }

  public void rewind() throws DbException, TransactionAbortedException {
    close();
    open();
    resetAggregator();
  }

  /**
   * Returns the TupleDesc of this Aggregate. If there is no group by field, this will have one
   * field - the aggregate column. If there is a group by field, the first field will be the group
   * by field, and the second will be the aggregate value column.
   * <p>
   * The name of an aggregate column should be informative. For example: "aggName(aop)
   * (child_td.getFieldName(afield))" where aop and afield are given in the constructor, and
   * child_td is the TupleDesc of the child iterator.
   */
  public TupleDesc getTupleDesc() {
    return aggregator.iterator().getTupleDesc();
  }

  public void close() {
    child.close();
    super.close();
  }

  @Override
  public DbIterator[] getChildren() {
    return new DbIterator[]{child};
  }

  @Override
  public void setChildren(DbIterator[] children) {
    assert children.length == 1;
    child = children[0];
    resetAggregator();
  }

}
