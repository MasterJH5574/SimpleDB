package simpledb;

import java.util.NoSuchElementException;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

  private static final long serialVersionUID = 1L;

  /**
   * @see Filter@constructor
   */
  private final Predicate p;
  /**
   * An iterator used for scanning the table to be filtered
   */
  private DbIterator child;

  /**
   * Constructor accepts a predicate to apply and a child operator to read tuples to filter from.
   *
   * @param p     The predicate to filter tuples with
   * @param child The child operator
   */
  public Filter(Predicate p, DbIterator child) {
    this.p = p;
    this.child = child;
  }

  public Predicate getPredicate() {
    return p;
  }

  public TupleDesc getTupleDesc() {
    return child.getTupleDesc();
  }

  public void open() throws DbException, NoSuchElementException, TransactionAbortedException {
    super.open();
    child.open();
  }

  public void close() {
    child.close();
    super.close();
  }

  public void rewind() throws DbException, TransactionAbortedException {
    child.rewind();
  }

  /**
   * AbstractDbIterator.readNext implementation. Iterates over tuples from the child operator,
   * applying the predicate to them and returning those that pass the predicate (i.e. for which the
   * Predicate.filter() returns true.)
   *
   * @return The next tuple that passes the filter, or null if there are no more tuples
   * @see Predicate#filter
   */
  protected Tuple fetchNext()
      throws NoSuchElementException, TransactionAbortedException, DbException {
    while (child.hasNext()) {
      Tuple tuple = child.next();
      if (p.filter(tuple)) {
        return tuple;
      }
    }
    return null;
  }

  @Override
  public DbIterator[] getChildren() {
    return new DbIterator[]{child};
  }

  @Override
  public void setChildren(DbIterator[] children) {
    assert children.length >= 1;
    child = children[0];
  }

}
