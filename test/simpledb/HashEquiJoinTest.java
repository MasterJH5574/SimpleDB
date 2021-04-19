package simpledb;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import junit.framework.JUnit4TestAdapter;
import org.junit.Before;
import org.junit.Test;
import simpledb.systemtest.SimpleDbTestBase;
import simpledb.systemtest.SystemTestUtil;

public class HashEquiJoinTest extends SimpleDbTestBase {

  int width1 = 2;
  int width2 = 3;
  DbIterator scan1;
  DbIterator scan2;
  DbIterator eqJoin;
  DbIterator gtJoin;

  /**
   * Initialize each unit test
   */
  @Before
  public void createTupleLists() throws Exception {
    this.scan1 = TestUtil.createTupleList(width1, new int[]{1, 2, 3, 4, 5, 6, 7, 8});
    this.scan2 = TestUtil
        .createTupleList(width2, new int[]{1, 2, 3, 2, 3, 4, 3, 4, 5, 4, 5, 6, 5, 6, 7});
    this.eqJoin = TestUtil
        .createTupleList(width1 + width2, new int[]{1, 2, 1, 2, 3, 3, 4, 3, 4, 5, 5, 6, 5, 6, 7});

  }

  /**
   * Unit test for Join.getTupleDesc()
   */
  @Test
  public void getTupleDesc() {
    JoinPredicate pred = new JoinPredicate(0, Predicate.Op.EQUALS, 0);
    HashEquiJoin op = new HashEquiJoin(pred, scan1, scan2);
    TupleDesc expected = Utility.getTupleDesc(width1 + width2);
    TupleDesc actual = op.getTupleDesc();
    assertEquals(expected, actual);
  }

  /**
   * Unit test for Join.getNext() using an = predicate
   */
  @Test
  public void eqJoin() throws Exception {
    JoinPredicate pred = new JoinPredicate(0, Predicate.Op.EQUALS, 0);
    HashEquiJoin op = new HashEquiJoin(pred, scan1, scan2);
    op.open();
    eqJoin.open();
    TestUtil.matchAllTuples(eqJoin, op);
  }

  private static final int COLUMNS = 2;

  public void validateJoin(int table1ColumnValue, int table1Rows, int table2ColumnValue,
      int table2Rows) throws IOException, DbException, TransactionAbortedException {
    // Create the two tables
    HashMap<Integer, Integer> columnSpecification = new HashMap<Integer, Integer>();
    columnSpecification.put(0, table1ColumnValue);
    ArrayList<ArrayList<Integer>> t1Tuples = new ArrayList<ArrayList<Integer>>();
    HeapFile table1 = SystemTestUtil
        .createRandomHeapFile(COLUMNS, table1Rows, columnSpecification, t1Tuples);
    assert t1Tuples.size() == table1Rows;

    columnSpecification.put(0, table2ColumnValue);
    ArrayList<ArrayList<Integer>> t2Tuples = new ArrayList<ArrayList<Integer>>();
    HeapFile table2 = SystemTestUtil
        .createRandomHeapFile(COLUMNS, table2Rows, columnSpecification, t2Tuples);
    assert t2Tuples.size() == table2Rows;

        /*
        // Generate the expected results
        ArrayList<ArrayList<Integer>> expectedResults = new ArrayList<ArrayList<Integer>>();
        for (ArrayList<Integer> t1 : t1Tuples) {
            for (ArrayList<Integer> t2 : t2Tuples) {
                // If the columns match, join the tuples
                if (t1.get(0).equals(t2.get(0))) {
                    ArrayList<Integer> out = new ArrayList<Integer>(t1);
                    out.addAll(t2);
                    expectedResults.add(out);
                }
            }
        }
        */

    // Begin the join
    TransactionId tid = new TransactionId();
    SeqScan ss1 = new SeqScan(tid, table1.getId(), "");
    SeqScan ss2 = new SeqScan(tid, table2.getId(), "");
    JoinPredicate p = new JoinPredicate(0, Predicate.Op.EQUALS, 0);
    HashEquiJoin joinOp = new HashEquiJoin(p, ss1, ss2);

    joinOp.open();

    int cnt = 0;
    while (joinOp.hasNext()) {
      Tuple t = joinOp.next();
      cnt++;
    }

    joinOp.close();
    Database.getBufferPool().transactionComplete(tid);
    int expected = (table1ColumnValue == table2ColumnValue) ? (table1Rows * table2Rows) : 0;
    System.out.println("JOIN PRODUCED " + cnt + " ROWS");
    assert (cnt == expected);
  }

  /**
   * Unit test for Join.getNext() using an = predicate
   */
  @Test
  public void bigJoin() throws Exception {
    validateJoin(1, 30001, 1, 10);
    validateJoin(1, 10, 1, 30001);
  }

  /**
   * JUnit suite target
   */
  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(HashEquiJoinTest.class);
  }
}

