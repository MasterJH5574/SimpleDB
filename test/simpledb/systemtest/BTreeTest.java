package simpledb.systemtest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import org.junit.After;
import org.junit.Test;
import simpledb.BTreeFile;
import simpledb.BTreeUtility;
import simpledb.BTreeUtility.BTreeDeleter;
import simpledb.BTreeUtility.BTreeInserter;
import simpledb.BufferPool;
import simpledb.Database;
import simpledb.DbFileIterator;
import simpledb.Field;
import simpledb.IndexPredicate;
import simpledb.IntField;
import simpledb.Predicate.Op;
import simpledb.TransactionId;
import simpledb.Tuple;

/**
 * System test for the BTree
 */
public class BTreeTest extends SimpleDbTestBase {

  private final static Random r = new Random();

  private static final int POLL_INTERVAL = 100;

  /**
   * Helper method to clean up the syntax of starting a BTreeInserter thread. The parameters pass
   * through to the BTreeInserter constructor.
   */
  public BTreeInserter startInserter(BTreeFile bf, int[] tupdata,
      BlockingQueue<ArrayList<Integer>> insertedTuples) {

    BTreeInserter bi = new BTreeInserter(bf, tupdata, insertedTuples);
    bi.start();
    return bi;
  }

  /**
   * Helper method to clean up the syntax of starting a BTreeDeleter thread. The parameters pass
   * through to the BTreeDeleter constructor.
   */
  public BTreeDeleter startDeleter(BTreeFile bf, BlockingQueue<ArrayList<Integer>> insertedTuples) {

    BTreeDeleter bd = new BTreeDeleter(bf, insertedTuples);
    bd.start();
    return bd;
  }

  private void waitForInserterThreads(ArrayList<BTreeInserter> insertThreads) throws Exception {
    Thread.sleep(POLL_INTERVAL);
    for (BTreeInserter thread : insertThreads) {
      while (!thread.succeeded() && thread.getError() == null) {
        Thread.sleep(POLL_INTERVAL);
      }
    }
  }

  private void waitForDeleterThreads(ArrayList<BTreeDeleter> deleteThreads) throws Exception {
    Thread.sleep(POLL_INTERVAL);
    for (BTreeDeleter thread : deleteThreads) {
      while (!thread.succeeded() && thread.getError() == null) {
        Thread.sleep(POLL_INTERVAL);
      }
    }
  }

  private int[] getRandomTupleData() {
    int item1 = r.nextInt(BTreeUtility.MAX_RAND_VALUE);
    int item2 = r.nextInt(BTreeUtility.MAX_RAND_VALUE);
    return new int[]{item1, item2};
  }

  @After
  public void tearDown() throws Exception {
    // set the page size back to the default
    BufferPool.resetPageSize();
    Database.reset();
  }

  /**
   * Test that doing lots of inserts and deletes in multiple threads works
   */
  @Test
  public void testBigFile() throws Exception {
    // For this test we will decrease the size of the Buffer Pool pages
    BufferPool.setPageSize(1024);

    // This should create a B+ tree with a packed second tier of internal pages
    // and packed third tier of leaf pages
    System.out.println("Creating large random B+ tree...");
    ArrayList<ArrayList<Integer>> tuples = new ArrayList<ArrayList<Integer>>();
    BTreeFile bf = BTreeUtility.createRandomBTreeFile(2, 31000, null, tuples, 0);

    // we will need more room in the buffer pool for this test
    Database.resetBufferPool(500);

    ArrayBlockingQueue<ArrayList<Integer>> insertedTuples = new ArrayBlockingQueue<ArrayList<Integer>>(
        100000);
    insertedTuples.addAll(tuples);
    assertEquals(31000, insertedTuples.size());
    int size = insertedTuples.size();

    // now insert some random tuples
    System.out.println("Inserting tuples...");
    ArrayList<BTreeInserter> insertThreads = new ArrayList<BTreeInserter>();
    for (int i = 0; i < 200; i++) {
      BTreeInserter bi = startInserter(bf, getRandomTupleData(), insertedTuples);
      insertThreads.add(bi);
      // The first few inserts will cause pages to split so give them a little
      // more time to avoid too many deadlock situations
      Thread.sleep(r.nextInt(POLL_INTERVAL));
    }

    for (int i = 0; i < 800; i++) {
      BTreeInserter bi = startInserter(bf, getRandomTupleData(), insertedTuples);
      insertThreads.add(bi);
    }

    // wait for all threads to finish
    waitForInserterThreads(insertThreads);
    assertTrue(insertedTuples.size() > size);

    // now insert and delete tuples at the same time
    System.out.println("Inserting and deleting tuples...");
    ArrayList<BTreeDeleter> deleteThreads = new ArrayList<BTreeDeleter>();
    for (BTreeInserter thread : insertThreads) {
      thread.rerun(bf, getRandomTupleData(), insertedTuples);
      BTreeDeleter bd = startDeleter(bf, insertedTuples);
      deleteThreads.add(bd);
    }

    // wait for all threads to finish
    waitForInserterThreads(insertThreads);
    waitForDeleterThreads(deleteThreads);
    int numPages = bf.numPages();
    size = insertedTuples.size();

    // now delete a bunch of tuples
    System.out.println("Deleting tuples...");
    for (int i = 0; i < 10; i++) {
      for (BTreeDeleter thread : deleteThreads) {
        thread.rerun(bf, insertedTuples);
      }

      // wait for all threads to finish
      waitForDeleterThreads(deleteThreads);
    }
    assertTrue(insertedTuples.size() < size);
    size = insertedTuples.size();

    // now insert a bunch of random tuples again
    System.out.println("Inserting tuples...");
    for (int i = 0; i < 10; i++) {
      for (BTreeInserter thread : insertThreads) {
        thread.rerun(bf, getRandomTupleData(), insertedTuples);
      }

      // wait for all threads to finish
      waitForInserterThreads(insertThreads);
    }
    assertTrue(insertedTuples.size() > size);
    size = insertedTuples.size();
    // we should be reusing the deleted pages
    assertTrue(bf.numPages() < numPages + 20);

    // kill all the threads
    insertThreads = null;
    deleteThreads = null;

    ArrayList<ArrayList<Integer>> tuplesList = new ArrayList<ArrayList<Integer>>();
    tuplesList.addAll(insertedTuples);
    TransactionId tid = new TransactionId();

    // First look for random tuples and make sure we can find them
    System.out.println("Searching for tuples...");
    for (int i = 0; i < 10000; i++) {
      int rand = r.nextInt(insertedTuples.size());
      ArrayList<Integer> tuple = tuplesList.get(rand);
      IntField randKey = new IntField(tuple.get(bf.keyField()));
      IndexPredicate ipred = new IndexPredicate(Op.EQUALS, randKey);
      DbFileIterator it = bf.indexIterator(tid, ipred);
      it.open();
      boolean found = false;
      while (it.hasNext()) {
        Tuple t = it.next();
        if (tuple.equals(SystemTestUtil.tupleToList(t))) {
          found = true;
          break;
        }
      }
      assertTrue(found);
      it.close();
    }

    // now make sure all the tuples are in order and we have the right number
    System.out.println("Performing sanity checks...");
    DbFileIterator it = bf.iterator(tid);
    Field prev = null;
    it.open();
    int count = 0;
    while (it.hasNext()) {
      Tuple t = it.next();
      if (prev != null) {
        assertTrue(t.getField(bf.keyField()).compare(Op.GREATER_THAN_OR_EQ, prev));
      }
      prev = t.getField(bf.keyField());
      count++;
    }
    it.close();
    assertEquals(count, tuplesList.size());
    Database.getBufferPool().transactionComplete(tid);

    // set the page size back
    BufferPool.resetPageSize();

  }

  /**
   * Make test compatible with older version of ant.
   */
  public static junit.framework.Test suite() {
    return new junit.framework.JUnit4TestAdapter(BTreeTest.class);
  }
}
