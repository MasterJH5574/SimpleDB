package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a query.
 * <p>
 * This class is not needed in implementing lab1, lab2 and lab3.
 */
public class TableStats {

  /**
   * The DbFile of the table.
   */
  private final DbFile file;
  /**
   * The tuple descriptor of the table
   */
  private final TupleDesc td;
  /**
   * The number of tuples in the table
   */
  private final int nTuples;
  /**
   * The IO cost.
   */
  private final int ioCostPerPage;
  /**
   * The histograms of this table. The elements are either IntHistograms or StringHistograms.
   */
  private final Object[] histograms;

  private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

  static final int IOCOSTPERPAGE = 1000;

  public static TableStats getTableStats(String tablename) {
    return statsMap.get(tablename);
  }

  public static void setTableStats(String tablename, TableStats stats) {
    statsMap.put(tablename, stats);
  }

  public static void setStatsMap(HashMap<String, TableStats> s) {
    try {
      java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
      statsMapF.setAccessible(true);
      statsMapF.set(null, s);
    } catch (NoSuchFieldException e) {
      e.printStackTrace();
    } catch (SecurityException e) {
      e.printStackTrace();
    } catch (IllegalArgumentException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    }

  }

  public static Map<String, TableStats> getStatsMap() {
    return statsMap;
  }

  public static void computeStatistics() {
    Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

    System.out.println("Computing table stats.");
    while (tableIt.hasNext()) {
      int tableid = tableIt.next();
      TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
      setTableStats(Database.getCatalog().getTableName(tableid), s);
    }
    System.out.println("Done.");
  }

  /**
   * Number of bins for the histogram. Feel free to increase this value over 100, though our tests
   * assume that you have at least 100 bins in your histograms.
   */
  static final int NUM_HIST_BINS = 100;

  /**
   * Create a new TableStats object, that keeps track of statistics on each column of a table
   *
   * @param tableid       The table over which to compute statistics
   * @param ioCostPerPage The cost per page of IO. This doesn't differentiate between
   *                      sequential-scan IO and disk seeks.
   */
  public TableStats(int tableid, int ioCostPerPage) {
    // For this function, you'll have to get the
    // DbFile for the table in question,
    // then scan through its tuples and calculate
    // the values that you need.
    // You should try to do this reasonably efficiently, but you don't
    // necessarily have to (for example) do everything
    // in a single scan of the table.

    this.file = Database.getCatalog().getDatabaseFile(tableid);
    this.td = file.getTupleDesc();
    this.ioCostPerPage = ioCostPerPage;

    // Collect the minimums and the maximums of each IntHistogram.
    int nFields = td.numFields();
    int[] vMin = new int[nFields];
    int[] vMax = new int[nFields];
    for (int i = 0; i < nFields; ++i) {
      vMin[i] = Integer.MAX_VALUE;
      vMax[i] = Integer.MIN_VALUE;
    }

    int nTuples = 0;
    DbFileIterator it = file.iterator(new TransactionId());
    try {
      it.open();
      while (it.hasNext()) {
        Tuple tuple = it.next();
        nTuples++;
        for (int i = 0; i < nFields; ++i) {
          if (td.getFieldType(i) == Type.INT_TYPE) {
            int value = ((IntField) tuple.getField(i)).getValue();
            vMin[i] = Integer.min(vMin[i], value);
            vMax[i] = Integer.max(vMax[i], value);
          }
        }
      }
      it.close();
    } catch (Exception e) {
      e.printStackTrace();
      assert false;
    }
    this.nTuples = nTuples;

    // Create histograms.
    this.histograms = new Object[nFields];
    for (int i = 0; i < nFields; ++i) {
      if (td.getFieldType(i) == Type.INT_TYPE) {
        histograms[i] = new IntHistogram(NUM_HIST_BINS, vMin[i], vMax[i]);
      } else {
        histograms[i] = new StringHistogram(NUM_HIST_BINS);
      }
    }
    try {
      it.open();
      while (it.hasNext()) {
        Tuple tuple = it.next();
        for (int i = 0; i < nFields; ++i) {
          if (td.getFieldType(i) == Type.INT_TYPE) {
            int value = ((IntField) tuple.getField(i)).getValue();
            ((IntHistogram) histograms[i]).addValue(value);
          } else {
            String value = ((StringField) tuple.getField(i)).getValue();
            ((StringHistogram) histograms[i]).addValue(value);
          }
        }
      }
      it.close();
    } catch (Exception e) {
      e.printStackTrace();
      assert false;
    }
  }

  /**
   * Estimates the cost of sequentially scanning the file, given that the cost to read a page is
   * costPerPageIO. You can assume that there are no seeks and that no pages are in the buffer
   * pool.
   * <p>
   * Also, assume that your hard drive can only read entire pages at once, so if the last page of
   * the table only has one tuple on it, it's just as expensive to read as a full page. (Most real
   * hard drives can't efficiently address regions smaller than a page at a time.)
   *
   * @return The estimated cost of scanning the table.
   */
  public double estimateScanCost() {
    return ((HeapFile) file).numPages() * ioCostPerPage;
  }

  /**
   * This method returns the number of tuples in the relation, given that a predicate with
   * selectivity selectivityFactor is applied.
   *
   * @param selectivityFactor The selectivity of any predicates over the table
   * @return The estimated cardinality of the scan with the specified selectivityFactor
   */
  public int estimateTableCardinality(double selectivityFactor) {
    return ((int) (nTuples * selectivityFactor));
  }

  /**
   * The average selectivity of the field under op.
   *
   * @param field the index of the field
   * @param op    the operator in the predicate The semantic of the method is that, given the table,
   *              and then given a tuple, of which we do not know the value of the field, return the
   *              expected selectivity. You may estimate this value from the histograms.
   */
  public double avgSelectivity(int field, Predicate.Op op) {
    return 1.0;
  }

  /**
   * Estimate the selectivity of predicate <tt>field op constant</tt> on the table.
   *
   * @param field    The field over which the predicate ranges
   * @param op       The logical operation in the predicate
   * @param constant The value against which the field is compared
   * @return The estimated selectivity (fraction of tuples that satisfy) the predicate
   */
  public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
    if (td.getFieldType(field) == Type.INT_TYPE) {
      return ((IntHistogram) histograms[field])
          .estimateSelectivity(op, ((IntField) constant).getValue());
    } else {
      return ((StringHistogram) histograms[field])
          .estimateSelectivity(op, ((StringField) constant).getValue());
    }
  }

  /**
   * return the total number of tuples in this table
   */
  public int totalTuples() {
    return nTuples;
  }

}
