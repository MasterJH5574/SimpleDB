package simpledb;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

  /**
   * @see IntHistogram@constructor
   */
  private final int nBuckets;
  private final int vMin;
  private final int vMax;
  /**
   * Number of tuples.
   */
  private int nTuples;
  /**
   * The width of each bucket.
   */
  private final int width;
  /**
   * The number of integers in each bucket.
   */
  private final int[] size;
  /**
   * The left bound and right bound of each bucket.
   */
  private final int[] left;
  private final int[] right;

  /**
   * Create a new IntHistogram.
   * <p>
   * This IntHistogram should maintain a histogram of integer values that it receives. It should
   * split the histogram into "buckets" buckets.
   * <p>
   * The values that are being histogrammed will be provided one-at-a-time through the "addValue()"
   * function.
   * <p>
   * Your implementation should use space and have execution time that are both constant with
   * respect to the number of values being histogrammed.  For example, you shouldn't simply store
   * every value that you see in a sorted list.
   *
   * @param buckets The number of buckets to split the input value into.
   * @param min     The minimum integer value that will ever be passed to this class for
   *                histogramming
   * @param max     The maximum integer value that will ever be passed to this class for
   *                histogramming
   */
  public IntHistogram(int buckets, int min, int max) {
    this.nBuckets = buckets;
    this.vMin = min;
    this.vMax = max;
    this.nTuples = 0;
    this.width = (max - min + 1) / nBuckets;
    this.size = new int[nBuckets];
    this.left = new int[nBuckets];
    this.right = new int[nBuckets];
    for (int i = 0; i < nBuckets; ++i) {
      left[i] = min + i * width;
      if (i > 0) {
        right[i - 1] = left[i] - 1;
      }
    }
    right[nBuckets - 1] = max;

  }

  private int getBucket(int v) {
    if (v < vMin) {
      return -1;
    } else if (v > vMax) {
      return nBuckets;
    }
    int l = 0, r = nBuckets - 1, mid;
    while (l <= r) {
      mid = (l + r) / 2;
      if (v >= left[mid] && v <= right[mid]) {
        return mid;
      } else if (v < left[mid]) {
        r = mid - 1;
      } else {
        l = mid + 1;
      }
    }
    assert false;
    return -1;
  }

  private int getWidth(int bucket) {
    return right[bucket] - left[bucket] + 1;
  }

  /**
   * Add a value to the set of values that you are keeping a histogram of.
   *
   * @param v Value to add to the histogram
   */
  public void addValue(int v) {
    assert v >= vMin && v <= vMax;
    ++size[getBucket(v)];
    ++nTuples;
  }

  /**
   * Estimate the selectivity of a particular predicate and operand on this table.
   * <p>
   * For example, if "op" is "GREATER_THAN" and "v" is 5, return your estimate of the fraction of
   * elements that are greater than 5.
   *
   * @param op Operator
   * @param v  Value
   * @return Predicted selectivity of this particular operator and value
   */
  public double estimateSelectivity(Predicate.Op op, int v) {
    int bucket = getBucket(v);
    double estEqual = 0, estLess = 0, estGreater = 0;
    for (int i = 0; i < nBuckets; ++i) {
      if (i < bucket) {
        estLess += ((double) size[i]) / nTuples;
      } else if (i > bucket) {
        estGreater += ((double) size[i]) / nTuples;
      } else {
        estEqual = ((double) size[bucket]) / getWidth(bucket) / nTuples;
        estLess += estEqual * (v - left[i]);
        estGreater += estEqual * (right[i] - v);
      }
    }

    switch (op) {
      case EQUALS:
      case LIKE:
        return estEqual;
      case NOT_EQUALS:
        return 1 - estEqual;
      case LESS_THAN:
        return estLess;
      case LESS_THAN_OR_EQ:
        return estLess + estEqual;
      case GREATER_THAN:
        return estGreater;
      case GREATER_THAN_OR_EQ:
        return estGreater + estEqual;
    }
    assert false;
    return 0;
  }

  /**
   * @return the average selectivity of this histogram.
   * <p>
   * This is not an indispensable method to implement the basic join optimization. It may be needed
   * if you want to implement a more efficient optimization
   */
  public double avgSelectivity() {
    return 1.0;
  }

  /**
   * @return A string describing this histogram, for debugging purposes
   */
  public String toString() {
    StringBuilder str = new StringBuilder();
    str.append("{");
    for (int i = 0; i < nBuckets; ++i) {
      str.append("[").append(left[i]).append(", ").append(right[i]).append("]: ").append(size[i]);
      if (i != nBuckets - 1) {
        str.append(", ");
      }
    }
    return str.toString();
  }
}
