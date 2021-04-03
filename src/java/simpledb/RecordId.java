package simpledb;

import java.io.Serializable;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a specific table.
 */
public class RecordId implements Serializable {

  private static final long serialVersionUID = 1L;

  /**
   * The pageId of the page on which the tuple resides
   */
  private final PageId pid;
  /**
   * The tuple number within the page
   */
  private final int tupleNo;

  /**
   * Creates a new RecordId referring to the specified PageId and tuple number.
   *
   * @param pid     the pageId of the page on which the tuple resides
   * @param tupleNo the tuple number within the page.
   */
  public RecordId(PageId pid, int tupleNo) {
    this.pid = pid;
    this.tupleNo = tupleNo;
  }

  /**
   * @return the tuple number this RecordId references.
   */
  public int tupleno() {
    return tupleNo;
  }

  /**
   * @return the page id this RecordId references.
   */
  public PageId getPageId() {
    return pid;
  }

  /**
   * Two RecordId objects are considered equal if they represent the same tuple.
   *
   * @return True if this and o represent the same tuple
   */
  @Override
  public boolean equals(Object o) {
    if (!(o instanceof RecordId)) {
      return false;
    }
    return pid.equals(((RecordId) o).pid) && tupleNo == ((RecordId) o).tupleNo;
  }

  /**
   * You should implement the hashCode() so that two equal RecordId instances (with respect to
   * equals()) have the same hashCode().
   *
   * @return An int that is the same for equal RecordId objects.
   */
  @Override
  public int hashCode() {
    return ((int) Long.parseLong(Integer.toString(pid.hashCode()) + tupleNo));
  }
}
