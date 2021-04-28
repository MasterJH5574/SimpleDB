package simpledb;

import java.io.Serializable;
import java.util.Iterator;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a specified schema
 * specified by a TupleDesc object and contain Field objects with the data for each field.
 */
public class Tuple implements Serializable {

  private static final long serialVersionUID = 1L;

  /**
   * The TupleDesc of this tuple
   */
  private TupleDesc td;
  /**
   * The fields of this tuple
   */
  private Field[] fields;
  /**
   * The RecordId of this tuple
   */
  private RecordId rid;

  /**
   * Create a new tuple with the specified schema (type).
   *
   * @param td the schema of this tuple. It must be a valid TupleDesc instance with at least one
   *           field.
   */
  public Tuple(TupleDesc td) {
    this.td = td;
    this.fields = new Field[td.getSize()];
    this.rid = null;
  }

  /**
   * @return The TupleDesc representing the schema of this tuple.
   */
  public TupleDesc getTupleDesc() {
    return td;
  }

  /**
   * @return The RecordId representing the location of this tuple on disk. May be null.
   */
  public RecordId getRecordId() {
    return rid;
  }

  /**
   * Set the RecordId information for this tuple.
   *
   * @param rid the new RecordId for this tuple.
   */
  public void setRecordId(RecordId rid) {
    this.rid = rid;
  }

  /**
   * Change the value of the ith field of this tuple.
   *
   * @param i index of the field to change. It must be a valid index.
   * @param f new value for the field.
   */
  public void setField(int i, Field f) {
    this.fields[i] = f;
  }

  /**
   * @param i field index to return. Must be a valid index.
   * @return the value of the ith field, or null if it has not been set.
   */
  public Field getField(int i) {
    return fields[i];
  }

  /**
   * Returns the contents of this Tuple as a string. Note that to pass the system tests, the format
   * needs to be as follows:
   * <p>
   * column1\tcolumn2\tcolumn3\t...\tcolumnN
   * <p>
   * where \t is any whitespace (except a newline)
   */
  @Override
  public String toString() {
    String[] strArr = new String[td.numFields()];
    for (int i = 0; i < td.numFields(); ++i) {
      strArr[i] = fields[i] != null ? fields[i].toString() : "FIELD_NOT_SET";
    }
    return String.join(" ", strArr);
  }

  /**
   * @return An iterator which iterates over all the fields of this tuple
   */
  public Iterator<Field> fields() {
    return new Iterator<Field>() {
      private int curIdx;

      @Override
      public boolean hasNext() {
        return curIdx < td.getSize();
      }

      @Override
      public Field next() {
        return fields[curIdx++];
      }
    };
  }

  /**
   * reset the TupleDesc of this tuple
   */
  public void resetTupleDesc(TupleDesc td) {
    this.td = td;
    this.fields = new Field[td.getSize()];
  }
}
