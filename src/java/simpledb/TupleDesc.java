package simpledb;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * TupleDesc describes the schema of a tuple.
 */
public final class TupleDesc implements Serializable {

  /**
   * A help class to facilitate organizing the information of each field
   */
  public static class TDItem implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * The type of the field
     */
    public final Type fieldType;

    /**
     * The name of the field
     */
    public final String fieldName;

    public TDItem(Type t, String n) {
      this.fieldName = n;
      this.fieldType = t;
    }

    @Override
    public String toString() {
      return fieldName + "(" + fieldType + ")";
    }

    @Override
    public int hashCode() {
      return fieldType.hashCode();
    }
  }

  /**
   * The TDItems of tuples corresponding to this TupleDesc
   */
  private final TDItem[] items;
  /**
   * The length of tuples corresponding to this TupleDesc
   */
  private final int numFields;
  /**
   * The size (in bytes) of tuples corresponding to this TupleDesc
   */
  private final int size;
  /**
   * A mapping which maps fieldNames to corresponding indices.
   */
  private final Map<String, Integer> nameIndexMap;

  /**
   * Helper to calculate the size (in bytes) of tuples corresponding to this TupleDesc
   *
   * @return Size in bytes of tuples corresponding to this TupleDesc
   */
  private int calculateSize() {
    int size = 0;
    for (TDItem item : this.items) {
      size += item.fieldType.getLen();
    }
    return size;
  }

  /**
   * Helper to construct a mapping from names to corresponding indices
   *
   * @return The map constructed
   */
  private Map<String, Integer> constructNameIndexMap() {
    Map<String, Integer> nameIndexMap = new HashMap<>();
    for (int i = 0; i < this.numFields; ++i) {
      nameIndexMap.put(items[i].fieldName, i);
    }
    return nameIndexMap;
  }

  /**
   * @return An iterator which iterates over all the field TDItems that are included in this
   * TupleDesc
   */
  public Iterator<TDItem> iterator() {
    return new Iterator<TDItem>() {
      private int curIdx = 0;

      @Override
      public boolean hasNext() {
        return curIdx < numFields;
      }

      @Override
      public TDItem next() {
        return items[curIdx++];
      }
    };
  }

  private static final long serialVersionUID = 1L;

  /**
   * Create a new TupleDesc with typeAr.length fields with fields of the specified types, with
   * associated named fields.
   *
   * @param typeAr  array specifying the number of and types of fields in this TupleDesc. It must
   *                contain at least one entry.
   * @param fieldAr array specifying the names of the fields. Note that names may be null.
   */
  public TupleDesc(Type[] typeAr, String[] fieldAr) {
    assert typeAr.length == fieldAr.length;
    this.numFields = typeAr.length;
    this.items = new TDItem[this.numFields];
    for (int i = 0; i < this.numFields; ++i) {
      this.items[i] = new TDItem(typeAr[i], fieldAr[i]);
    }
    this.size = this.calculateSize();
    this.nameIndexMap = this.constructNameIndexMap();
  }

  /**
   * Constructor. Create a new tuple desc with typeAr.length fields with fields of the specified
   * types, with anonymous (unnamed) fields.
   *
   * @param typeAr array specifying the number of and types of fields in this TupleDesc. It must
   *               contain at least one entry.
   */
  public TupleDesc(Type[] typeAr) {
    this.numFields = typeAr.length;
    this.items = new TDItem[this.numFields];
    for (int i = 0; i < this.numFields; ++i) {
      this.items[i] = new TDItem(typeAr[i], null);
    }
    this.size = this.calculateSize();
    this.nameIndexMap = this.constructNameIndexMap();
  }

  /**
   * @return the number of fields in this TupleDesc
   */
  public int numFields() {
    return this.numFields;
  }

  /**
   * Gets the (possibly null) field name of the ith field of this TupleDesc.
   *
   * @param i index of the field name to return. It must be a valid index.
   * @return the name of the ith field
   * @throws NoSuchElementException if i is not a valid field reference.
   */
  public String getFieldName(int i) throws NoSuchElementException {
    if (i < 0 || i >= this.numFields) {
      throw new NoSuchElementException();
    }
    return this.items[i].fieldName;
  }

  /**
   * Gets the type of the ith field of this TupleDesc.
   *
   * @param i The index of the field to get the type of. It must be a valid index.
   * @return the type of the ith field
   * @throws NoSuchElementException if i is not a valid field reference.
   */
  public Type getFieldType(int i) throws NoSuchElementException {
    if (i < 0 || i >= this.numFields) {
      throw new NoSuchElementException();
    }
    return this.items[i].fieldType;
  }

  /**
   * Find the index of the field with a given name.
   *
   * @param name name of the field.
   * @return the index of the field that is first to have the given name.
   * @throws NoSuchElementException if no field with a matching name is found.
   */
  public int fieldNameToIndex(String name) throws NoSuchElementException {
    Integer index = this.nameIndexMap.get(name);
    if (index == null) {
      throw new NoSuchElementException();
    }
    return index;
  }

  /**
   * @return The size (in bytes) of tuples corresponding to this TupleDesc. Note that tuples from a
   * given TupleDesc are of a fixed size.
   */
  public int getSize() {
    return this.size;
  }

  /**
   * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields, with the first
   * td1.numFields coming from td1 and the remaining from td2.
   *
   * @param td1 The TupleDesc with the first fields of the new TupleDesc
   * @param td2 The TupleDesc with the last fields of the TupleDesc
   * @return the new TupleDesc
   */
  public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
    int td1Len = td1.numFields, td2Len = td2.numFields;

    Type[] types = new Type[td1Len + td2Len];
    String[] names = new String[td1Len + td2Len];
    for (int i = 0; i < td1Len; ++i) {
      types[i] = td1.items[i].fieldType;
      names[i] = td1.items[i].fieldName;
    }
    for (int i = 0; i < td2Len; ++i) {
      types[td1Len + i] = td2.items[i].fieldType;
      names[td1Len + i] = td2.items[i].fieldName;
    }

    return new TupleDesc(types, names);
  }

  /**
   * Compares the specified object with this TupleDesc for equality. Two TupleDescs are considered
   * equal if they are the same size and if the n-th type in this TupleDesc is equal to the n-th
   * type in td.
   *
   * @param o the Object to be compared for equality with this TupleDesc.
   * @return true if the object is equal to this TupleDesc.
   */
  @Override
  public boolean equals(Object o) {
    if (!(o instanceof TupleDesc)) {
      return false;
    }
    if (((TupleDesc) o).numFields != this.numFields) {
      return false;
    }
    for (int i = 0; i < this.numFields; ++i) {
      if (this.items[i].fieldType != ((TupleDesc) o).items[i].fieldType) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int hashCode() {
    // If you want to use TupleDesc as keys for HashMap, implement this so
    // that equal objects have equals hashCode() results
    return Arrays.asList(this.items).hashCode();
  }

  /**
   * Returns a String describing this descriptor. It should be of the form
   * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although the exact format does
   * not matter.
   *
   * @return String describing this descriptor.
   */
  @Override
  public String toString() {
    String[] strArr = new String[this.numFields];
    for (int i = 0; i < this.numFields; ++i) {
      String name = this.items[i].fieldName != null ? this.items[i].fieldName : "NONAME";
      strArr[i] = this.items[i].fieldType.name() + "(" + name + ")";
    }
    return "[" + String.join(", ", strArr) + "]";
  }
}
