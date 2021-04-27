package simpledb;

import java.util.NoSuchElementException;

/**
 * SeqScan is an implementation of a sequential scan access method that reads each tuple of a table
 * in no particular order (e.g., as they are laid out on disk).
 */
public class SeqScan implements DbIterator {

  private static final long serialVersionUID = 1L;

  /**
   * The transaction id
   */
  private final TransactionId trId;
  /**
   * The table id
   */
  private int tid;
  /**
   * The alias of the table
   */
  private String tableAlias;
  /**
   * The file iterator of the underlying file
   */
  private DbFileIterator fileIt;

  /**
   * Creates a sequential scan over the specified table as a part of the specified transaction.
   *
   * @param tid        The transaction this scan is running as a part of.
   * @param tableid    the table to scan.
   * @param tableAlias the alias of this table (needed by the parser); the returned tupleDesc should
   *                   have fields with name tableAlias.fieldName (note: this class is not
   *                   responsible for handling a case where tableAlias or fieldName are null. It
   *                   shouldn't crash if they are, but the resulting name can be null.fieldName,
   *                   tableAlias.null, or null.null).
   */
  public SeqScan(TransactionId tid, int tableid, String tableAlias) {
    this.trId = tid;
    this.tid = tableid;
    this.tableAlias = tableAlias;
    this.fileIt = null;
  }

  /**
   * @return return the table name of the table the operator scans. This should be the actual name
   * of the table in the catalog of the database
   */
  public String getTableName() {
    return null;
  }

  /**
   * @return Return the alias of the table this operator scans.
   */
  public String getAlias() {
    return tableAlias;
  }

  /**
   * Reset the tableid, and tableAlias of this operator.
   *
   * @param tableid    the table to scan.
   * @param tableAlias the alias of this table (needed by the parser); the returned tupleDesc should
   *                   have fields with name tableAlias.fieldName (note: this class is not
   *                   responsible for handling a case where tableAlias or fieldName are null. It
   *                   shouldn't crash if they are, but the resulting name can be null.fieldName,
   *                   tableAlias.null, or null.null).
   */
  public void reset(int tableid, String tableAlias) {
    this.tid = tableid;
    this.tableAlias = tableAlias;
  }

  public SeqScan(TransactionId tid, int tableid) {
    this(tid, tableid, Database.getCatalog().getTableName(tableid));
  }

  public void open() throws DbException, TransactionAbortedException {
    fileIt = Database.getCatalog().getDatabaseFile(tid).iterator(trId);
    fileIt.open();
  }

  /**
   * Returns the TupleDesc with field names from the underlying HeapFile, prefixed with the
   * tableAlias string from the constructor. This prefix becomes useful when joining tables
   * containing a field(s) with the same name.  The alias and name should be separated with a "."
   * character (e.g., "alias.fieldName").
   *
   * @return the TupleDesc with field names from the underlying HeapFile, prefixed with the
   * tableAlias string from the constructor.
   */
  public TupleDesc getTupleDesc() {
    TupleDesc td = Database.getCatalog().getTupleDesc(tid);
    int numFields = td.numFields();
    Type[] types = new Type[numFields];
    String[] names = new String[numFields];
    for (int i = 0; i < numFields; ++i) {
      types[i] = td.getFieldType(i);
      names[i] = tableAlias + "." + td.getFieldName(i);
    }
    return new TupleDesc(types, names);
  }

  public boolean hasNext() throws TransactionAbortedException, DbException {
    return fileIt.hasNext();
  }

  public Tuple next() throws NoSuchElementException, TransactionAbortedException, DbException {
    return fileIt.next();
  }

  public void close() {
    if (fileIt != null) {
      fileIt.close();
      fileIt = null;
    }
  }

  public void rewind() throws DbException, NoSuchElementException, TransactionAbortedException {
    fileIt = Database.getCatalog().getDatabaseFile(tid).iterator(trId);
    fileIt.open();
  }
}
