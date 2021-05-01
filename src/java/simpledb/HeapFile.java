package simpledb;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples in no particular
 * order. Tuples are stored on pages, each of which is a fixed size, and the file is simply a
 * collection of those pages. HeapFile works closely with HeapPage. The format of HeapPages is
 * described in the HeapPage constructor.
 *
 * @see simpledb.HeapPage#HeapPage
 */
public class HeapFile implements DbFile {
  /**
   * The physical file of this HeapFile
   */
  private final File f;
  /**
   * The schema of this HeapFile
   */
  private final TupleDesc td;

  /**
   * Constructs a heap file backed by the specified file.
   *
   * @param f the file that stores the on-disk backing store for this heap file.
   */
  public HeapFile(File f, TupleDesc td) {
    this.f = f;
    this.td = td;
  }

  /**
   * Returns the File backing this HeapFile on disk.
   *
   * @return the File backing this HeapFile on disk.
   */
  public File getFile() {
    return f;
  }

  /**
   * Returns an ID uniquely identifying this HeapFile. Implementation note: you will need to
   * generate this tableid somewhere ensure that each HeapFile has a "unique id," and that you
   * always return the same value for a particular HeapFile. We suggest hashing the absolute file
   * name of the file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
   *
   * @return an ID uniquely identifying this HeapFile.
   */
  public int getId() {
    return f.getAbsoluteFile().hashCode();
  }

  /**
   * Returns the TupleDesc of the table stored in this DbFile.
   *
   * @return TupleDesc of this DbFile.
   */
  public TupleDesc getTupleDesc() {
    return td;
  }

  // see DbFile.java for javadocs
  public Page readPage(PageId pid) {
    assert pid instanceof HeapPageId;
    int pageSize = BufferPool.getPageSize();
    int ofs = pageSize * pid.pageNumber();

    try {
      RandomAccessFile raf = new RandomAccessFile(f, "r");
      byte[] data = new byte[pageSize];
      raf.seek(ofs);
      raf.read(data, 0, pageSize);
      raf.close();

      return new HeapPage(((HeapPageId) pid), data);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  // see DbFile.java for javadocs
  public void writePage(Page page) throws IOException {
    RandomAccessFile raf = new RandomAccessFile(f, "rw");
    raf.seek((long) page.getId().pageNumber() * BufferPool.getPageSize());
    raf.write(page.getPageData());
  }

  /**
   * Returns the number of pages in this HeapFile.
   */
  public int numPages() {
    int pageSize = BufferPool.getPageSize();
    assert f.length() % pageSize == 0;
    return Math.toIntExact(f.length() / pageSize);
  }

  // see DbFile.java for javadocs
  public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
      throws DbException, IOException, TransactionAbortedException {
    for (int i = 0; i < this.numPages(); ++i) {
      HeapPage page = ((HeapPage) Database.getBufferPool()
          .getPage(tid, new HeapPageId(getId(), i), Permissions.READ_WRITE));

      if (page.getNumEmptySlots() == 0) {
        continue;
      }
      page.insertTuple(t);
      return new ArrayList<Page>(){{add(page);}};
    }

    HeapPage page = new HeapPage(new HeapPageId(getId(), numPages()),
        HeapPage.createEmptyPageData());
    page.insertTuple(t);
    this.writePage(page);
    return new ArrayList<>();
  }

  // see DbFile.java for javadocs
  public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
      TransactionAbortedException {
    HeapPageId pid = ((HeapPageId) t.getRecordId().getPageId());
    if (pid.getTableId() != getId()) {
      throw new DbException("");
    }

    HeapPage page = ((HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE));
    page.deleteTuple(t);
    return new ArrayList<Page>(){{add(page);}};
  }

  // see DbFile.java for javadocs
  public DbFileIterator iterator(TransactionId tid) {
    return new DbFileIterator() {
      private int curPgNo = -1;
      private final int numPages = numPages();
      private Iterator<Tuple> it = null;

      /**
       * Fetch a page from file
       * @param pgNo The page number of the page to read
       * @return The iterator of the tuples in the page to read
       */
      private Iterator<Tuple> fetchPage(int pgNo) throws DbException, TransactionAbortedException {
        // Table id is exactly the id of the DbFile in our design.
        int tableId = getId();
        PageId pid = new HeapPageId(tableId, pgNo);
        Page page = Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
        assert page instanceof HeapPage;
        return ((HeapPage) page).iterator();
      }

      @Override
      public void open() throws DbException, TransactionAbortedException {
        curPgNo = 0;
        it = fetchPage(curPgNo);
      }

      @Override
      public boolean hasNext() throws DbException, TransactionAbortedException {
        if (it == null) {
          return false;
        }
        if (it.hasNext()) {
          // If the current page has more unvisited tuple, just return true.
          return true;
        }
        while (curPgNo + 1 < numPages) {
          // If there is any unvisited page, get an iterator of the unvisited one, and check
          // `hasNext()` of the new iterator.
          it = fetchPage(++curPgNo);
          if (it.hasNext()) {
            return true;
          }
        }
        return false;
      }

      @Override
      public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        assert it.hasNext();
        return it.next();
      }

      @Override
      public void rewind() throws DbException, TransactionAbortedException {
        curPgNo = 0;
        it = fetchPage(curPgNo);
      }

      @Override
      public void close() {
        curPgNo = -1;
        it = null;
      }
    };
  }
}

