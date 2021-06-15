package simpledb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import simpledb.LockManager.LockType;
import simpledb.utils.Pair;

/**
 * BufferPool manages the reading and writing of pages into memory from disk. Access methods call
 * into it to retrieve pages, and it fetches pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches a page, BufferPool
 * checks that the transaction has the appropriate locks to read/write the page.
 *
 * @Threadsafe all fields are final
 */
public class BufferPool {

  /**
   * Bytes per page, including header.
   */
  private static final int PAGE_SIZE = 4096;

  private static int pageSize = PAGE_SIZE;

  /**
   * Default number of pages passed to the constructor. This is used by other classes. BufferPool
   * should use the numPages argument to the constructor instead.
   */
  public static final int DEFAULT_PAGES = 50;

  /**
   * Number of pages of this buffer pool
   */
  private final int numPages;
  /**
   * A mapping which maps PageIds to pages
   */
  private final Map<PageId, Pair<Page, Long>> pages;
  /**
   * A timer used for implementing LRU
   */
  private long timer;
  /**
   * The lock manager
   */
  private final LockManager lockManager;

  /**
   * Creates a BufferPool that caches up to numPages pages.
   *
   * @param numPages maximum number of pages in this buffer pool.
   */
  public BufferPool(int numPages) {
    this.pages = new HashMap<>();
    this.numPages = numPages;
    this.timer = 0;
    this.lockManager = new LockManager();
  }

  public static int getPageSize() {
    return pageSize;
  }

  // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
  public static void setPageSize(int pageSize) {
    BufferPool.pageSize = pageSize;
  }

  // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
  public static void resetPageSize() {
    BufferPool.pageSize = PAGE_SIZE;
  }

  /**
   * Retrieve the specified page with the associated permissions. Will acquire a lock and may block
   * if that lock is held by another transaction.
   * <p>
   * The retrieved page should be looked up in the buffer pool.  If it is present, it should be
   * returned.  If it is not present, it should be added to the buffer pool and returned.  If there
   * is insufficient space in the buffer pool, an page should be evicted and the new page should be
   * added in its place.
   *
   * @param tid  the ID of the transaction requesting the page
   * @param pid  the ID of the requested page
   * @param perm the requested permissions on the page
   */
  public Page getPage(TransactionId tid, PageId pid, Permissions perm)
      throws TransactionAbortedException, DbException {
    // Step 0. Acquire a lock for the page, according to `perm`.
    lockManager.acquireLock(tid, pid, perm);
    // Step 1. Increase the timer. If the timer reaches the max value, reset it.
    ++timer;
    if (timer == Long.MAX_VALUE) {
      timer = 1;
      for (Pair<Page, Long> pair : pages.values()) {
        pair.second = 1L;
      }
    }
    // Step 2. Query the page from the buffer pool.
    Pair<Page, Long> pair = pages.get(pid);
    // Step 3. If the page exists in the buffer pool, set the accessed time and return the page
    // directly.
    if (pair != null) {
      pair.second = this.timer;
      return pair.first;
    }
    // Step 4. The page doesn't exist.
    // * If the buffer pool is full, we evict a page from buffer pool and read the new page in.
    // * If the buffer pool is not full, we directly read the page in.
    if (pages.size() == numPages) {
      this.evictPage();
    }
    assert pages.size() < numPages;
    Page page = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
    pages.put(pid, new Pair<>(page, this.timer));
    return page;
  }

  /**
   * Releases the lock on a page. Calling this is very risky, and may result in wrong behavior.
   * Think hard about who needs to call this and why, and why they can run the risk of calling it.
   *
   * @param tid the ID of the transaction requesting the unlock
   * @param pid the ID of the page to unlock
   */
  public void releasePage(TransactionId tid, PageId pid) {
    lockManager.releaseLock(tid, pid);
  }

  /**
   * Release all locks associated with a given transaction.
   *
   * @param tid the ID of the transaction requesting the unlock
   */
  public void transactionComplete(TransactionId tid) throws IOException {
    transactionComplete(tid, true);
  }

  /**
   * Return true if the specified transaction has a lock on the specified page
   */
  public boolean holdsLock(TransactionId tid, PageId pid) {
    return lockManager.holdsLock(tid, pid, Permissions.READ_ONLY);
  }

  /**
   * Commit or abort a given transaction; release all locks associated to the transaction.
   *
   * @param tid    the ID of the transaction requesting the unlock
   * @param commit a flag indicating whether we should commit or abort
   */
  public void transactionComplete(TransactionId tid, boolean commit)
      throws IOException {
    ArrayList<Pair<PageId, LockType>> lockedPages = new ArrayList<>(lockManager.getLockedPages(tid));
    for (Pair<PageId, LockType> pair :lockedPages) {
      if (pair.second == LockType.shared) {
        continue;
      }
      if (pages.containsKey(pair.first)) {
        if (commit) {
          flushPage(pair.first);
        } else {
          discardPage(pair.first);
        }
      }
    }
    for (Pair<PageId, LockType> pair : lockedPages) {
      releasePage(tid, pair.first);
    }
  }

  /**
   * Add a tuple to the specified table on behalf of transaction tid.  Will acquire a write lock on
   * the page the tuple is added to and any other pages that are updated (Lock acquisition is not
   * needed for lab2). May block if the lock(s) cannot be acquired.
   * <p>
   * Marks any pages that were dirtied by the operation as dirty by calling their markDirty bit, and
   * adds versions of any pages that have been dirtied to the cache (replacing any existing versions
   * of those pages) so that future requests see up-to-date pages.
   *
   * @param tid     the transaction adding the tuple
   * @param tableId the table to add the tuple to
   * @param t       the tuple to add
   */
  public void insertTuple(TransactionId tid, int tableId, Tuple t)
      throws DbException, IOException, TransactionAbortedException {
    DbFile file = Database.getCatalog().getDatabaseFile(tableId);
    if (file == null) {
      return;
    }
    ArrayList<Page> dirtyPages = file.insertTuple(tid, t);
    assert dirtyPages.size() <= numPages;
    for (Page dirtyPage : dirtyPages) {
      dirtyPage.markDirty(true, tid);
      Pair<Page, Long> pair = pages.get(dirtyPage.getId());
      if (pair != null) {
        pair.first = dirtyPage;
      } else {
        if (pages.size() == numPages) {
          this.evictPage();
        }
        pages.put(dirtyPage.getId(), new Pair<>(dirtyPage, ++this.timer));
      }
    }
  }

  /**
   * Remove the specified tuple from the buffer pool. Will acquire a write lock on the page the
   * tuple is removed from and any other pages that are updated. May block if the lock(s) cannot be
   * acquired.
   * <p>
   * Marks any pages that were dirtied by the operation as dirty by calling their markDirty bit, and
   * adds versions of any pages that have been dirtied to the cache (replacing any existing versions
   * of those pages) so that future requests see up-to-date pages.
   *
   * @param tid the transaction deleting the tuple.
   * @param t   the tuple to delete
   */
  public void deleteTuple(TransactionId tid, Tuple t)
      throws DbException, IOException, TransactionAbortedException {
    DbFile file = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
    if (file == null) {
      return;
    }
    ArrayList<Page> dirtyPages = file.deleteTuple(tid, t);
    assert dirtyPages.size() <= numPages;
    for (Page dirtyPage : dirtyPages) {
      dirtyPage.markDirty(true, tid);
      Pair<Page, Long> pair = pages.get(dirtyPage.getId());
      assert pair != null;
      pair.first = dirtyPage;
    }
  }

  /**
   * Flush all dirty pages to disk. NB: Be careful using this routine -- it writes dirty data to
   * disk so will break simpledb if running in NO STEAL mode.
   */
  public synchronized void flushAllPages() throws IOException {
    for (PageId pid : pages.keySet()) {
      flushPage(pid);
    }
  }

  /**
   * Remove the specific page id from the buffer pool. Needed by the recovery manager to ensure that
   * the buffer pool doesn't keep a rolled back page in its cache.
   * <p>
   * Also used by B+ tree files to ensure that deleted pages are removed from the cache so they can
   * be reused safely
   */
  public synchronized void discardPage(PageId pid) {
    pages.remove(pid);
  }

  /**
   * Flushes a certain page to disk
   *
   * @param pid an ID indicating the page to flush
   */
  private synchronized void flushPage(PageId pid) throws IOException {
    Pair<Page, Long> pair = pages.get(pid);
    assert pair != null;
    Page page = pair.first;
    // If the page is not dirty, just return.
    if (page.isDirty() == null) {
      return;
    }
//    System.out.println("T" + page.isDirty().getId() + " flushed page " + pid);
    // Write the page back to file, and mark it as clean.
    DbFile file = Database.getCatalog().getDatabaseFile(page.getId().getTableId());
    file.writePage(page);
    page.markDirty(false, null);
  }

  /**
   * Write all pages of the specified transaction to disk.
   */
  public synchronized void flushPages(TransactionId tid) throws IOException {
    for (Entry<PageId, Pair<Page, Long>> e : pages.entrySet()) {
      if (e.getValue().first.isDirty() == tid) {
        flushPage(e.getKey());
      }
    }
  }

  /**
   * Discards a page from the buffer pool. Flushes the page to disk to ensure dirty pages are
   * updated on disk.
   */
  private synchronized void evictPage() throws DbException {
    // Step 0. Collect the candidate pages for eviction.
    Map<PageId, Pair<Page, Long>> pageCandidates = new HashMap<>();
    for (Entry<PageId, Pair<Page, Long>> e : pages.entrySet()) {
      if (e.getValue().first.isDirty() == null) {
        pageCandidates.put(e.getKey(), e.getValue());
      }
    }
    if (pageCandidates.isEmpty()) {
      throw new DbException("No candidate page for eviction.");
    }
    // Step 1. Find the page with minimum accessed time.
    Entry<PageId, Pair<Page, Long>> evictEntry = null;
    long minTime = Long.MAX_VALUE;
    for (Entry<PageId, Pair<Page, Long>> e : pageCandidates.entrySet()) {
      if (e.getValue().second < minTime) {
        minTime = e.getValue().second;
        evictEntry = e;
      }
    }
    // Step 2. Flush the selected page.
    try {
      flushPage(evictEntry.getKey());
    } catch (IOException e) {
      e.printStackTrace();
      throw new DbException("");
    }
    // Step 3. Remove the page from the page map.
    boolean removeRes = pages.remove(evictEntry.getKey(), evictEntry.getValue());
    assert removeRes;
  }

}
