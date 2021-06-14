package simpledb;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {
  public enum LockType {shared, exclusive}

  private static class Lock {
    private LockType type;
    private final ArrayList<TransactionId> holders;

    public Lock() {
      this.type = LockType.shared;
      this.holders = new ArrayList<>();
    }
  }

  private final ConcurrentHashMap<TransactionId, ArrayList<PageId>> tid2LockedPages;
  private final ConcurrentHashMap<PageId, Lock> page2Lock;

  public LockManager() {
    tid2LockedPages = new ConcurrentHashMap<>();
    page2Lock = new ConcurrentHashMap<>();
  }

  public boolean holdsLock(TransactionId tid, PageId pid, Permissions perm) {
    if (page2Lock.get(pid) == null) {
      assert tid2LockedPages.get(tid) == null || !tid2LockedPages.get(tid).contains(pid);
      return false;
    }
    Lock lock = page2Lock.get(pid);
    synchronized (lock) {
      assert lock != null;

      if (!lock.holders.contains(tid)) {
        assert tid2LockedPages.get(tid) == null || !tid2LockedPages.get(tid).contains(pid);
        return false;
      }
      boolean res = perm == Permissions.READ_ONLY || lock.type == LockType.exclusive;
      if (res) {
        assert tid2LockedPages.get(tid) != null && tid2LockedPages.get(tid).contains(pid);
      } else {
        assert tid2LockedPages.get(tid) == null || !tid2LockedPages.get(tid).contains(pid) || (
            perm == Permissions.READ_WRITE && lock.type == LockType.shared);
      }
      return res;
    }
  }

  private Lock getLock(PageId pid) {
    if (page2Lock.get(pid) != null) {
      return page2Lock.get(pid);
    }
    Lock lock = new Lock();
    synchronized (lock) {
      page2Lock.put(pid, lock);
      return lock;
    }
  }

  private void addLock(TransactionId tid, PageId pid) {
    tid2LockedPages.putIfAbsent(tid, new ArrayList<>());
    ArrayList<PageId> pageIds = tid2LockedPages.get(tid);
    assert !pageIds.contains(pid);
    pageIds.add(pid);
  }

  public void acquireLock(TransactionId tid, PageId pid, Permissions perm) {
    if (holdsLock(tid, pid, perm)) {
      return;
    }
    if (perm == Permissions.READ_ONLY) {
      acquireSLock(tid, pid);
    } else {
      acquireXLock(tid, pid);
    }
  }

  private void acquireSLock(TransactionId tid, PageId pid) {
    Lock lock = getLock(pid);
    while (true) {
      synchronized (lock) {
        if (lock.holders.isEmpty()) {
          lock.type = LockType.shared;
          lock.holders.add(tid);
          addLock(tid, pid);
          break;
        } else if (lock.type == LockType.shared) {
          lock.holders.add(tid);
          addLock(tid, pid);
          break;
        } else {
          assert lock.type == LockType.exclusive;
          assert lock.holders.size() == 1;
          if (lock.holders.get(0) == tid) {
            break;
          }
        }
      }
    }
  }

  private void acquireXLock(TransactionId tid, PageId pid) {
    Lock lock = getLock(pid);
    while (true) {
      synchronized (lock) {
        if (lock.holders.isEmpty()) {
          lock.type = LockType.exclusive;
          lock.holders.add(tid);
          addLock(tid, pid);
          break;
        } else if (lock.holders.size() == 1 && lock.holders.get(0) == tid) {
          lock.type = LockType.exclusive;
          break;
        } else {
          assert lock.holders.size() == 1 || lock.type == LockType.shared;
        }
      }
    }
  }

  public void releaseLock(TransactionId tid, PageId pid) {
    assert holdsLock(tid, pid, Permissions.READ_ONLY);
    Lock lock = getLock(pid);
    assert lock.holders.contains(tid);

    synchronized (lock) {
      ArrayList<PageId> heldLocks = tid2LockedPages.get(tid);
      assert heldLocks != null && heldLocks.contains(pid);
      heldLocks.remove(pid);
      lock.holders.remove(tid);
    }
  }

  public ArrayList<PageId> getLockedPages(TransactionId tid) {
    return tid2LockedPages.get(tid);
  }
}
