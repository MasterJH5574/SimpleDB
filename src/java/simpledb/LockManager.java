package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.Queue;
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

  private static class DependencyGraph {
    private final ConcurrentHashMap<TransactionId, HashSet<TransactionId>> edgesOut;
    private final ConcurrentHashMap<TransactionId, HashSet<TransactionId>> edgesIn;

    public DependencyGraph() {
      edgesOut = new ConcurrentHashMap<>();
      edgesIn = new ConcurrentHashMap<>();
    }

    private boolean isTheOldestOnACycle(TransactionId tid) {
      // Detect loops via topological sorting
      HashMap<TransactionId, Integer> degreesIn = new HashMap<>();
      Queue<TransactionId> queue = new LinkedList<>();
      for (Entry<TransactionId, HashSet<TransactionId>> e : edgesIn.entrySet()) {
        degreesIn.put(e.getKey(), e.getValue().size());
        if (e.getValue().size() == 0) {
          queue.offer(e.getKey());
        }
      }

      int visited = 0;
      while (!queue.isEmpty()) {
        TransactionId cur = queue.poll();
        ++visited;
        if (edgesOut.get(cur) != null) {
          for (TransactionId to : edgesOut.get(cur)) {
            int newDeg = degreesIn.get(to) - 1;
            if (newDeg == 0) {
              queue.offer(to);
            }
            degreesIn.replace(to, newDeg);
          }
        }
      }
      if (visited == degreesIn.size()) {
        return false;
      }

      TransactionId maxTid = null;
      for (Entry<TransactionId, Integer> e : degreesIn.entrySet()) {
        if (e.getValue() != 0) {
          if (maxTid == null) {
            maxTid = e.getKey();
          } else if (e.getKey().getId() > maxTid.getId()) {
            maxTid = e.getKey();
          }
        }
      }
      return maxTid == tid;
    }

    public synchronized void addEdge(TransactionId from, TransactionId to)
        throws TransactionAbortedException {
      if (from == to) {
        return;
      }
      edgesOut.putIfAbsent(from, new HashSet<>());
      edgesIn.putIfAbsent(to, new HashSet<>());
      edgesOut.get(from).add(to);
      edgesIn.get(to).add(from);
      if (isTheOldestOnACycle(from)) {
        throw new TransactionAbortedException();
      }
    }

    public synchronized void removeInEdges(TransactionId to) {
      if (edgesIn.get(to) == null) {
        return;
      }
      for (TransactionId from : edgesIn.get(to)) {
        assert edgesOut.get(from) != null;
        assert edgesOut.get(from).contains(to);
        edgesOut.get(from).remove(to);
      }
      edgesIn.get(to).clear();
    }
  }

  private final ConcurrentHashMap<TransactionId, ArrayList<PageId>> tid2LockedPages;
  private final ConcurrentHashMap<PageId, Lock> page2Lock;
  private final DependencyGraph depGraph;

  public LockManager() {
    tid2LockedPages = new ConcurrentHashMap<>();
    page2Lock = new ConcurrentHashMap<>();
    depGraph = new DependencyGraph();
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

  private synchronized Lock getLock(PageId pid) {
    if (page2Lock.get(pid) != null) {
      return page2Lock.get(pid);
    }
    Lock lock = new Lock();
    page2Lock.put(pid, lock);
    return lock;
  }

  private void addLock(TransactionId tid, PageId pid) {
    tid2LockedPages.putIfAbsent(tid, new ArrayList<>());
    ArrayList<PageId> pageIds = tid2LockedPages.get(tid);
    assert !pageIds.contains(pid);
    pageIds.add(pid);
  }

  public void acquireLock(TransactionId tid, PageId pid, Permissions perm)
      throws TransactionAbortedException {
    if (holdsLock(tid, pid, perm)) {
      return;
    }
    if (perm == Permissions.READ_ONLY) {
      acquireSLock(tid, pid);
    } else {
      acquireXLock(tid, pid);
    }
  }

  private void acquireSLock(TransactionId tid, PageId pid) throws TransactionAbortedException {
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
          } else {
            depGraph.addEdge(tid, lock.holders.get(0));
          }
        }
      }
    }
  }

  private void acquireXLock(TransactionId tid, PageId pid) throws TransactionAbortedException {
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
          for (TransactionId holder : lock.holders) {
            depGraph.addEdge(tid, holder);
          }
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
      depGraph.removeInEdges(tid);
    }
  }

  public ArrayList<PageId> getLockedPages(TransactionId tid) {
    ArrayList<PageId> lockedPages = tid2LockedPages.get(tid);
    return lockedPages == null ? new ArrayList<>() : lockedPages;
  }
}
