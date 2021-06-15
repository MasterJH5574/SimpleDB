package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import simpledb.utils.Pair;

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
    private final HashMap<TransactionId, ArrayList<TransactionId>> edgesOut;

    public DependencyGraph() {
      edgesOut = new HashMap<>();
    }

    private void dfs(TransactionId tid, TransactionId start, HashSet<TransactionId> visited)
        throws TransactionAbortedException {
      ArrayList<TransactionId> tos = edgesOut.get(tid);
      if (tos == null) {
        return;
      }
      for (TransactionId to : tos) {
        if (to == start) {
          throw new TransactionAbortedException();
        }
        if (visited.contains(to)) {
          continue;
        }
        visited.add(to);
        dfs(to, start, visited);
      }
    }

    private void findCycle(TransactionId tid) throws TransactionAbortedException {
      HashSet<TransactionId> visited = new HashSet<>();
      visited.add(tid);
      dfs(tid, tid, visited);
    }

    public void setEdges(TransactionId from, ArrayList<TransactionId> tos)
        throws TransactionAbortedException {
      edgesOut.putIfAbsent(from, new ArrayList<>());
      ArrayList<TransactionId> edges = edgesOut.get(from);
      ArrayList<TransactionId> oldEdges = new ArrayList<>(edges);
      oldEdges.removeAll(tos);
      for (TransactionId oldTo : oldEdges) {
        edges.remove(oldTo);
      }
      boolean changed = false;
      for (TransactionId to : tos) {
        if (from == to) {
          continue;
        }
        if (edges.contains(to)) {
          continue;
        }
        edges.add(to);
        changed = true;
      }
      if (changed) {
        findCycle(from);
      }
    }

    public void removeOutEdges(TransactionId from) {
      ArrayList<TransactionId> tos = edgesOut.get(from);
      if (tos != null) {
        tos.clear();
      }
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
    assert lock != null;

    synchronized (lock) {
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
          assert lock.holders.get(0) != tid;
          synchronized (depGraph) {
            depGraph.setEdges(tid, new ArrayList<TransactionId>() {{
              add(lock.holders.get(0));
            }});
          }
        }
      }
    }
    synchronized (depGraph) {
      depGraph.removeOutEdges(tid);
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
          synchronized (depGraph) {
            depGraph.setEdges(tid, lock.holders);
          }
        }
      }
    }
    synchronized (depGraph) {
      depGraph.removeOutEdges(tid);
    }
  }

  public void releaseLock(TransactionId tid, PageId pid) {
    assert holdsLock(tid, pid, Permissions.READ_ONLY);
    Lock lock = getLock(pid);

    synchronized (lock) {
      assert lock.holders.contains(tid);
      ArrayList<PageId> heldLocks = tid2LockedPages.get(tid);
      assert heldLocks != null && heldLocks.contains(pid);
      heldLocks.remove(pid);
      lock.holders.remove(tid);
    }
  }

  public ArrayList<Pair<PageId, LockType>> getLockedPages(TransactionId tid) {
    ArrayList<PageId> lockedPages = tid2LockedPages.get(tid);
    ArrayList<Pair<PageId, LockType>> res = new ArrayList<>();
    if (lockedPages == null) {
      return res;
    }
    for (PageId pid : lockedPages) {
      res.add(new Pair<>(pid, page2Lock.get(pid).type));
    }
    return res;
  }
}
