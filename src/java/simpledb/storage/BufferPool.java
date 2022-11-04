package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {

    private static class LockManager {
        public Map<PageId, Set<TransactionId>> sLockTable;
        public Map<PageId, TransactionId> xLockTable;

        public LockManager() {
            sLockTable = new HashMap<>();
            xLockTable = new HashMap<>();
        }
    }

    /**
     * Bytes per page, including header.
     */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;

    /**
     * Default timeout in milliseconds when trying to acquire a lock
     */
    public static final int TIME_OUT = 100;

    /**
     * Default retry interval for acquiring a lock
     */
    public static final int RETRY_INTERVAL = 10;

    private final Page[] pages;
    private final Map<PageId, Integer> pgId2Idx;
    private final List<Integer> nullPageIdxes;

    private final Object lk;    // lock for BufferPool

    private final LockManager lockMgr;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        pages = new Page[numPages];
        pgId2Idx = new HashMap<>();
        nullPageIdxes = new LinkedList<>();

        for (int i = 0; i < pages.length; i++) {
            nullPageIdxes.add(i);
        }

        lk = new Object();

        lockMgr = new LockManager();
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
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {

        // try to acquire the lock within timeout
        long startTime = System.currentTimeMillis();
        boolean acquired = false;

        while (System.currentTimeMillis() - startTime < TIME_OUT) {
            // Try to acquire the needed lock within TIME_OUT

            synchronized (lockMgr) {
                TransactionId xTid = lockMgr.xLockTable.get(pid);
                Set<TransactionId> sTidSet = lockMgr.sLockTable.get(pid);

                switch (perm) {
                    case READ_ONLY:
                        if (xTid != null) {
                            if (tid.equals(xTid)) {
                                acquired = true;
                            }
                        } else {
                            // add this tx to sLockTable
                            if (sTidSet == null) {
                                lockMgr.sLockTable.put(pid, new HashSet<>());
                                lockMgr.sLockTable.get(pid).add(tid);
                            } else {
                                lockMgr.sLockTable.get(pid).add(tid);
                            }
                            acquired = true;
                        }
                        break;
                    case READ_WRITE:
                        if (xTid != null) {
                            if (tid.equals(xTid)) {
                                acquired = true;
                            }
                        } else {
                            if (sTidSet == null) {
                                // if there is no tx holding sLock on pid
                                lockMgr.xLockTable.put(pid, tid);
                                acquired = true;
                            } else {
                                if (sTidSet.size() == 1 && sTidSet.contains(tid)) {
                                    // if tx is the only one holding sLock on pid
                                    lockMgr.sLockTable.remove(pid);
                                    lockMgr.xLockTable.put(pid, tid);
                                    acquired = true;
                                }
                            }
                        }
                        break;
                    default:
                        throw new DbException("BufferPool::getPage: Unexpected permission");
                }
            }

            if (acquired) break;

            try {
                Thread.sleep(RETRY_INTERVAL);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        // if the required lock not acquired with TIME_OUT
        if (!acquired)
            throw new TransactionAbortedException();

        synchronized (lk) {
            if (pgId2Idx.containsKey(pid)) {
                return pages[pgId2Idx.get(pid)];
            }

            if (nullPageIdxes.isEmpty()) {
                // if there is no null page, evict one
                evictPage();
            }

            int idx = nullPageIdxes.remove(0);
            pages[idx] = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
            pgId2Idx.put(pages[idx].getId(), idx);

            return pages[idx];
        }
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        synchronized (lockMgr) {
            TransactionId xTid = lockMgr.xLockTable.get(pid);
            Set<TransactionId> sTidSet = lockMgr.sLockTable.get(pid);

            if (xTid != null) {
                if (xTid.equals(tid))
                    // if tid is holding xLock on pid
                    lockMgr.xLockTable.remove(pid);
            } else {
                // no tx is holding xLock on pid
                if (!sTidSet.isEmpty() && sTidSet.contains(tid)) {
                    // tid is holding sLock on pid
                    lockMgr.sLockTable.get(pid).remove(tid);
                    if (lockMgr.sLockTable.get(pid).isEmpty()) {
                        lockMgr.sLockTable.remove(pid);
                    }
                }
            }
        }
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // TODO: some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        synchronized (lockMgr) {
            TransactionId xTid = lockMgr.xLockTable.get(p);
            Set<TransactionId> sTidSet = lockMgr.sLockTable.get(p);

            if (xTid != null) {
                return xTid.equals(tid);
            } else {
                return !sTidSet.isEmpty() && sTidSet.contains(tid);
            }
        }
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // TODO: some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        List<Page> pages = Database.getCatalog().getDatabaseFile(tableId).insertTuple(tid, t);
        updatePage(pages, tid);
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        List<Page> pages = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId()).deleteTuple(tid, t);
        updatePage(pages, tid);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for (Page page : pages) {
            if (page == null) continue;
            flushPage(page.getId());
        }
    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     * <p>
     * Also used by B+ tree files to ensure that deleted pages
     * are removed from the cache so they can be reused safely
     */
    public synchronized void removePage(PageId pid) {
        int idx = pgId2Idx.getOrDefault(pid, -1);

        if (idx == -1) {
            System.out.println("BufferPool::removePage: page not in BufferPool");
            return;
        }

        pgId2Idx.remove(pid);
        nullPageIdxes.add(idx);
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        int idx = pgId2Idx.getOrDefault(pid, -1);

        if (idx == -1) {
            System.out.println("BufferPool::flushPage: page not in BufferPool");
            return;
        }

        TransactionId dirtier = pages[idx].isDirty();
        if (dirtier == null) {
            return;
        }

        DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
        dbFile.writePage(pages[idx]);
        pages[idx].markDirty(false, dirtier);
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // TODO: some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // When evictPage is called, it is guaranteed that all the pages are not null
        // randomly pick an index in [0, pages.length) to evict
        int randomIdx = ThreadLocalRandom.current().nextInt() & Integer.MAX_VALUE % pages.length;

        try {
            PageId pid = pages[randomIdx].getId();
            flushPage(pid);
            removePage(pid);
        } catch (IOException ioe) {
            throw new DbException("BufferPool::evictPage: fail to write dirty page to disk");
        }
    }

    // Fixme: temporary implementation
    private synchronized void updatePage(List<Page> pages, TransactionId tid)
            throws TransactionAbortedException, DbException {
        for (Page p : pages) {
            // get the page and overwrite it with the dirty one
            getPage(tid, p.getId(), null);
            p.markDirty(true, tid);

            int idx = pgId2Idx.get(p.getId());
            this.pages[idx] = p;
        }
    }
}
