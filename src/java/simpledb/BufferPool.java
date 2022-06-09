package simpledb;

import java.io.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

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


    private final ConcurrentHashMap<PageId, PageManager> bufferPool;
    private final LinkedList<PageId> LRUList;
    private final int maxSize;

    public static class WaitingItem implements Serializable {

        private static final long serialVersionUID = 1L;

        private final TransactionId tid;

        private final boolean isWriteLock;


        public WaitingItem(TransactionId tid, boolean isWriteLock) {
            this.tid = tid;
            this.isWriteLock = isWriteLock;
        }

        public TransactionId getTid() {
            return tid;
        }

        public boolean isWriteLock() {
            return isWriteLock;
        }
    }

    /**
     * A help class used to represent a BufferPool page with lock.
     */
    public static class PageManager implements Serializable {

        private static final long serialVersionUID = 1L;

        private final Page page;
        private final List<TransactionLock> locks = new ArrayList<>();
        private final List<WaitingItem> waitingItems = new ArrayList<>();

        public PageManager(Page page) {
            this.page = page;
        }

        private void acquireNewLock(TransactionId tid, boolean isWriteLock) throws TransactionAbortedException {

            // determine whether the new lock is conflict with other locks on this page
            boolean conflict = false;
            if (isWriteLock) {
                conflict = !locks.isEmpty();
            } else {
                for (TransactionLock lock : locks) {
                    if (lock.isWriteLock()) {
                        conflict = true;
                        break;
                    }
                }
            }

            // good, there's no conflict
            if (!conflict) {
                locks.add(new TransactionLock(tid, isWriteLock));
                return;
            }

            // oops, there has conflict, so we should wait on this page
            WaitingItem waitingItem = new WaitingItem(tid, isWriteLock);
            waitingItems.add(waitingItem);
            // very bad synchronized, but it works :)
            synchronized(waitingItem) {
                try {
                    long timeout = new Random().nextInt(3000) + 1000;
                    long begin = System.currentTimeMillis();
                    waitingItem.wait(timeout);
                    long end = System.currentTimeMillis();
                    if (end - begin >= timeout * 0.9) {
                        throw new TransactionAbortedException();
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            // try again
            acquireNewLock(tid, isWriteLock);
        }

        private void releaseLock(TransactionId tid) {
            // remove the first transaction who
            for (int i = 0; i < locks.size(); i++) {
                if (locks.get(i).getTid().equals(tid)) {
                    locks.remove(i);
                    break;
                }
            }
            notifyWaitingThread();
        }

        private void notifyWaitingThread() {
            // if there's no waiting item, just return
            if (waitingItems.isEmpty()) {
                return;
            }

            // satisfy write lock if exists
            for (int i = 0; i < waitingItems.size(); i++) {
                WaitingItem waitingItem = waitingItems.get(i);
                if(waitingItem.isWriteLock()) {
                    waitingItems.remove(i);
                    synchronized (waitingItem) {
                        waitingItem.notify();
                    }
                    return;
                }
            }

            // if there's no write locks, then using FIFO policy
            WaitingItem waitingItem = waitingItems.remove(0);
            waitingItem.notify();
        }

        private void upgradeLock(TransactionLock lockToUpgrade) throws TransactionAbortedException {

            // release previous lock
            releaseLock(lockToUpgrade.getTid());

            // this time, we should acquire write lock
            acquireNewLock(lockToUpgrade.getTid(), true);
        }

        public synchronized Page getPage(TransactionId tid, Permissions perm) throws TransactionAbortedException {
            // get the lock's acquire type
            boolean isWriteLock = (perm.equals(Permissions.READ_WRITE));

            // try to find a lock whose tid is equal to the given tid
            TransactionLock transactionLock = null;
            for (TransactionLock lock : locks) {
                if (lock.getTid().equals(tid)) {
                    transactionLock = lock;
                    break;
                }
            }

            // if this is a new transaction want to access this page
            if (transactionLock == null) {
                acquireNewLock(tid, isWriteLock);
                return page;
            }

            // otherwise, check if the transaction want to upgrade its lock
            if (!transactionLock.isWriteLock() && isWriteLock) {
                upgradeLock(transactionLock);
                return page;
            }

            // it is recursively calling getPage, just give it the page
            return page;
        }

        public synchronized void releasePage(TransactionId tid) {
            releaseLock(tid);
        }

        public synchronized boolean holdsLock(TransactionId tid) {
            for (TransactionLock lock : locks) {
                if (lock.getTid().equals(tid)) {
                    return true;
                }
            }
            return false;
        }

        public synchronized Page getRawPage() {
            return page;
        }

        public synchronized boolean idle() {
            return locks.isEmpty();
        }
    }

    public static class TransactionLock {
        private final TransactionId tid;
        private final boolean isWriteLock;

        public TransactionLock(TransactionId tid, boolean isWriteLock) {
            this.tid = tid;
            this.isWriteLock = isWriteLock;
        }

        public TransactionId getTid() {
            return tid;
        }

        public boolean isWriteLock() {
            return isWriteLock;
        }
    }


    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        maxSize = numPages;
        bufferPool = new ConcurrentHashMap<>(numPages);
        LRUList = new LinkedList<>();
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
    public synchronized Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {

        // make sure the parameters are valid
        assert tid != null && pid != null && perm != null;

        // if our buffer pool is full, we need to evict a page
        if (bufferPool.size() >= maxSize) {
            evictPage();
        }

        PageManager pageManager = bufferPool.get(pid);
        if (pageManager == null) {
            // we need to read the page from disk
            pageManager = new PageManager(Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid));
            bufferPool.put(pid, pageManager);
            // add the new page to the LRU list head
            LRUList.add(0, pid);
        } else {
            // adjust the LRU list
            LRUList.remove(pid);
            LRUList.add(0, pid);
        }
        return pageManager.getPage(tid, perm);
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
    public synchronized void releasePage(TransactionId tid, PageId pid) {
        PageManager pageManager = bufferPool.get(pid);
        assert pageManager != null;
        pageManager.releasePage(tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        transactionComplete(tid ,true);
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        PageManager pageManager = bufferPool.get(p);
        if (pageManager == null) {
            return true;
        }
        return pageManager.holdsLock(tid);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
            throws IOException {
        if (commit) {
            flushPages(tid);
        } else {
            discardPages(tid);
        }
        for (PageId pid : bufferPool.keySet()) {
            if (holdsLock(tid, pid)) {
                releasePage(tid, pid);
            }
        }
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
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> dirtyPages = dbFile.insertTuple(tid, t);
        for (Page page : dirtyPages) {
            page.markDirty(true, tid);
            PageManager pageManager = new PageManager(page);
            bufferPool.put(page.getId(), pageManager);
            pageManager.getPage(tid, Permissions.READ_WRITE);
        }
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
        int tableId = t.getRecordId().getPageId().getTableId();
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> dirtyPages = dbFile.deleteTuple(tid, t);
        for (Page page : dirtyPages) {
            page.markDirty(true, tid);
            PageManager pageManager = new PageManager(page);
            bufferPool.put(page.getId(), pageManager);
            pageManager.getPage(tid, Permissions.READ_WRITE);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // duplicate the PageId set because we will be modifying it
        Set<PageId> ids = new HashSet<PageId>(bufferPool.keySet());
        for (PageId id : ids) {
            flushPage(id);
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
    public synchronized void discardPage(PageId pid) {
        assert bufferPool.containsKey(pid);
        bufferPool.remove(pid);
        LRUList.remove(pid);
    }

    public synchronized void discardPages(TransactionId tid) {
        Set<PageId> pids = new HashSet<PageId>(bufferPool.keySet());
        for (PageId pid : pids) {
            if (holdsLock(tid, pid)) {
                discardPage(pid);
            }
        }
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        assert bufferPool.containsKey(pid);
        // get page
        PageManager pageManager = bufferPool.get(pid);
        Page page = pageManager.getRawPage();
        // remove the cached page from the buffer pool
        bufferPool.remove(pid);
        LRUList.remove(pid);
        // if the page is clean, no need to flush
        if (page.isDirty() == null) {
            return;
        }
        // otherwise, write the page to disk, and mark it clean
        DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
        dbFile.writePage(page);
        page.markDirty(false, null);
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        Set<PageId> pids = new HashSet<PageId>(bufferPool.keySet());
        for (PageId pid : pids) {
            if (holdsLock(tid, pid)) {
                flushPage(pid);
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        try {
            Iterator<PageId> iterator = LRUList.descendingIterator();
            while(iterator.hasNext()) {
                PageId probePageId = iterator.next();
                PageManager pageManager = bufferPool.get(probePageId);
                if (pageManager.getRawPage().isDirty() == null) {
                    flushPage(probePageId);
                    return;
                }
            }
            throw new DbException("all pages in buffer pool is dirty");

        } catch (IOException e) {
            throw new DbException("IOException in evictPage()");
        }
    }

}
