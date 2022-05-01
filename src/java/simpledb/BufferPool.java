package simpledb;

import java.io.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;

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
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;


    private final ConcurrentHashMap<PageId, BufferedPageInfo> bufferPool;
    private final List<PageId> LRUList;
    private final int maxSize;

    /**
     * A help class used to represent a search key for a page.
     */
    public static class Key implements Serializable {

        private static final long serialVersionUID = 1L;

        private final TransactionId tid;
        private final PageId pid;

        public Key(TransactionId tid, PageId pid) {
            this.tid = tid;
            this.pid = pid;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Key key = (Key) o;
            return Objects.equals(tid, key.tid) && Objects.equals(pid, key.pid);
        }

        @Override
        public int hashCode() {
            return Objects.hash(tid, pid);
        }
    }

    /**
     * A help class used to represent a BufferPool page with lock.
     */
    public static class BufferedPageInfo implements Serializable {

        private static final long serialVersionUID = 1L;

        private final Page page;
        private final Lock lock;

        public BufferedPageInfo(Page page, Lock lock) {
            this.page = page;
            this.lock = lock;
        }

        public Page getPage() {
            return page;
        }

        public Lock getLock() {
            return lock;
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
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // TODO: maybe we should use a lock and the given permission here

        // make sure the parameters are valid
        assert tid != null && pid != null && perm != null;

        // if our buffer pool is full, we need to evict a page
        if (bufferPool.size() + 1 >= maxSize) {
            evictPage();
        }

        BufferedPageInfo bufferedPageInfo = bufferPool.get(pid);
        Page page = null;
        if (bufferedPageInfo == null) {
            // we need to read the page from disk
            page = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
            bufferPool.put(pid, new BufferedPageInfo(page, null /* may be a lock */));
            // add the new page to the LRU list head
            LRUList.add(0, pid);
        } else {
            // the page is already in the buffer pool
            page = bufferedPageInfo.getPage();
            // adjust the LRU list
            LRUList.remove(pid);
            LRUList.add(0, pid);
        }
        return page;
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
    public void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> dirtyPages = dbFile.insertTuple(tid, t);
        for (Page page : dirtyPages) {
            page.markDirty(true, tid);
            bufferPool.put(page.getId(), new BufferedPageInfo(page, null));
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        int tableId = t.getRecordId().getPageId().getTableId();
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> dirtyPages = dbFile.deleteTuple(tid, t);
        for (Page page : dirtyPages) {
            page.markDirty(true, tid);
            bufferPool.put(page.getId(), new BufferedPageInfo(page, null));
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // duplicate the PageId set because we will be modifying it
        Set<PageId> ids = new HashSet<PageId>(bufferPool.keySet());
        for (PageId id : ids) {
            flushPage(id);
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        if (!bufferPool.containsKey(pid)) {
            return;
        }
        BufferedPageInfo bufferedPageInfo = bufferPool.get(pid);
        Page page = bufferedPageInfo.getPage();
        bufferPool.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        assert bufferPool.containsKey(pid);
        BufferedPageInfo bufferedPageInfo = bufferPool.get(pid);
        Page page = bufferedPageInfo.getPage();
        // remove the cached page from the buffer pool
        bufferPool.remove(pid);
        // if the page is clean, no need to flush
        if (page.isDirty() == null) {
            return;
        }
        // otherwise, write the page to disk, and mark it clean
        DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
        dbFile.writePage(page);
        page.markDirty(false, null);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        try {
            PageId theLastPage = LRUList.get(LRUList.size() - 1);
            LRUList.remove(theLastPage);
            flushPage(theLastPage);
        } catch (IOException e) {
            throw new DbException("IOException in evictPage()");
        }
    }

}
