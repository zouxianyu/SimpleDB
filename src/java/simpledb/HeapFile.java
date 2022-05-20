package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private final File file;
    private final TupleDesc td;
    private RandomAccessFile raf;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.td = td;
        try {
            // TODO: determine whether the mode should be "r" or "rw"
            raf = new RandomAccessFile(file, "rw");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return file.getAbsoluteFile().hashCode();
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
        int pageSize = BufferPool.getPageSize();
        int pos = pid.getPageNumber() * pageSize;
        if(pos >= file.length()){
            throw new IllegalArgumentException("Page number is out of bound");
        }
        try {
            raf.seek((long) pid.getPageNumber() * pageSize);
            byte[] data = new byte[pageSize];
            raf.read(data, 0, pageSize);
            return new HeapPage(new HeapPageId(pid.getTableId(),pid.getPageNumber()), data);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        int pageSize = BufferPool.getPageSize();
        long pos = (long) page.getId().getPageNumber() * pageSize;
        try {
            raf.seek(pos);
            raf.write(page.getPageData());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) ((file.length() - 1) / BufferPool.getPageSize() + 1);
    }

    private void createNewPage() throws IOException {
        raf.seek(file.length());
        byte[] data = new byte[BufferPool.getPageSize()];
        raf.write(data);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        //lookup each page in the file
        //if the page is full, create a new page
        HeapPageId pid = new HeapPageId(getId(), numPages() - 1);
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        if (page.getNumEmptySlots() == 0) {
            pid = new HeapPageId(getId(), numPages());
            createNewPage();
            page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        }
        page.insertTuple(t);
        return new ArrayList<>(Collections.singletonList(page));
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        PageId pageId = t.getRecordId().getPageId();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
        page.deleteTuple(t);
        return new ArrayList<>(Collections.singletonList(page));
    }

    class HeapFileIterator extends AbstractDbFileIterator {
        private final TransactionId tid;
        private int pageNumber = 0;
        private Iterator<Tuple> tupleIterator = null;

        private boolean openFailed = false;
        public boolean isOpenFailed() {
            return openFailed;
        }

        public HeapFileIterator(TransactionId tid){
            this.tid = tid;
            try {
                open();
            }catch (DbException | TransactionAbortedException e) {
                openFailed = true;
                e.printStackTrace();
            }
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            //get the first page using BufferPool
            HeapPage curPage = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), pageNumber), Permissions.READ_ONLY);
            tupleIterator = curPage.iterator();
        }

        @Override
        public void close(){
            super.close();
            pageNumber = 0;
            tupleIterator = null;
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        protected Tuple readNext() throws DbException, TransactionAbortedException {
            // not open
            if(tupleIterator == null){
                return null;
            }
            // if there is no more tuples, get the next page
            if(!tupleIterator.hasNext()) {
                pageNumber++;
                // if there is no more pages, return null
                if(pageNumber >= numPages()){
                    return null;
                }
                // TODO: maybe we should use another permission here
                HeapPage curPage = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), pageNumber), Permissions.READ_ONLY);
                tupleIterator = curPage.iterator();
            }
            return tupleIterator.next();
        }
    };


    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        HeapFileIterator it = new HeapFileIterator(tid);
        return it.isOpenFailed() ? null : it;
    }
}

