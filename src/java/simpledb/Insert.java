package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private final TransactionId tid;
    private OpIterator child;
    private final int tableid;
    private int insertionCount;
    private static final TupleDesc tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
    private boolean isCalled = false;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        this.tid = t;
        this.child = child;
        this.tableid = tableId;
    }

    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        child.open();
        insertionCount = 0;
    }

    public void close() {
        child.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (isCalled) {
            return null;
        }
        isCalled = true;

        Tuple result = new Tuple(tupleDesc);
        result.setField(0, new IntField(0));
        while (child.hasNext()) {
            try {
                Tuple tuple = child.next();
                Database.getBufferPool().insertTuple(tid, tableid, tuple);
                result.setField(0, new IntField(++insertionCount));
            } catch (IOException e) {
                throw new DbException("Insert failed");
            }
        }
        return result;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        assert children.length == 1;
        child = children[0];
    }
}
