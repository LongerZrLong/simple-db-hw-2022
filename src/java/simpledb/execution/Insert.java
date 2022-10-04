package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Utility;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import javax.xml.crypto.Data;
import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private final TransactionId tid;
    private OpIterator child;
    private final int tableId;

    private boolean done;

    /**
     * Constructor.
     *
     * @param t       The transaction running the insert.
     * @param child   The child operator from which to read tuples to be inserted.
     * @param tableId The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to
     *                     insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        this.tid = t;
        this.child = child;
        this.tableId = tableId;
    }

    public TupleDesc getTupleDesc() {
        return Utility.getTupleDesc(1);
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        child.open();
        done = false;
    }

    public void close() {
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
        done = false;
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
        if (done) return null;

        int count = 0;
        while (child.hasNext()) {
            try {
                Database.getBufferPool().insertTuple(tid, tableId, child.next());
                count++;
            } catch (IOException ioe) {
                throw new DbException("Insert::fetchNext");
            }
        }

        done = true;

        Tuple ret = new Tuple(getTupleDesc());
        ret.setField(0, new IntField(count));
        return ret;
    }

    @Override
    public OpIterator[] getChildren() {
        OpIterator[] ret = new OpIterator[1];
        ret[0] = child;
        return ret;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        child = children[0];
    }
}
