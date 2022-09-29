package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private static final Field dummyField = new IntField(Integer.MAX_VALUE);

    private final int gbfield;

    private final TupleDesc aggTupleDesc;

    Map<Field, Integer> map;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        if (what != Op.COUNT) {
            throw new IllegalArgumentException();
        }

        this.gbfield = gbfield;

        aggTupleDesc = Aggregator.constructTupleDesc(gbfield, gbfieldtype);

        map = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field key = (gbfield == NO_GROUPING) ? dummyField : tup.getField(gbfield);
        map.put(key, map.getOrDefault(key, 0) + 1);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *         aggregateVal) if using group, or a single (aggregateVal) if no
     *         grouping. The aggregateVal is determined by the type of
     *         aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        return new StrAggIterator();
    }

    private class StrAggIterator implements OpIterator {
        List<Tuple> list;
        Iterator<Tuple> iter;

        StrAggIterator() {
            list = new LinkedList<>();

            for (Map.Entry<Field, Integer> entry : map.entrySet()) {
                Tuple t = new Tuple(aggTupleDesc);
                if (gbfield == NO_GROUPING) {
                    t.setField(0, new IntField(entry.getValue()));
                } else {
                    t.setField(0, entry.getKey());
                    t.setField(1, new IntField(entry.getValue()));
                }
                list.add(t);
            }
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            iter = list.iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            return iter.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            return iter.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            open();
        }

        @Override
        public TupleDesc getTupleDesc() {
            return aggTupleDesc;
        }

        @Override
        public void close() {
            iter = null;
        }
    }
}
