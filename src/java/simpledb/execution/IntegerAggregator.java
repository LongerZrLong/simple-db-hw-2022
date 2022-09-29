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
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private static final Field dummyField = new IntField(Integer.MAX_VALUE);

    private final int gbfield;
    private final int afield;
    private final Op what;

    private final TupleDesc aggTupleDesc;

    Map<Field, List<Float>> map;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.afield = afield;
        this.what = what;

        aggTupleDesc = Aggregator.constructTupleDesc(gbfield, gbfieldtype);

        map = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field key = (gbfield == NO_GROUPING) ? dummyField : tup.getField(gbfield);

        if (!map.containsKey(key)) {
            map.put(key, new LinkedList<>());
        }

        int tupValue = ((IntField)tup.getField(afield)).getValue();
        map.get(key).add((float)tupValue);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        return new IntAggIterator();
    }

    private class IntAggIterator implements OpIterator {
        List<Tuple> list;
        Iterator<Tuple> iter = null;

        IntAggIterator() {
            list = new LinkedList<>();

            for (Map.Entry<Field, List<Float>> entry : map.entrySet()) {
                float agg = 0;
                switch (what) {
                    case MIN:
                        agg = Collections.min(entry.getValue()); break;
                    case MAX:
                        agg = Collections.max(entry.getValue()); break;
                    case SUM:
                        for (Float it : entry.getValue()) { agg += it; } break;
                    case AVG:
                        for (Float it : entry.getValue()) { agg += it; }
                        agg = agg / entry.getValue().size(); break;
                    case COUNT:
                        agg = entry.getValue().size(); break;
                    default:
                        throw new RuntimeException("Unexpected Branch");
                }

                Tuple t = new Tuple(aggTupleDesc);
                if (gbfield == NO_GROUPING) {
                    t.setField(0, new IntField((int) agg));
                } else {
                    t.setField(0, entry.getKey());
                    t.setField(1, new IntField((int) agg));
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
