package simpledb.optimizer;

import simpledb.common.Database;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.execution.SeqScan;
import simpledb.storage.*;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import javax.xml.crypto.Data;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query.
 * <p>
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }

    public static void setStatsMap(Map<String, TableStats> s) {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    private final int tableid;
    private final int ioCostPerPage;
    private int cardinality;
    private final Map<Integer, IntHistogram> intColHistMap;     // fieldIdx -> Hist
    private final Map<Integer, StringHistogram> strColHistMap;  // fieldIdx -> Hist

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     *
     * @param tableid       The table over which to compute statistics
     * @param ioCostPerPage The cost per page of IO. This doesn't differentiate between
     *                      sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.

        this.tableid = tableid;
        this.cardinality = 0;
        this.ioCostPerPage = ioCostPerPage;

        intColHistMap = new HashMap<>();
        strColHistMap = new HashMap<>();

        Map<Integer, Integer> intColMinMap = new HashMap<>();
        Map<Integer, Integer> intColMaxMap = new HashMap<>();

        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableid);
        TupleDesc tupleDesc = Database.getCatalog().getTupleDesc(tableid);

        for (int i = 0; i < tupleDesc.numFields(); i++) {
            if (tupleDesc.getFieldType(i).equals(Type.INT_TYPE)) {
                intColMinMap.put(i, Integer.MAX_VALUE);
                intColMaxMap.put(i, Integer.MIN_VALUE);
            } else if (tupleDesc.getFieldType(i).equals(Type.STRING_TYPE)) {
                strColHistMap.put(i, new StringHistogram(NUM_HIST_BINS));
            } else {
                throw new RuntimeException("TableStats::TableStats: Unexpected field type");
            }
        }

        // scan through the table to find the max and min for each int column
        DbFileIterator iter = dbFile.iterator(new TransactionId());
        try {
            iter.open();
            while (iter.hasNext()) {
                Tuple tup = iter.next();
                for (Integer key : intColMinMap.keySet()) {
                    int val = ((IntField)tup.getField(key)).getValue();
                    intColMinMap.put(key, Math.min(intColMinMap.get(key), val));
                    intColMaxMap.put(key, Math.max(intColMaxMap.get(key), val));
                }
                cardinality++;
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("TableStats::TableStats: Unable to scan table");
        }

        for (Integer key : intColMinMap.keySet()) {
            intColHistMap.put(key, new IntHistogram(NUM_HIST_BINS, intColMinMap.get(key), intColMaxMap.get(key)));
        }

        // scan the table to compute histogram for each field
        try {
            iter.rewind();
            while (iter.hasNext()) {
                Tuple tup = iter.next();
                for (int i = 0; i < tupleDesc.numFields(); i++) {
                    if (tupleDesc.getFieldType(i) == Type.INT_TYPE) {
                        intColHistMap.get(i).addValue(((IntField)tup.getField(i)).getValue());
                    } else if (tupleDesc.getFieldType(i) == Type.STRING_TYPE) {
                        strColHistMap.get(i).addValue(((StringField)tup.getField(i)).getValue());
                    } else {
                        throw new RuntimeException("TableStats::TableStats: Unexpected field type");
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("TableStats::TableStats: Unable to scan table");
        }
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * <p>
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     *
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // estimate number of pages
        int pageSize = BufferPool.getPageSize();
        int estimatedNumPages = (cardinality * Database.getCatalog().getTupleDesc(tableid).getSize() + pageSize - 1) / pageSize;
        return estimatedNumPages * ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     *
     * @param selectivityFactor The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        return (int) (cardinality * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     *
     * @param field the index of the field
     * @param op    the operator in the predicate
     *              The semantic of the method is that, given the table, and then given a
     *              tuple, of which we do not know the value of the field, return the
     *              expected selectivity. You may estimate this value from the histograms.
     */
    public double avgSelectivity(int field, Predicate.Op op) {
        // TODO: some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     *
     * @param field    The field over which the predicate ranges
     * @param op       The logical operation in the predicate
     * @param constant The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        if (intColHistMap.containsKey(field)) {
            return intColHistMap.get(field).estimateSelectivity(op, ((IntField)constant).getValue());
        }

        if (strColHistMap.containsKey(field)) {
            return strColHistMap.get(field).estimateSelectivity(op, ((StringField)constant).getValue());
        }

        throw new RuntimeException("TableStats::estimateSelectivity: Unexpected field");
    }

    /**
     * return the total number of tuples in this table
     */
    public int totalTuples() {
        return cardinality;
    }

}
