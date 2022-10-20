package simpledb.optimizer;

import simpledb.execution.Predicate;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int[] bins;
    private int nTups;
    private final int min;
    private final int max;
    private final int elementsEach;

    /**
     * Create a new IntHistogram.
     * <p>
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * <p>
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * <p>
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min     The minimum integer value that will ever be passed to this class for histogramming
     * @param max     The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        this.bins = new int[buckets];
        this.nTups = 0;
        this.min = min;
        this.max = max;
        this.elementsEach = ((max - min + 1) + buckets - 1) / buckets;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        bins[(v - min) / elementsEach]++;
        nTups++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * <p>
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v  Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        int idx = (v - min) / elementsEach;
        switch (op) {
            case EQUALS:
                if (v < min) return 0.0;
                if (v > max) return 0.0;
                return (double) bins[idx] / elementsEach / nTups;
            case GREATER_THAN:
                if (v >= max) return 0.0;
                if (v < min) return 1.0;
                int right = min + elementsEach * (idx + 1) - 1;
                double b_part = (double) (right - v) / elementsEach * bins[idx] / nTups;
                int sum = 0;
                for (int i = idx + 1; i < bins.length; i++) { sum += bins[i]; }
                double rest = (double) sum / nTups;
                return b_part + rest;
            case LESS_THAN:
                return 1.0 - estimateSelectivity(Predicate.Op.GREATER_THAN_OR_EQ, v);
            case GREATER_THAN_OR_EQ:
                return estimateSelectivity(Predicate.Op.EQUALS, v) + estimateSelectivity(Predicate.Op.GREATER_THAN, v);
            case LESS_THAN_OR_EQ:
                return 1.0 - estimateSelectivity(Predicate.Op.GREATER_THAN, v);
            case NOT_EQUALS:
                return 1.0 - estimateSelectivity(Predicate.Op.EQUALS, v);
            default:
                throw new RuntimeException("IntHistogram::Unsupported Op");
        }
    }

    /**
     * @return the average selectivity of this histogram.
     *         <p>
     *         This is not an indispensable method to implement the basic
     *         join optimization. It may be needed if you want to
     *         implement a more efficient optimization
     */
    public double avgSelectivity() {
        // TODO: some code goes here
        return 1.0;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("min: ").append(min).append(", ");
        stringBuilder.append("max: ").append(max).append(", ");
        stringBuilder.append("bins: [");
        for (int i = 0; i < bins.length - 1; i++) {
            stringBuilder.append(bins[i]).append(", ");
        }
        stringBuilder.append(bins[bins.length - 1]).append("]");
        return stringBuilder.toString();
    }
}
