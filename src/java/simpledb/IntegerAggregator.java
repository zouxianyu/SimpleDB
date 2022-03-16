package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;

    private final HashMap<Field, Integer> map = new HashMap<>();
    private Integer noGroupingResult = null;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        switch (what) {
            case MIN:
                minHandler(tup);
                break;
            case MAX:
                maxHandler(tup);
                break;
            case SUM:
                sumHandler(tup);
                break;
            case AVG:
                averageHandler(tup);
                break;
            case COUNT:
                countHandler(tup);
                break;
            default:
                throw new UnsupportedOperationException("SUM_COUNT and SC_AVG are not supported");
        }
    }

    private void minHandler(Tuple tuple) {

        if(gbfield == NO_GROUPING) {
            // no grouping case
            IntField aggregateField = (IntField)tuple.getField(afield);
            if(noGroupingResult != null) {
                noGroupingResult = Math.min(noGroupingResult, aggregateField.getValue());
            }else{
                noGroupingResult = aggregateField.getValue();
            }
        }else{
            // grouping case
            Field groupField = tuple.getField(gbfield);
            IntField aggregateField = (IntField)tuple.getField(afield);

            if (map.containsKey(groupField)) {
                int min = map.get(groupField);
                int value = aggregateField.getValue();
                if (value < min) {
                    map.put(groupField, value);
                }
            }else{
                map.put(groupField, aggregateField.getValue());
            }
        }
    }

    private void maxHandler(Tuple tuple) {

        if(gbfield == NO_GROUPING) {
            // no grouping case
            IntField aggregateField = (IntField)tuple.getField(afield);
            if(noGroupingResult != null) {
                noGroupingResult = Math.max(noGroupingResult, aggregateField.getValue());
            }else{
                noGroupingResult = aggregateField.getValue();
            }
        }else{
            // grouping case
            Field groupField = tuple.getField(gbfield);
            IntField aggregateField = (IntField)tuple.getField(afield);

            if (map.containsKey(groupField)) {
                int max = map.get(groupField);
                int value = aggregateField.getValue();
                if (value > max) {
                    map.put(groupField, value);
                }
            }else{
                map.put(groupField, aggregateField.getValue());
            }
        }
    }

    private void sumHandler(Tuple tuple) {

        if(gbfield == NO_GROUPING) {
            // no grouping case
            IntField aggregateField = (IntField)tuple.getField(afield);
            if(noGroupingResult != null) {
                noGroupingResult += aggregateField.getValue();
            }else{
                noGroupingResult = aggregateField.getValue();
            }
        }else{
            // grouping case
            Field groupField = tuple.getField(gbfield);
            IntField aggregateField = (IntField)tuple.getField(afield);

            if (map.containsKey(groupField)) {
                int sum = map.get(groupField) + aggregateField.getValue();
                map.put(groupField, sum);
            }else{
                map.put(groupField, aggregateField.getValue());
            }
        }
    }

    // only for average case
    private final List<Integer> listForAvg = new ArrayList<>();
    private final HashMap<Field, List<Integer>> mapForAvg = new HashMap<>();

    private void averageHandler(Tuple tuple) {
        if(gbfield == NO_GROUPING) {
            // no grouping case
            IntField aggregateField = (IntField)tuple.getField(afield);
            listForAvg.add(aggregateField.getValue());
            noGroupingResult = listForAvg.stream().reduce(0, Integer::sum) / listForAvg.size();
        }else{
            // grouping case
            Field groupField = tuple.getField(gbfield);
            IntField aggregateField = (IntField)tuple.getField(afield);

            if (mapForAvg.containsKey(groupField)) {
                List<Integer> list = mapForAvg.get(groupField);
                list.add(aggregateField.getValue());
                map.put(groupField, list.stream().reduce(0, Integer::sum) / list.size());
            }else{
                List<Integer> list = new ArrayList<>();
                list.add(aggregateField.getValue());
                mapForAvg.put(groupField, list);
                map.put(groupField,aggregateField.getValue());
            }
        }
    }


    private void countHandler(Tuple tuple) {

        if(gbfield == NO_GROUPING) {
            // no grouping case
            IntField aggregateField = (IntField)tuple.getField(afield);
            if(noGroupingResult != null) {
                noGroupingResult ++;
            }else{
                noGroupingResult = 1;
            }
        }else{
            // grouping case
            Field groupField = tuple.getField(gbfield);
            IntField aggregateField = (IntField)tuple.getField(afield);

            if (map.containsKey(groupField)) {
                int sum = map.get(groupField) + 1;
                map.put(groupField, sum);
            }else{
                map.put(groupField, 1);
            }
        }
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
        if(gbfield == NO_GROUPING) {
            return new OpIterator() {

                Iterator<Integer> iterator;
                TupleDesc tupleDesc;

                @Override
                public void open() throws DbException, TransactionAbortedException {
                    ArrayList<Integer> resultWrapper = new ArrayList<>();
                    resultWrapper.add(noGroupingResult);
                    iterator = resultWrapper.iterator();
                    tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{what.toString()});
                }

                @Override
                public boolean hasNext() throws DbException, TransactionAbortedException {
                    return iterator.hasNext();
                }

                @Override
                public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                    Tuple tuple = new Tuple(tupleDesc);
                    tuple.setField(0, new IntField(iterator.next()));
                    return tuple;
                }

                @Override
                public void rewind() throws DbException, TransactionAbortedException {
                    close();
                    open();
                }

                @Override
                public TupleDesc getTupleDesc() {
                    return tupleDesc;
                }

                @Override
                public void close() {
                    tupleDesc = null;
                    iterator = null;
                }
            };
        }else{
            return new OpIterator() {

                Iterator<Map.Entry<Field, Integer>> iterator;
                TupleDesc tupleDesc;

                @Override
                public void open() throws DbException, TransactionAbortedException {
                    iterator = map.entrySet().iterator();
                    tupleDesc = new TupleDesc(new Type[]{gbfieldtype ,Type.INT_TYPE}, new String[]{"groupby",what.toString()});
                }

                @Override
                public boolean hasNext() throws DbException, TransactionAbortedException {
                    if(iterator == null) {
                        return false;
                    }
                    return iterator.hasNext();
                }

                @Override
                public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                    if (iterator == null) {
                        return null;
                    }
                    Tuple tuple = new Tuple(tupleDesc);
                    Map.Entry<Field, Integer> entry = iterator.next();
                    tuple.setField(0, entry.getKey());
                    tuple.setField(1, new IntField(entry.getValue()));
                    return tuple;
                }

                @Override
                public void rewind() throws DbException, TransactionAbortedException {
                    close();
                    open();
                }

                @Override
                public TupleDesc getTupleDesc() {
                    return tupleDesc;
                }

                @Override
                public void close() {
                    tupleDesc = null;
                    iterator = null;
                }
            };
    }
    }

}
