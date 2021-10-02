package simpledb;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {

    private static final long serialVersionUID = 1L;
    int tableID;
    String tableALIAS;
    TransactionId tID;
    DbFile heapFile;
    DbFileIterator iter;
    
    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // some code goes here -- ALMOST COMPLETE NEEDS FILE ASSERT PART
    	tableID = tableid;
    	tID = tid;
    	tableALIAS = tableAlias;
    	heapFile = Database.getCatalog().getDatabaseFile(tableid);
    	iter = heapFile.iterator(tid);
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        String tableName = Database.getCatalog().getTableName(tableID);
    	return tableName;
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias()
    {
        // some code goes here -- COMPLETE
        return this.tableALIAS;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        // some code goes here -- COMPLETE
    	tableID = tableid;
    	tableALIAS = tableAlias;
    }

    public SeqScan(TransactionId tid, int tableid) {
        this(tid, tableid, Database.getCatalog().getTableName(tableid));
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here -- COMPLETE
    	iter.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here -- COMPLETE
        TupleDesc oriTupleDesc = Database.getCatalog().getTupleDesc(tableID);
        Type[] types = new Type[oriTupleDesc.numFields()];
        String[] fields = new String[oriTupleDesc.numFields()];
        for (int i = 0; i < oriTupleDesc.numFields(); i++) {
        	types[i] = oriTupleDesc.getFieldType(i);
        	fields[i] = oriTupleDesc.getFieldName(i);
        }
        return new TupleDesc(types,fields);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here -- COMPLETE
        return iter.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here -- COMPLETE
        return iter.next();
    }

    public void close() {
        // some code goes here -- COMPLETE
    	iter.close();
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here -- COMPLETE
    	iter.rewind();
    }
}
