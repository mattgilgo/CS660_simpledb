package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {
	private int nFields;	 		//-- COMPLETE
	private String[] fNames;		//-- COMPLETE
	private Type[] fTypes;			//-- COMPLETE

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return null;
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here -- COMPLETE
    	this.nFields = typeAr.length;
    	this.fNames = new String[nFields];
    	this.fTypes = new Type[nFields];
    	for (int i = 0; i < this.nFields; i++) {
    		fNames[i] = fieldAr[i];
    		fTypes[i] = typeAr[i];
    	}
    	
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here -- COMPLETE
    	this.nFields = typeAr.length;
    	this.fNames = new String[nFields];
    	this.fTypes = new Type[nFields];
    	for (int i = 0; i < this.nFields; i++) {
    		fNames[i] = null;
    		fTypes[i] = typeAr[i];
    	}
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here -- COMPLETE
        return this.nFields;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here -- COMPLETE
    	if (i >= this.nFields) {
    		throw new NoSuchElementException();
    	}
        return this.fNames[i];
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here -- COMPLETE
    	if (i >= this.nFields) {
    		throw new NoSuchElementException();
    	}
        return this.fTypes[i];
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        return 0;
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here -- COMPLETE
    	int tupleSizeInBytes = 0;
    	for (int i = 0; i < this.nFields; i ++) {
    		tupleSizeInBytes += fTypes[i].getLen();
    	}
        return tupleSizeInBytes;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here -- COMPLETE
    	int td1td2Len = td1.numFields() + td2.numFields();
    	String mergedFNames[] = new String[td1td2Len];
    	Type mergedFTypes[] = new Type[td1td2Len];
    	for (int i = 0; i < td1.numFields(); i++) {
    		mergedFNames[i] = td1.getFieldName(i);
    		mergedFTypes[i] = td1.getFieldType(i);
    	}
    	for (int i = 0; i < td2.numFields(); i++) {
    		mergedFNames[i+td1.numFields()] = td2.getFieldName(i);
    		mergedFTypes[i+td1.numFields()] = td2.getFieldType(i);
    	}
    	TupleDesc mergedTuple = new TupleDesc(mergedFTypes, mergedFNames);
        return mergedTuple;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        // some code goes here -- COMPLETE
    	TupleDesc tdObject;
    	if (o == null) {
    		return false;
    	}
    	try{
    		tdObject = (TupleDesc) o;
    	} catch (ClassCastException cce) {
    	    return false;
    	}
    	if (this.getSize()!=tdObject.getSize()) {
    		return false;
    	}    
    	for (int i = 0; i < this.nFields; i++) {
    	    if (this.getFieldType(i)!=tdObject.getFieldType(i))
    		return false;
    	} 
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here -- COMPLETE
    	String tupleStr = new String();
    	for (int i=0; i < this.nFields; i++) {
    		tupleStr += fTypes[i];
    		tupleStr += "(";
    		tupleStr += fNames[i];
    		tupleStr += ")";
    	}
        return tupleStr;
    }
}
