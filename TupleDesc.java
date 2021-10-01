package simpledb;

import org.hamcrest.internal.ArrayIterator;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {
    private static final long serialVersionUID = 1L;
    private int numFields;
    private String[] fieldNames;
    private TDItem[] fieldItems;
    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        Iterator itr = new ArrayIterator();
        return itr;
    }

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
        // some code goes here
        this.numFields = typeAr.length;
        this.fieldNames = new String[fieldAr.length];
        this.fieldItems = new TDItem[typeAr.length];
        for (int i = 0; i < numFields; i++){
            this.fieldNames[i] = fieldAr[i];
            this.fieldItems[i] = new TDItem(typeAr[i], fieldAr[i]);
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
        // some code goes here
        this.numFields = typeAr.length;
        this.fieldNames = new String[typeAr.length];
        this.fieldItems = new TDItem[typeAr.length];
        for (int i = 0; i < typeAr.length; i++){
            this.fieldNames[i] = null;
            this.fieldItems[i] = new TDItem(typeAr[i]);
        }
    }
    public TupleDesc(TDItem[] tdItems) {
        // some code goes here
        this.numFields = tdItems.length;
        this.fieldNames = new String[tdItems.length];
        this.fieldItems = new TDItem[tdItems.length];
        for (int i = 0; i < tdItems.length; i++){
            this.fieldNames[i] = tdItems[i].fieldName;
            this.fieldItems[i] = tdItems[i];
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return this.numFields;
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
        // some code goes here
        if (i >= this.numFields){
            throw new NoSuchElementException();
        }
        return this.fieldNames[i];
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
        // some code goes here
        if (i >= this.numFields) {
            throw new NoSuchElementException();
        }
        return this.fieldItems[i].getFieldType();
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
        for (int i=0; i < this.numFields; i++) {
            if (this.fieldNames[i] == null){
                throw new NoSuchElementException();
            }
            if (this.fieldItems[i].fieldName.equals(name)){
                return i;
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int byteSize = 0;
        for (int i = 0; i < this.numFields; i++) {
            byteSize += this.fieldItems[i].fieldType.getLen();
        }
        //System.out.println(byteSize);
        return byteSize;
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
        // some code goes here
        Type mergedTypes[] = new Type[td1.numFields()+td2.numFields()];
        String mergedNames[] = new String[td1.numFields()+td2.numFields()];
        for (int i=0; i<td1.numFields(); i++){
            mergedTypes[i]=td1.getFieldType(i);
            mergedNames[i]=td1.getFieldName(i);
        }
        for (int j=0; j<td2.numFields(); j++){
            mergedTypes[td1.numFields()+j]=td2.getFieldType(j);
            mergedNames[td1.numFields()+j]=td2.getFieldName(j);
        }
        TupleDesc mergedtuple = new TupleDesc(mergedTypes,mergedNames);
        return mergedtuple;
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
        // some code goes here
        TupleDesc td;
        if (this == o) return true;
        if (o == null) {
            return false;
        }
        try{
            td = (TupleDesc) o;
        } catch (ClassCastException cce){
            return false;
        }
        if (this.getSize()!=td.getSize()) {
            return false;
        }
        for (int i = 0; i < this.numFields; i++) {
            if (this.getFieldType(i)!=td.getFieldType(i))
                return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(this.numFields);
        result = 31 * result + (this.fieldItems[numFields-1].fieldType == null ? 0 : this.fieldItems[numFields-1].fieldType.hashCode());
        return result;
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        String output = new String();
        for ( int i = 0; i < this.numFields; i++){
            output += this.fieldItems[i].toString();
        }
        return output;
    }

    /**
     * Helper Class for Iterator
     * */
    private class ArrayIterator implements Iterator {
        int current;
        public ArrayIterator (){
            this.current = 0;
        }
        public boolean hasNext(){
            return (current < numFields);
        }
        public Object next() {
            if (!hasNext())
                throw new NoSuchElementException();
            current++;
            return fieldItems[current - 1];
        }
    }
}
