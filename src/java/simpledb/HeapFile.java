package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
	
	File heapFile;
	TupleDesc tD;
	private int id;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here --COMPLETE
    	this.heapFile = f;
    	this.tD = td;
    	this.id = f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here --COMPLETE
        return this.heapFile;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here -- COMPLETE
    	return this.heapFile.getAbsoluteFile().hashCode();
        //throw new UnsupportedOperationException("implement this");
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here -- COMPLETE
    	return this.tD;
        //throw new UnsupportedOperationException("implement this");
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here -- COMPLETE
    	try {
            RandomAccessFile raf = new RandomAccessFile(this.heapFile,"r");
            int offset = BufferPool.getPageSize() * pid.pageNumber();
            if (offset + BufferPool.getPageSize() > raf.length()) {
                System.err.println("The page offset is above the max");
                System.exit(1);
            }
            raf.seek(offset);
            byte[] storedData = new byte[BufferPool.getPageSize()];
            raf.readFully(storedData);
            raf.close();
            return new HeapPage((HeapPageId) pid, storedData);
        } catch (FileNotFoundException e) {
            System.err.println("FileNotFoundException: " + e.getMessage());
            throw new IllegalArgumentException();
        } catch (IOException e) {
            System.err.println("IOException: " + e.getMessage());
            throw new IllegalArgumentException();
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here -- COMPLETE
    	int bPoolSize = BufferPool.getPageSize();
    	int heapFileLen = (int) this.heapFile.length();
    	int nPages = (int) (Math.ceil(heapFileLen/bPoolSize)); 
        return nPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
    	return new HeapFileIterator(this, tid);
    }
    
    class HeapFileIterator extends AbstractDbFileIterator {

        Iterator<Tuple> tupleIter;
        int pageNumber;
        TransactionId currentTid;
        HeapFile heapFile;

        /**
         * Set local variables for HeapFile and Transactionid
         * @param hf The underlying HeapFile.
         * @param tid The transaction ID.
         */
        public HeapFileIterator(HeapFile hf, TransactionId tid) {            
        	heapFile = hf;
            currentTid = tid;
        }

        /**
         * Open the iterator, must be called before readNext.
         */
        public void open() throws DbException, TransactionAbortedException {
            pageNumber = -1;
        }

        @Override
        protected Tuple readNext() throws TransactionAbortedException, DbException {
            
        	// If the current tuple iterator has no more tuples.
        	if (tupleIter != null && !tupleIter.hasNext()) {	
                tupleIter = null;
            }

        	// Keep trying to open a tuple iterator until we find one of run out of pages.
            while (tupleIter == null && pageNumber < heapFile.numPages()-1) {
                pageNumber++;		// Go to next page.
                
                // Get the iterator for the current page
                HeapPageId currentPageId = new HeapPageId(heapFile.getId(), pageNumber);
                HeapPage currentPage = (HeapPage) Database.getBufferPool().getPage(currentTid,
                        currentPageId, Permissions.READ_ONLY);
                tupleIter = currentPage.iterator();
                
                // Make sure the iterator has tuples in it
                if (!tupleIter.hasNext())
                	tupleIter = null;
            }

            // Make sure we found a tuple iterator
            if (tupleIter == null)
                return null;
            
            // Return the next tuple.
            return tupleIter.next();
        }

        /**
         * Rewind closes the current iterator and then opens it again.
         */
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        /**
         * Close the iterator, which resets the counters so it can be opened again.
         */
        public void close() {
            super.close();
            tupleIter = null;
            pageNumber = Integer.MAX_VALUE;
        }
    }
}

