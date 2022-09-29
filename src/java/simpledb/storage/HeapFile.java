package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    private final File file;
    private final TupleDesc td;
    private final int tableId;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.td = td;
        this.tableId = f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return tableId;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        try {
            int offset = BufferPool.getPageSize() * pid.getPageNumber();
            FileInputStream fis = new FileInputStream(file);

            // set fis at offset
            fis.getChannel().position(offset);

            byte[] data = new byte[BufferPool.getPageSize()];
            fis.read(data);

            fis.close();

            return new HeapPage((HeapPageId) pid, data);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        PageId pid = page.getId();

        int offset = pid.getPageNumber() * BufferPool.getPageSize();
        final RandomAccessFile raf = new RandomAccessFile(file, "rw");
        raf.seek(offset);
        raf.write(page.getPageData());
        raf.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        int pageSize = BufferPool.getPageSize();
        return (int) (file.length() + pageSize - 1) / pageSize;
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // find a page with empty slot
        int pgNo;
        List<Page> ret = new ArrayList<>();
        for (pgNo = 0; pgNo < numPages(); pgNo++) {
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(tableId, pgNo), null);
            try {
                page.insertTuple(t);

                ret.add(page);
                break;
            } catch (DbException ignored) {
            }
        }

        if (pgNo == numPages()) {
            // No empty slot in existing pages of this file, create a new page

            // append a page of data to file
            RandomAccessFile raf = new RandomAccessFile(file, "rw");
            raf.seek(file.length());
            raf.write(HeapPage.createEmptyPageData());
            raf.close();

            // get the new page
            HeapPage newPage = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(tableId, pgNo), null);
            newPage.insertTuple(t);

            ret.add(newPage);
        }

        return ret;
    }

    // see DbFile.java for javadocs
    public List<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        int pgNo;
        ArrayList<Page> ret = new ArrayList<>();
        for (pgNo = 0; pgNo < numPages(); pgNo++) {
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(tableId, pgNo), null);
            try {
                page.deleteTuple(t);

                ret.add(page);
                break;
            } catch (DbException dbe) {
            }
        }

        if (pgNo == numPages()) {
            throw new DbException("HeapFile::deleteTuple: Fail to delete tuple");
        }

        return ret;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new DbFileIterator() {

            private int pageNo = -1;
            private Iterator<Tuple> tupleIter;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                pageNo = 0;
                HeapPage page = (HeapPage)Database.getBufferPool().getPage(null, new HeapPageId(tableId, pageNo), null);
                tupleIter = page.iterator();
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (pageNo == -1) return false;     // not open

                if (tupleIter.hasNext()) return true;

                if (++pageNo == numPages()) return false;

                HeapPage page = (HeapPage)Database.getBufferPool().getPage(null, new HeapPageId(tableId, pageNo), null);
                tupleIter = page.iterator();

                return hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (!hasNext()) throw new NoSuchElementException();

                return tupleIter.next();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                open();
            }

            @Override
            public void close() {
                pageNo = -1;
            }
        };
    }

}

