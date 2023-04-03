package com.xxyw.naivedb.backend.datamanager.pagecache;

import com.xxyw.naivedb.backend.common.AbstractCache;
import com.xxyw.naivedb.backend.datamanager.page.Page;
import com.xxyw.naivedb.backend.datamanager.page.PageImpl;
import com.xxyw.naivedb.backend.utils.Panic;
import com.xxyw.naivedb.common.MyError;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PageCacheImpl extends AbstractCache<Page> implements PageCache {

    private static final int MEM_MIN_LIM = 10;
    public static final String DB_SUFFIX = ".db";

    private RandomAccessFile file;
    private FileChannel fc;
    private Lock fileLock;

    private AtomicInteger pageNumbers;

    PageCacheImpl(RandomAccessFile file, FileChannel fileChannel, int maxResource) {
        super(maxResource);
        if (maxResource < MEM_MIN_LIM) {
            Panic.panic(MyError.MemTooSmallException);
        }
        long length = 0;
        try {
            length = file.length();
        } catch (IOException e) {
            Panic.panic(e);
        }
        this.file = file;
        this.fc = fileChannel;
        this.fileLock = new ReentrantLock();
        this.pageNumbers = new AtomicInteger((int) length / PAGE_SIZE);
    }


    @Override
    public int newPage(byte[] initData) {
        int pageNo = pageNumbers.incrementAndGet();
        Page page = new PageImpl(pageNo, initData, null);
        flush(page);
        return pageNo;
    }

    @Override
    public Page getPage(int pgNo) throws Exception {
        return get(pgNo);
    }

    @Override
    public void close() {
        super.close();
        try {
            fc.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    @Override
    protected Page getForCache(long key) throws Exception {

        int pgNo = (int) key;
        long offset = PageCacheImpl.pageOffset(pgNo);

        ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);

        fileLock.lock();

        try {
            fc.position(offset);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        fileLock.unlock();

        return new PageImpl(pgNo, buf.array(), this);
    }

    private static long pageOffset(int pgNo) {
        return (long) (pgNo - 1) * PAGE_SIZE;
    }

    @Override
    protected void releaseForCache(Page page) {

        if (page.isDirty()) {
            flush(page);
            page.setDirty(false);
        }

    }

    private void flush(Page page) {
        int pageNo = page.getPageNumber();
        long offset = pageOffset(pageNo);
        fileLock.lock();
        try {
            ByteBuffer buf = ByteBuffer.wrap(page.getData());
            fc.position(offset);
            fc.write(buf);
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        } finally {
            fileLock.unlock();
        }
    }

    @Override
    public void release(Page page) {
        release(page.getPageNumber());
    }

    @Override
    public void truncateByPgNo(int maxPgNo) {

        long size = pageOffset(maxPgNo + 1);

        try {
            file.setLength(size);
        } catch (IOException e) {
            Panic.panic(e);
        }

        pageNumbers.set(maxPgNo);

    }

    @Override
    public int getPageNumber() {
        return pageNumbers.intValue();
    }

    @Override
    public void flushPage(Page pg) {
        flush(pg);
    }
}
