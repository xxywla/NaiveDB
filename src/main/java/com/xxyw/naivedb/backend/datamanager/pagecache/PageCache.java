package com.xxyw.naivedb.backend.datamanager.pagecache;

import com.xxyw.naivedb.backend.datamanager.page.Page;
import com.xxyw.naivedb.backend.utils.Panic;
import com.xxyw.naivedb.common.MyError;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

public interface PageCache {
    public static final int PAGE_SIZE = 1 << 13;

    int newPage(byte[] initData);

    Page getPage(int pgNo) throws Exception;

    void close();

    void release(Page page);

    void truncateByPgNo(int maxPgNo);

    int getPageNumber();

    void flushPage(Page pg);

    public static PageCacheImpl create(String path, long memory) {
        File f = new File(path + PageCacheImpl.DB_SUFFIX);
        try {
            if (!f.createNewFile()) {
                Panic.panic(MyError.FileExistsException);
            }
        } catch (Exception e) {
            Panic.panic(e);
        }
        if (!f.canRead() || !f.canWrite()) {
            Panic.panic(MyError.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        return new PageCacheImpl(raf, fc, (int) memory / PAGE_SIZE);
    }

    public static PageCacheImpl open(String path, long memory) {
        File f = new File(path + PageCacheImpl.DB_SUFFIX);
        if (!f.exists()) {
            Panic.panic(MyError.FileNotExistsException);
        }
        if (!f.canRead() || !f.canWrite()) {
            Panic.panic(MyError.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        return new PageCacheImpl(raf, fc, (int) memory / PAGE_SIZE);
    }
}
