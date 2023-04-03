package com.xxyw.naivedb.backend.datamanager.pagecache;

import com.xxyw.naivedb.backend.datamanager.page.MockPage;
import com.xxyw.naivedb.backend.datamanager.page.Page;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 直接使用 Map 结构作为缓存 用于测试
 */
public class MockPageCache implements PageCache {


    private Map<Integer, MockPage> cache = new HashMap<>();
    private Lock lock = new ReentrantLock();
    private AtomicInteger pageCnt = new AtomicInteger(0);

    @Override
    public int newPage(byte[] initData) {
        lock.lock();
        try {
            int pageNo = pageCnt.incrementAndGet();
            MockPage page = MockPage.newMockPage(pageNo, initData);
            cache.put(pageNo, page);
            return pageNo;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Page getPage(int pgNo) throws Exception {
        lock.lock();
        try {
            return cache.get(pgNo);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void release(Page page) {

    }

    @Override
    public void truncateByPgNo(int maxPgNo) {

    }

    @Override
    public int getPageNumber() {
        return pageCnt.intValue();
    }

    @Override
    public void flushPage(Page pg) {

    }
}
