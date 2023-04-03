package com.xxyw.naivedb.backend.datamanager.page;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class MockPage implements Page {

    private int pageNo;
    private byte[] data;
    private Lock lock = new ReentrantLock();

    public static MockPage newMockPage(int pageNo, byte[] data) {
        MockPage mockPage = new MockPage();
        mockPage.pageNo = pageNo;
        mockPage.data = data;
        return mockPage;
    }

    @Override
    public void lock() {
        lock.lock();
    }

    @Override
    public void unlock() {
        lock.unlock();
    }

    @Override
    public void release() {

    }

    @Override
    public void setDirty(boolean dirty) {

    }

    @Override
    public boolean isDirty() {
        return false;
    }

    @Override
    public int getPageNumber() {
        return pageNo;
    }

    @Override
    public byte[] getData() {
        return data;
    }
}
