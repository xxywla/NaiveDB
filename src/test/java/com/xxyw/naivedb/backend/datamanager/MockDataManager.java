package com.xxyw.naivedb.backend.datamanager;

import com.xxyw.naivedb.backend.common.SubArray;
import com.xxyw.naivedb.backend.datamanager.dataitem.DataItem;
import com.xxyw.naivedb.backend.datamanager.dataitem.MockDataItem;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class MockDataManager implements DataManager {

    private Map<Long, DataItem> cache;
    private Lock lock;

    public static MockDataManager newMockDataManager() {
        MockDataManager dataMan = new MockDataManager();
        dataMan.cache = new HashMap<>();
        dataMan.lock = new ReentrantLock();
        return dataMan;
    }

    @Override
    public DataItem read(long uid) throws Exception {
        lock.lock();
        try {
            return cache.get(uid);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public long insert(long xid, byte[] data) throws Exception {
        lock.lock();
        try {
            long uid = 0;
            while (true) {
                uid = Math.abs(new Random(System.nanoTime()).nextInt(Integer.MAX_VALUE));
                if (uid == 0) continue;
                if (cache.containsKey(uid)) continue;
                break;
            }
            DataItem dataItem = MockDataItem.newMockDataItem(uid, new SubArray(data, 0, data.length));
            cache.put(uid, dataItem);
            return uid;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {

    }
}
