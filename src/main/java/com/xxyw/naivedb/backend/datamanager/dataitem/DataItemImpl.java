package com.xxyw.naivedb.backend.datamanager.dataitem;

import com.xxyw.naivedb.backend.common.SubArray;
import com.xxyw.naivedb.backend.datamanager.page.Page;

public class DataItemImpl implements DataItem {

    static final int OF_VALID = 0;
    static final int OF_SIZE = 1;
    static final int OF_DATA = 3;

    @Override
    public SubArray data() {
        return null;
    }

    @Override
    public void before() {

    }

    @Override
    public void unBefore() {

    }

    @Override
    public void after(long xid) {

    }

    @Override
    public void release() {

    }

    @Override
    public void lock() {

    }

    @Override
    public void unlock() {

    }

    @Override
    public void rLock() {

    }

    @Override
    public void rUnLock() {

    }

    @Override
    public Page page() {
        return null;
    }

    @Override
    public long getUid() {
        return 0;
    }

    @Override
    public byte[] getOldRaw() {
        return new byte[0];
    }

    @Override
    public SubArray getRaw() {
        return null;
    }
}
