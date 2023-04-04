package com.xxyw.naivedb.backend.datamanager.dataitem;

import com.xxyw.naivedb.backend.common.SubArray;
import com.xxyw.naivedb.backend.datamanager.page.Page;

public interface DataItem {
    SubArray data();

    void before();

    void unBefore();

    void after(long xid);

    void release();

    void lock();

    void unlock();

    void rLock();

    void rUnLock();

    Page page();

    long getUid();

    byte[] getOldRaw();

    SubArray getRaw();

    public static void setDataItemRawInvalid(byte[] raw) {
        raw[DataItemImpl.OF_VALID] = (byte) 1;
    }
}
