package com.xxyw.naivedb.backend.datamanager.dataitem;

import com.google.common.primitives.Bytes;
import com.xxyw.naivedb.backend.common.SubArray;
import com.xxyw.naivedb.backend.datamanager.DataManagerImpl;
import com.xxyw.naivedb.backend.datamanager.page.Page;
import com.xxyw.naivedb.backend.utils.Parser;
import com.xxyw.naivedb.backend.utils.Types;

import java.util.Arrays;

/*
在上层模块试图对 DataItem 进行修改时，需要遵循一定的流程
在修改之前需要调用 before() 方法
想要撤销修改时，调用 unBefore() 方法
在修改完成后，调用 after() 方法
整个流程，主要是为了保存前相数据，并及时落日志。
 */
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

    // 从页面的offset处解析处dataitem
    public static DataItem parseDataItem(Page pg, short offset, DataManagerImpl dm) {
        byte[] raw = pg.getData();
        short size = Parser.parseShort(Arrays.copyOfRange(raw, offset + DataItemImpl.OF_SIZE, offset + DataItemImpl.OF_DATA));
        short length = (short) (size + DataItemImpl.OF_DATA);
        long uid = Types.addressToUid(pg.getPageNumber(), offset);
        return new DataItemImpl(new SubArray(raw, offset, offset + length), new byte[length], pg, uid, dm);
    }

    public static byte[] wrapDataItemRaw(byte[] raw) {
        byte[] valid = new byte[1];
        byte[] size = Parser.short2Byte((short) raw.length);
        return Bytes.concat(valid, size, raw);
    }
}
