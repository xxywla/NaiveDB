package com.xxyw.naivedb.backend.datamanager.page;

import com.xxyw.naivedb.backend.datamanager.pagecache.PageCache;
import com.xxyw.naivedb.backend.utils.Parser;

import java.util.Arrays;

/**
 * 普通页面
 * 2字节无符号数开始 表示这一页的空闲位置的偏移量
 * Free Space Offset FSO
 */
public class PageX {

    private static final short OF_FREE = 0;
    private static final short OF_DATA = 2;
    public static final int MAX_FREE_SPACE = PageCache.PAGE_SIZE - OF_DATA;

    // 将 raw 的数据 写入 page 中 返回插入位置
    public static short insert(Page page, byte[] raw) {
        page.setDirty(true);
        short offset = getFSO(page.getData());
        System.arraycopy(raw, 0,
                page.getData(), offset,
                raw.length);
        // 更新页开头的空闲位置偏移量
        setFSO(page.getData(), (short) (offset + raw.length));
        return offset;
    }

    public static short getFSO(Page page) {
        return getFSO(page.getData());
    }

    private static short getFSO(byte[] raw) {
        return Parser.parseShort(Arrays.copyOfRange(raw, 0, 2));
    }

    private static void setFSO(byte[] raw, short curFSO) {
        System.arraycopy(Parser.short2Byte(curFSO), 0,
                raw, OF_FREE,
                OF_DATA);
    }

    // 获取页面的空闲空间大小
    public static int getFreeSpace(Page page) {
        return PageCache.PAGE_SIZE - (int) getFSO(page.getData());
    }

    // 将raw插入page中的offset位置 并将page的offset设置为较大的offset
    public static void recoverInsert(Page page, byte[] raw, short offset) {
        page.setDirty(true);
        System.arraycopy(raw, 0,
                page.getData(), offset,
                raw.length);
        short rawFSO = getFSO(page.getData());
        if (rawFSO < offset + raw.length) {
            setFSO(page.getData(), (short) (offset + raw.length));
        }
    }

    // 将raw插入page中的offset位置 不更新
    public static void recoverUpdate(Page page, byte[] raw, short offset) {
        page.setDirty(true);
        System.arraycopy(raw, 0,
                page.getData(), offset,
                raw.length);
    }

    public static byte[] initRaw() {
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setFSO(raw, OF_DATA);
        return raw;
    }

}
