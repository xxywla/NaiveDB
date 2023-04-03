package com.xxyw.naivedb.backend.datamanager.page;

import com.xxyw.naivedb.backend.datamanager.pagecache.PageCache;
import com.xxyw.naivedb.backend.utils.RandomUtil;

import java.util.Arrays;

/*
Valid Check
特殊管理第一页
DB启动时给100-107字节填入随机数
DB关闭时将随机数拷贝到108-115
用于判断上一次DB是否正常关闭
 */
public class PageOne {

    private static final int OF_VC = 100;
    private static final int LEN_VC = 8;

    public static byte[] InitRaw() {
        byte[] raw = new byte[PageCache.PAGE_SIZE];

        setValidCheckOpen(raw);

        return raw;
    }

    private static void setValidCheckOpen(byte[] raw) {

        System.arraycopy(RandomUtil.randomBytes(LEN_VC), 0,
                raw, OF_VC, LEN_VC);

    }

    public static void setValidCheckOpen(Page page) {

        page.setDirty(true);

        setValidCheckOpen(page.getData());

    }

    public static void setValidCheckClose(Page page) {

        page.setDirty(true);

        setValidCheckClose(page.getData());

    }

    private static void setValidCheckClose(byte[] raw) {

        System.arraycopy(raw, OF_VC,
                raw, OF_VC + LEN_VC, LEN_VC);

    }

    public static boolean checkValidCheck(Page page) {

        return checkValidCheck(page.getData());

    }

    private static boolean checkValidCheck(byte[] raw) {

        return Arrays.equals(
                Arrays.copyOfRange(raw, OF_VC, OF_VC + LEN_VC),
                Arrays.copyOfRange(raw, OF_VC + LEN_VC, OF_VC + 2 * LEN_VC)
        );

    }

}
