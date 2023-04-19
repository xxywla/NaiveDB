package com.xxyw.naivedb.backend.datamanager.pageindex;

import com.xxyw.naivedb.backend.datamanager.pagecache.PageCache;
import org.junit.Test;

public class PageIndexTest {
    @Test
    public void testPageIndex() {
        PageIndex pageIndex = new PageIndex();
        int threshold = PageCache.PAGE_SIZE / 20;
        for (int i = 0; i < 20; i++) {
            pageIndex.add(i, i * threshold);
            pageIndex.add(i, i * threshold);
            pageIndex.add(i, i * threshold);
        }
        for (int k = 0; k < 3; k++) {
            for (int i = 0; i < 19; i++) {
                PageInfo pageInfo = pageIndex.select(i * threshold);
                assert pageInfo != null;
                assert pageInfo.pageNo == i + 1;
            }
        }
    }
}
