package com.xxyw.naivedb.backend.datamanager;

import com.xxyw.naivedb.backend.common.AbstractCache;
import com.xxyw.naivedb.backend.datamanager.dataitem.DataItem;
import com.xxyw.naivedb.backend.datamanager.dataitem.DataItemImpl;
import com.xxyw.naivedb.backend.datamanager.logger.Logger;
import com.xxyw.naivedb.backend.datamanager.page.Page;
import com.xxyw.naivedb.backend.datamanager.page.PageOne;
import com.xxyw.naivedb.backend.datamanager.page.PageX;
import com.xxyw.naivedb.backend.datamanager.pagecache.PageCache;
import com.xxyw.naivedb.backend.datamanager.pageindex.PageIndex;
import com.xxyw.naivedb.backend.datamanager.pageindex.PageInfo;
import com.xxyw.naivedb.backend.datamanager.recover.Recover;
import com.xxyw.naivedb.backend.transaction.TransactionManager;
import com.xxyw.naivedb.backend.utils.Panic;
import com.xxyw.naivedb.backend.utils.Types;
import com.xxyw.naivedb.common.MyError;

public class DataManagerImpl extends AbstractCache<DataItem> implements DataManager {

    TransactionManager tranMan;
    PageCache pageCache;
    Logger logger;
    PageIndex pageIndex;
    Page pageOne;

    public DataManagerImpl(PageCache pageCache, Logger logger, TransactionManager tranMan) {
        super(0);
        this.pageCache = pageCache;
        this.logger = logger;
        this.tranMan = tranMan;
        this.pageIndex = new PageIndex();
    }

    // 初始化 PageIndex
    void fillPageIndex() {
        int pageNumber = pageCache.getPageNumber();
        for (int i = 2; i <= pageNumber; i++) {
            Page page = null;
            try {
                page = pageCache.getPage(i);
            } catch (Exception e) {
                Panic.panic(e);
            }
            pageIndex.add(pageCache.getPageNumber(), PageX.getFreeSpace(page));
            page.release();
        }
    }

    // 为 xid 生成 update 日志
    public void logDataItem(long xid, DataItem dataItem) {
        byte[] log = Recover.updateLog(xid, dataItem);
        logger.log(log);
    }

    // 在创建文件时初始化PageOne
    void initPageOne() {
        int pageNo = pageCache.newPage(PageOne.InitRaw());
        assert pageNo == 1;
        try {
            pageOne = pageCache.getPage(pageNo);
        } catch (Exception e) {
            Panic.panic(e);
        }
        pageCache.flushPage(pageOne);
    }

    // 在打开已有文件时读入 PageOne 并验证正确性
    boolean loadCheckPageOne() {
        try {
            pageOne = pageCache.getPage(1);
        } catch (Exception e) {
            Panic.panic(e);
        }
        return PageOne.checkValidCheck(pageOne);
    }

    @Override
    public DataItem read(long uid) throws Exception {
        DataItemImpl dataItem = (DataItemImpl) super.get(uid);
        if (!dataItem.isValid()) {
            dataItem.release();
            return null;
        }
        return dataItem;
    }

    @Override
    public long insert(long xid, byte[] data) throws Exception {
        byte[] raw = DataItem.wrapDataItemRaw(data);
        if (raw.length > PageX.MAX_FREE_SPACE) {
            throw MyError.DataTooLargeException;
        }

        PageInfo pageInfo = null;
        for (int i = 0; i < 5; i++) {
            pageInfo = pageIndex.select(raw.length);
            if (pageInfo != null) {
                break;
            } else {
                int newPgno = pageCache.newPage(PageX.initRaw());
                pageIndex.add(newPgno, PageX.MAX_FREE_SPACE);
            }
        }
        if (pageInfo == null) {
            throw MyError.DatabaseBusyException;
        }

        Page pg = null;
        int freeSpace = 0;
        try {
            pg = pageCache.getPage(pageInfo.pageNo);
            byte[] log = Recover.insertLog(xid, pg, raw);
            logger.log(log);

            short offset = PageX.insert(pg, raw);

            pg.release();
            return Types.addressToUid(pageInfo.pageNo, offset);

        } finally {
            // 将取出的pg重新插入pIndex
            if (pg != null) {
                pageIndex.add(pageInfo.pageNo, PageX.getFreeSpace(pg));
            } else {
                pageIndex.add(pageInfo.pageNo, freeSpace);
            }
        }
    }

    @Override
    public void close() {
        super.close();
        logger.close();

        PageOne.setValidCheckClose(pageOne);
        pageOne.release();
        pageCache.close();
    }

    // 获取缓存的DataItem
    @Override
    protected DataItem getForCache(long uid) throws Exception {
        short offset = (short) (uid & ((1L << 16) - 1));
        uid >>>= 32;
        int pageNo = (int) (uid & ((1L << 32) - 1));
        Page page = pageCache.getPage(pageNo);
        return DataItem.parseDataItem(page, offset, this);
    }

    @Override
    protected void releaseForCache(DataItem dataItem) {
        dataItem.page().release();
    }

    public void releaseDataItem(DataItem dataItem) {
        super.release(dataItem.getUid());
    }
}
