package com.xxyw.naivedb.backend.datamanager;

import com.xxyw.naivedb.backend.datamanager.dataitem.DataItem;
import com.xxyw.naivedb.backend.datamanager.logger.Logger;
import com.xxyw.naivedb.backend.datamanager.page.PageOne;
import com.xxyw.naivedb.backend.datamanager.pagecache.PageCache;
import com.xxyw.naivedb.backend.datamanager.recover.Recover;
import com.xxyw.naivedb.backend.transaction.TransactionManager;

public interface DataManager {

    DataItem read(long uid) throws Exception;

    long insert(long xid, byte[] data) throws Exception;

    void close();

    public static DataManager create(String path, long memory, TransactionManager tranMan) {
        PageCache pageCache = PageCache.create(path, memory);
        Logger logger = Logger.create(path);
        DataManagerImpl dataMan = new DataManagerImpl(pageCache, logger, tranMan);
        dataMan.initPageOne();
        return dataMan;
    }

    public static DataManager open(String path, long memory, TransactionManager tranMan) {
        PageCache pageCache = PageCache.open(path, memory);
        Logger logger = Logger.open(path);
        DataManagerImpl dataMan = new DataManagerImpl(pageCache, logger, tranMan);
        if (!dataMan.loadCheckPageOne()) {
            Recover.recover(tranMan, logger, pageCache);
        }
        dataMan.fillPageIndex();
        PageOne.setValidCheckOpen(dataMan.pageOne);
        dataMan.pageCache.flushPage(dataMan.pageOne);

        return dataMan;
    }
}
