package com.xxyw.naivedb.backend.datamanager.recover;

import com.xxyw.naivedb.backend.datamanager.dataitem.DataItem;
import com.xxyw.naivedb.backend.datamanager.logger.Logger;
import com.xxyw.naivedb.backend.datamanager.page.Page;
import com.xxyw.naivedb.backend.datamanager.page.PageX;
import com.xxyw.naivedb.backend.datamanager.pagecache.PageCache;
import com.xxyw.naivedb.backend.transaction.TransactionManager;
import com.xxyw.naivedb.backend.utils.Panic;
import com.xxyw.naivedb.backend.utils.Parser;

import java.util.*;

/**
 * DataManager 为上层提供两种操作 插入新数据 I 和 更新现有数据 U
 * 在 I 和 U 操作之前 必须先进行日志操作 保证日志写入磁盘
 * 规定1 正在进行的事务 不会读取其他未提交事务的数据
 * 规定2 正在进行的事务 不会修改其他未提交的事务修改或产生的数据
 * 基于以上规定 日志恢复
 * 1 重做所有崩溃时 已完成 committed aborted 的事务
 * 2 撤销所有崩溃时 未完成 active 的事务
 * <p>
 * 两种日志格式
 * updateLog
 * [LogType] [XID] [UID] [OldRaw] [NewRaw]
 * insertLog
 * [LogType] [XID] [PageNo] [Offset] [Raw]
 */
public class Recover {
    private static final byte LOG_TYPE_INSERT = 0;
    private static final byte LOG_TYPE_UPDATE = 1;

    private static final int REDO = 0;
    private static final int UNDO = 1;

    static class InsertLogInfo {
        long xid;
        int pageNo;
        short offset;
        byte[] raw;
    }

    static class UpdateLogInfo {
        long xid;
        int pageNo;
        short offset;
        byte[] oldRaw;
        byte[] newRaw;
    }

    public static void recover(TransactionManager tranMan, Logger logger, PageCache pageCache) {
        System.out.println("Recovering...");
        logger.rewind();
        int maxPageNo = 0;
        while (true) {
            byte[] log = logger.next();
            if (log == null) break;
            int pageNo;
            if (isInsertLog(log)) {
                InsertLogInfo logInfo = parseInsertLog(log);
                pageNo = logInfo.pageNo;
            } else {
                UpdateLogInfo logInfo = parseUpdateLog(log);
                pageNo = logInfo.pageNo;
            }
            if (pageNo > maxPageNo) {
                maxPageNo = pageNo;
            }
        }
        if (maxPageNo == 0) {
            maxPageNo = 1;
        }
        pageCache.truncateByPgNo(maxPageNo);
        System.out.println("Truncate to " + maxPageNo + " pages.");

        // 重做所有崩溃时 已完成 committed aborted 的事务
        redoTransactions(tranMan, logger, pageCache);
        System.out.println("Redo Transactions Over.");

        // 撤销所有崩溃时 未完成 active 的事务
        undoTransactions(tranMan, logger, pageCache);
        System.out.println("Undo Transactions Over.");

        System.out.println("Recovery Over.");
    }

    private static void undoTransactions(TransactionManager tranMan, Logger logger, PageCache pageCache) {
        Map<Long, List<byte[]>> logCache = new HashMap<>();
        logger.rewind();
        while (true) {
            byte[] log = logger.next();
            if (log == null) break;
            if (isInsertLog(log)) {
                InsertLogInfo logInfo = parseInsertLog(log);
                long xid = logInfo.xid;
                if (tranMan.isActive(xid)) {
                    if (!logCache.containsKey(xid)) {
                        logCache.put(xid, new ArrayList<>());
                    }
                    logCache.get(xid).add(log);
                }
            } else {
                UpdateLogInfo logInfo = parseUpdateLog(log);
                long xid = logInfo.xid;
                if (tranMan.isActive(xid)) {
                    if (!logCache.containsKey(xid)) {
                        logCache.put(xid, new ArrayList<>());
                    }
                    logCache.get(xid).add(log);
                }
            }
        }
        // 对所有的 active log 倒序 undo
        for (Map.Entry<Long, List<byte[]>> entry : logCache.entrySet()) {
            List<byte[]> value = entry.getValue();
            for (int i = value.size() - 1; i >= 0; i--) {
                byte[] log = value.get(i);
                if (isInsertLog(log)) {
                    doInsertLog(pageCache, log, UNDO);
                } else {
                    doUpdateLog(pageCache, log, UNDO);
                }
            }
            tranMan.abort(entry.getKey());
        }
    }

    private static void redoTransactions(TransactionManager tranMan, Logger logger, PageCache pageCache) {
        logger.rewind();
        while (true) {
            byte[] log = logger.next();
            if (log == null) break;
            if (isInsertLog(log)) {
                InsertLogInfo logInfo = parseInsertLog(log);
                long xid = logInfo.xid;
                if (!tranMan.isActive(xid)) {
                    doInsertLog(pageCache, log, REDO);
                }
            } else {
                UpdateLogInfo logInfo = parseUpdateLog(log);
                long xid = logInfo.xid;
                if (!tranMan.isActive(xid)) {
                    doUpdateLog(pageCache, log, REDO);
                }
            }
        }
    }

    private static boolean isInsertLog(byte[] log) {
        return log[0] == LOG_TYPE_INSERT;
    }

    private static final int OF_TYPE = 0;
    private static final int OF_XID = OF_TYPE + 1;

    // [LogType] [XID] [PageNo] [Offset] [Raw]
    private static final int OF_INSERT_PAGE_NO = OF_XID + 8;
    private static final int OF_INSERT_OFFSET = OF_INSERT_PAGE_NO + 4;
    private static final int OF_INSERT_RAW = OF_INSERT_OFFSET + 2;

    // 解析 插入数据的 日志
    private static InsertLogInfo parseInsertLog(byte[] log) {
        InsertLogInfo logInfo = new InsertLogInfo();
        logInfo.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_INSERT_PAGE_NO));
        logInfo.pageNo = Parser.parseInt(Arrays.copyOfRange(log, OF_INSERT_PAGE_NO, OF_INSERT_OFFSET));
        logInfo.offset = Parser.parseShort(Arrays.copyOfRange(log, OF_INSERT_OFFSET, OF_INSERT_RAW));
        logInfo.raw = Arrays.copyOfRange(log, OF_INSERT_RAW, log.length);
        return logInfo;
    }

    // 执行插入数据
    private static void doInsertLog(PageCache pageCache, byte[] log, int flag) {
        InsertLogInfo logInfo = parseInsertLog(log);
        Page page = null;
        try {
            page = pageCache.getPage(logInfo.pageNo);
        } catch (Exception e) {
            Panic.panic(e);
        }
        try {
            if (flag == UNDO) {
                DataItem.setDataItemRawInvalid(logInfo.raw);
            }
            PageX.recoverInsert(page, logInfo.raw, logInfo.offset);
        } finally {
            page.release();
        }
    }


    // [LogType] [XID] [UID] [OldRaw] [NewRaw]

    private static final int OF_UPDATE_UID = OF_XID + 8;
    private static final int OF_UPDATE_RAW = OF_UPDATE_UID + 8;

    // 解析更新
    private static UpdateLogInfo parseUpdateLog(byte[] log) {
        UpdateLogInfo logInfo = new UpdateLogInfo();
        logInfo.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_UPDATE_UID));
        long uid = Parser.parseLong(Arrays.copyOfRange(log, OF_UPDATE_UID, OF_UPDATE_RAW));
        logInfo.offset = (short) (uid & ((1L << 16) - 1));
        uid >>>= 32;
        logInfo.pageNo = (int) (uid & ((1L << 32) - 1));
        int length = (log.length - OF_UPDATE_RAW) / 2;
        logInfo.oldRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW, OF_UPDATE_RAW + length);
        logInfo.newRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW + length, OF_UPDATE_RAW + length * 2);
        return logInfo;
    }

    private static void doUpdateLog(PageCache pageCache, byte[] log, int flag) {
        UpdateLogInfo logInfo = parseUpdateLog(log);
        int pageNo = logInfo.pageNo;
        short offset = logInfo.offset;
        byte[] raw;
        if (flag == REDO) {
            raw = logInfo.newRaw;
        } else {
            raw = logInfo.oldRaw;
        }
        Page page = null;
        try {
            page = pageCache.getPage(pageNo);
        } catch (Exception e) {
            Panic.panic(e);
        }
        try {
            PageX.recoverUpdate(page, raw, offset);
        } finally {
            page.release();
        }
    }

}
