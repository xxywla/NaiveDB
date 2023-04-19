package com.xxyw.naivedb.backend.datamanager;

import com.xxyw.naivedb.backend.common.SubArray;
import com.xxyw.naivedb.backend.datamanager.dataitem.DataItem;
import com.xxyw.naivedb.backend.datamanager.logger.LoggerImpl;
import com.xxyw.naivedb.backend.datamanager.pagecache.PageCache;
import com.xxyw.naivedb.backend.transaction.MockTransactionManager;
import com.xxyw.naivedb.backend.transaction.TransactionManager;
import com.xxyw.naivedb.backend.utils.Panic;
import com.xxyw.naivedb.backend.utils.RandomUtil;
import org.junit.Test;

import java.io.File;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class DataManagerTest {

    static Random random = new SecureRandom();

    static List<Long> uidList0, uidList1;
    static Lock uidLock;

    private void initUidList() {
        uidList0 = new ArrayList<>();
        uidList1 = new ArrayList<>();
        uidLock = new ReentrantLock();
    }

    private void worker(DataManager dataMan0, DataManager dataMan1, int iterNum, int insertRation, CountDownLatch cdl) {
        int dataLen = 60;
        try {
            for (int i = 0; i < iterNum; i++) {
                int op = Math.abs(random.nextInt()) % 100;
                if (op < insertRation) {
                    byte[] data = RandomUtil.randomBytes(dataLen);
                    long uid0, uid1 = 0;

                    try {
                        uid0 = dataMan0.insert(0, data);
                    } catch (Exception e) {
                        continue;
                    }

                    try {
                        uid1 = dataMan1.insert(0, data);
                    } catch (Exception e) {
                        Panic.panic(e);
                    }

                    uidLock.lock();
                    uidList0.add(uid0);
                    uidList1.add(uid1);
                    uidLock.unlock();

                } else {

                    uidLock.lock();

                    if (uidList0.size() == 0) {
                        uidLock.unlock();
                        continue;
                    }

                    int tmp = Math.abs(random.nextInt()) % uidList0.size();
                    long uid0 = uidList0.get(tmp);
                    long uid1 = uidList1.get(tmp);

                    uidLock.unlock();

                    DataItem dataItem0, dataItem1 = null;
                    try {
                        dataItem0 = dataMan0.read(uid0);
                    } catch (Exception e) {
                        Panic.panic(e);
                        continue;
                    }
                    if (dataItem0 == null) continue;
                    try {
                        dataItem1 = dataMan1.read(uid1);
                    } catch (Exception ignored) {

                    }

                    dataItem0.rLock();
                    dataItem1.rLock();
                    SubArray s0 = dataItem0.data();
                    SubArray s1 = dataItem1.data();
                    assert Arrays.equals(Arrays.copyOfRange(s0.raw, s0.start, s0.end), Arrays.copyOfRange(s1.raw, s1.start, s1.end));
                    dataItem0.rUnLock();
                    dataItem1.rUnLock();

                    //byte[] newData = RandomUtil.randomBytes(dataLen);
                    //dataItem0.before();
                    //dataItem1.before();
                    //System.arraycopy(newData, 0, s0.raw, s0.start, dataLen);
                    //System.arraycopy(newData, 0, s1.raw, s1.start, dataLen);
                    //dataItem0.after(0);
                    //dataItem1.after(0);
                    //dataItem0.release();
                    //dataItem1.release();

                }
            }
        } finally {
            cdl.countDown();
        }
    }

    @Test
    public void testDMSingle() throws Exception {

        String fileName = "/tmp/testDMSingle";

        TransactionManager tranMan = new MockTransactionManager();
        DataManager dataMan = DataManager.create(fileName, PageCache.PAGE_SIZE * 10, tranMan);
        DataManager dataManMock = MockDataManager.newMockDataManager();

        int workerNum = 10000;
        CountDownLatch cdl = new CountDownLatch(1);
        initUidList();
        Runnable r = () -> worker(dataMan, dataManMock, workerNum, 50, cdl);
        new Thread(r).start();
        cdl.await();

        dataMan.close();
        dataManMock.close();

        assert new File(fileName + ".db").delete();
        assert new File(fileName + ".log").delete();

    }

    @Test
    public void testDMMulti() throws Exception {

        String fileName = "/tmp/testDMMulti";

        TransactionManager tranMan = new MockTransactionManager();
        DataManager dataMan = DataManager.create(fileName, PageCache.PAGE_SIZE * 30, tranMan);
        DataManager dataManMock = MockDataManager.newMockDataManager();

        int iterNum = 500;
        int threadNum = 10;
        CountDownLatch cdl = new CountDownLatch(threadNum);
        initUidList();
        for (int i = 0; i < threadNum; i++) {
            Runnable r = () -> worker(dataMan, dataManMock, iterNum, 50, cdl);
            new Thread(r).start();
        }
        cdl.await();

        dataMan.close();
        dataManMock.close();

        assert new File(fileName + ".db").delete();
        assert new File(fileName + ".log").delete();
    }

    @Test
    public void testRecovery() throws Exception {
        String fileName = "/tmp/testRecovery";
        TransactionManager tranMan = new MockTransactionManager();
        DataManager dataMan = DataManager.create(fileName, PageCache.PAGE_SIZE * 30, tranMan);
        dataMan.close();

        int dataLen = 60;
        dataMan = DataManager.open(fileName, PageCache.PAGE_SIZE * 30, tranMan);
        byte[] data = RandomUtil.randomBytes(dataLen);
        dataMan.insert(0, data);
        dataMan.close();

        dataMan = DataManager.open(fileName, PageCache.PAGE_SIZE * 30, tranMan);
        dataMan.insert(0, data);

        DataManagerImpl dm = (DataManagerImpl) dataMan;
        LoggerImpl logger = (LoggerImpl) dm.logger;
        logger.rewind();
        System.out.println(Arrays.toString(data));
        byte[] bytes = logger.next();
        System.out.println(Arrays.toString(bytes));


        dataMan.close();
    }
}
