package com.xxyw.naivedb.backend.datamanager.pagecache;

import com.xxyw.naivedb.backend.datamanager.page.Page;
import com.xxyw.naivedb.backend.utils.Panic;
import com.xxyw.naivedb.backend.utils.Parser;
import com.xxyw.naivedb.backend.utils.RandomUtil;
import org.junit.Test;

import java.io.File;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PageCacheTest {

    @Test
    public void testPageCache() throws Exception {

        String fileName = "/tmp/page_cache_test0";

        PageCacheImpl pageCache = PageCache.create(fileName, PageCache.PAGE_SIZE * 50);
        for (int i = 0; i < 100; i++) {
            byte[] tmp = new byte[PageCache.PAGE_SIZE];
            tmp[0] = (byte) i;
            int pageNo = pageCache.newPage(tmp);
            Page page = pageCache.getPage(pageNo);
            page.setDirty(true);
            page.release();
        }
        pageCache.close();

        pageCache = PageCache.open(fileName, PageCache.PAGE_SIZE * 50);
        for (int i = 1; i <= 100; i++) {
            Page page = pageCache.getPage(i);
            System.out.println(Parser.parseShort(page.getData()));
            assert page.getData()[0] == (byte) (i - 1);
            page.release();
        }
        pageCache.close();

        assert new File(fileName + ".db").delete();
    }

    @Test
    public void testPageCacheMultiSimple() throws Exception {

        String fileName = "/tmp/page_cache_test1";
        int N = 30;

        PageCache pageCache = PageCache.create(fileName, PageCache.PAGE_SIZE * 50);

        CountDownLatch cdl = new CountDownLatch(N);
        AtomicInteger pageNoCnt = new AtomicInteger(0);

        for (int i = 0; i < N; i++) {
            int finalI = i;
            Runnable r = () -> worker1(finalI, pageCache, pageNoCnt, cdl);
            new Thread(r).start();
        }
        cdl.await();
        pageCache.close();

        assert new File(fileName + ".db").delete();
    }

    private void worker1(int id, PageCache pageCache, AtomicInteger pageNoCnt, CountDownLatch cdl) {
        Random random = new SecureRandom();
        for (int i = 0; i < 100; i++) {
            int op = Math.abs(random.nextInt() % 20);
            if (op == 0) {
                byte[] data = RandomUtil.randomBytes(PageCache.PAGE_SIZE);
                int pageNo = pageCache.newPage(data);
                Page page = null;
                try {
                    page = pageCache.getPage(pageNo);
                } catch (Exception e) {
                    Panic.panic(e);
                }
                pageNoCnt.incrementAndGet();
                page.release();
            } else if (op < 20) {
                int mod = pageNoCnt.intValue();
                if (mod == 0) {
                    continue;
                }
                int pageNo = Math.abs(random.nextInt()) % mod + 1;
                Page page = null;
                try {
                    page = pageCache.getPage(pageNo);
                } catch (Exception e) {
                    Panic.panic(e);
                }
                page.release();
            }
        }
        cdl.countDown();
    }

    @Test
    public void testPageCacheMulti() throws InterruptedException {

        String fileName = "/tmp/page_cache_test2";
        int N = 30;

        PageCache pageCache = PageCache.create(fileName, PageCache.PAGE_SIZE * 50);
        PageCache mockPageCache = new MockPageCache();

        Lock lock = new ReentrantLock();
        AtomicInteger pageNoCnt = new AtomicInteger(0);
        CountDownLatch cdl = new CountDownLatch(N);

        for (int i = 0; i < 30; i++) {
            int id = i;
            Runnable r = () -> worker2(id, pageCache, mockPageCache, lock, pageNoCnt, cdl);
            new Thread(r).start();
        }
        cdl.await();

        pageCache.close();
        assert new File(fileName + ".db").delete();

    }

    private void worker2(int id, PageCache pageCache, PageCache mockPageCache, Lock lock, AtomicInteger pageNoCnt, CountDownLatch cdl) {
        Random random = new SecureRandom();
        for (int i = 0; i < 1000; i++) {
            int op = Math.abs(random.nextInt() % 20);
            if (op == 0) {
                // 创建新的数据页
                byte[] data = RandomUtil.randomBytes(PageCache.PAGE_SIZE);
                lock.lock();
                int pageNo = pageCache.newPage(data);
                int mPageNo = mockPageCache.newPage(data);
                assert pageNo == mPageNo;
                lock.unlock();
                pageNoCnt.incrementAndGet();
                System.out.println("线程 " + id + " 创建第 " + pageNo + " 页数据");
            } else if (op < 10) {
                // 读取相同的数据页进行数据一致检查
                int mod = pageNoCnt.intValue();
                if (mod == 0) continue;
                int pageNo = Math.abs(random.nextInt()) % mod + 1;
                System.out.println("线程 " + id + " 检查第 " + pageNo + " 页数据");
                Page page1 = null, page2 = null;
                try {
                    page1 = pageCache.getPage(pageNo);
                } catch (Exception e) {
                    Panic.panic(e);
                }
                try {
                    page2 = mockPageCache.getPage(pageNo);
                } catch (Exception e) {
                    Panic.panic(e);
                }
                page1.lock();
                assert Arrays.equals(page1.getData(), page2.getData());
                page1.unlock();
                page1.release();
            } else {
                // 更新数据页的数据
                int mod = pageNoCnt.intValue();
                if (mod == 0) continue;
                int pageNo = Math.abs(random.nextInt() % mod) + 1;
                System.out.println("线程 " + id + " 更新第 " + pageNo + " 页数据");
                Page page1 = null, page2 = null;
                try {
                    page1 = pageCache.getPage(pageNo);
                } catch (Exception e) {
                    Panic.panic(e);
                }
                try {
                    page2 = mockPageCache.getPage(pageNo);
                } catch (Exception e) {
                    Panic.panic(e);
                }
                byte[] newData = RandomUtil.randomBytes(PageCache.PAGE_SIZE);
                page1.lock();
                page1.setDirty(true);
                for (int j = 0; j < PageCache.PAGE_SIZE; j++) {
                    page1.getData()[j] = newData[j];
                }
                for (int j = 0; j < PageCache.PAGE_SIZE; j++) {
                    page2.getData()[j] = newData[j];
                }
                page1.unlock();
                page1.release();
            }
        }
        cdl.countDown();
    }

}
