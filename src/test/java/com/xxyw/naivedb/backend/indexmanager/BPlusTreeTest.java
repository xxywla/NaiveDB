package com.xxyw.naivedb.backend.indexmanager;

import com.xxyw.naivedb.backend.datamanager.DataManager;
import com.xxyw.naivedb.backend.datamanager.pagecache.PageCache;
import com.xxyw.naivedb.backend.transaction.MockTransactionManager;
import com.xxyw.naivedb.backend.transaction.TransactionManager;
import org.junit.Test;

import java.io.File;
import java.util.List;

/**
 * @author Youjing Ju
 * @create 2023-05-09 16:37
 */
public class BPlusTreeTest {
    @Test
    public void testTreeSingle() throws Exception {
        TransactionManager tm = new MockTransactionManager();
        DataManager dm = DataManager.create("/tmp/TestTreeSingle", PageCache.PAGE_SIZE * 10, tm);

        long root = BPlusTree.create(dm);
        BPlusTree tree = BPlusTree.load(root, dm);

        int lim = 100;
        for (int i = lim - 1; i >= 0; i--) {
            tree.insert(i, i);
        }

        for (int i = 0; i < lim; i++) {
            List<Long> uids = tree.search(i);
            assert uids.size() == 1;
            assert uids.get(0) == i;
        }

        tree.close();
        dm.close();
        tm.close();

        assert new File("/tmp/TestTreeSingle.db").delete();
        assert new File("/tmp/TestTreeSingle.log").delete();
    }
}
