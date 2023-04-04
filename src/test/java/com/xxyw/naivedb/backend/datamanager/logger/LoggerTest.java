package com.xxyw.naivedb.backend.datamanager.logger;

import org.junit.Test;

import java.io.File;

public class LoggerTest {
    @Test
    public void testLogger() {
        String fileName = "/tmp/logger_test";

        String[] arr = {"aaa", "abc", "ddd"};

        Logger logger = Logger.create(fileName);
        for (String s : arr) {
            logger.log(s.getBytes());
        }
        logger.close();

        logger = Logger.open(fileName);
        logger.rewind();
        for (String s : arr) {
            byte[] log = logger.next();
            assert log != null;
            assert s.equals(new String(log));
        }

        byte[] log = logger.next();
        assert log == null;

        logger.close();

        assert new File(fileName + ".log").delete();
    }
}
