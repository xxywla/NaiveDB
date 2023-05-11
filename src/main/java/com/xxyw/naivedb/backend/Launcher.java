package com.xxyw.naivedb.backend;

import com.xxyw.naivedb.backend.datamanager.DataManager;
import com.xxyw.naivedb.backend.server.Server;
import com.xxyw.naivedb.backend.tablemanager.TableManager;
import com.xxyw.naivedb.backend.transaction.TransactionManager;
import com.xxyw.naivedb.backend.utils.Panic;
import com.xxyw.naivedb.backend.versionmanager.VersionManager;
import com.xxyw.naivedb.backend.versionmanager.VersionManagerImpl;
import com.xxyw.naivedb.common.MyError;
import org.apache.commons.cli.*;

/**
 * @author Youjing Ju
 * @create 2023-05-10 19:55
 */

/*
服务器的启动入口 解析命令行参数
参数 -open 或者 -create 来决定是创建数据库文件，还是启动一个已有的数据库
 */
public class Launcher {
    public static final int port = 9999;

    public static final long DEFALUT_MEM = (1 << 20) * 64;
    public static final long KB = 1 << 10;
    public static final long MB = 1 << 20;
    public static final long GB = 1 << 30;

    public static void main(String[] args) throws ParseException {
        Options options = new Options();
        options.addOption("open", true, "-open DBPath");
        options.addOption("create", true, "-create DBPath");
        options.addOption("mem", true, "-mem 64MB");
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        if (cmd.hasOption("open")) {
            openDB(cmd.getOptionValue("open"), parseMem(cmd.getOptionValue("mem")));
            return;
        }
        if (cmd.hasOption("create")) {
            createDB(cmd.getOptionValue("create"));
            return;
        }
        System.out.println("Usage: launcher (open|create) DBPath");
    }

    private static void createDB(String path) {
        TransactionManager tm = TransactionManager.create(path);
        DataManager dm = DataManager.create(path, DEFALUT_MEM, tm);
        VersionManager vm = new VersionManagerImpl(tm, dm);
        TableManager.create(path, vm, dm);
        tm.close();
        dm.close();
    }

    private static void openDB(String path, long mem) {
        TransactionManager tm = TransactionManager.open(path);
        DataManager dm = DataManager.open(path, mem, tm);
        VersionManager vm = new VersionManagerImpl(tm, dm);
        TableManager tbm = TableManager.open(path, vm, dm);
        new Server(port, tbm).start();
    }

    private static long parseMem(String memStr) {
        if (memStr == null || "".equals(memStr)) {
            return DEFALUT_MEM;
        }
        if (memStr.length() < 2) {
            Panic.panic(MyError.InvalidMemException);
        }
        String unit = memStr.substring(memStr.length() - 2);
        long memNum = Long.parseLong(memStr.substring(0, memStr.length() - 2));
        switch (unit) {
            case "KB":
                return memNum * KB;
            case "MB":
                return memNum * MB;
            case "GB":
                return memNum * GB;
            default:
                Panic.panic(MyError.InvalidMemException);
        }
        return DEFALUT_MEM;
    }
}
