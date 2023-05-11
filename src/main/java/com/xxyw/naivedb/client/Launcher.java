package com.xxyw.naivedb.client;

import com.xxyw.naivedb.transport.Encoder;
import com.xxyw.naivedb.transport.Packager;
import com.xxyw.naivedb.transport.Transporter;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

/**
 * @author Youjing Ju
 * @create 2023-05-09 19:42
 */

/*
客户端的启动入口
 */
public class Launcher {
    public static void main(String[] args) throws UnknownHostException, IOException {
        Socket socket = new Socket("127.0.0.1", 9999);
        Encoder e = new Encoder();
        Transporter t = new Transporter(socket);
        Packager packager = new Packager(t, e);

        Client client = new Client(packager);
        Shell shell = new Shell(client);
        shell.run();
    }
}
