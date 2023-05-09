package com.xxyw.naivedb.client;


import com.xxyw.naivedb.transport.Package;
import com.xxyw.naivedb.transport.Packager;

/**
 * @author Youjing Ju
 * @create 2023-05-09 19:41
 */
public class Client {
    private RoundTripper rt;

    public Client(Packager packager) {
        this.rt = new RoundTripper(packager);
    }

    public byte[] execute(byte[] stat) throws Exception {
        Package pkg = new Package(stat, null);
        Package resPkg = rt.roundTrip(pkg);
        if (resPkg.getErr() != null) {
            throw resPkg.getErr();
        }
        return resPkg.getData();
    }

    public void close() {
        try {
            rt.close();
        } catch (Exception e) {
        }
    }
}
