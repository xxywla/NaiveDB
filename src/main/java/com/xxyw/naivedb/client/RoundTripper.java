package com.xxyw.naivedb.client;


import com.xxyw.naivedb.transport.Packager;
import com.xxyw.naivedb.transport.Package;

/**
 * @author Youjing Ju
 * @create 2023-05-09 19:42
 */
public class RoundTripper {
    private Packager packager;

    public RoundTripper(Packager packager) {
        this.packager = packager;
    }

    public Package roundTrip(Package pkg) throws Exception {
        packager.send(pkg);
        return packager.receive();
    }

    public void close() throws Exception {
        packager.close();
    }
}
