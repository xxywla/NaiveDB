package com.xxyw.naivedb.transport;

/**
 * @author Youjing Ju
 * @create 2023-05-09 19:44
 */
public class Package {
    byte[] data;
    Exception err;

    public Package(byte[] data, Exception err) {
        this.data = data;
        this.err = err;
    }

    public byte[] getData() {
        return data;
    }

    public Exception getErr() {
        return err;
    }
}
