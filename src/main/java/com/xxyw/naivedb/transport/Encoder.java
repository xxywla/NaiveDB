package com.xxyw.naivedb.transport;

import com.google.common.primitives.Bytes;
import com.xxyw.naivedb.common.MyError;

import java.util.Arrays;

/**
 * @author Youjing Ju
 * @create 2023-05-09 19:43
 */

/*
[Flag][data]
 */
public class Encoder {

    /*
    若 flag 为 0，表示发送的是数据，那么 data 即为这份数据本身；
    如果 flag 为 1，表示发送的是错误，data 是 Exception.getMessage() 的错误提示信息。
     */
    public byte[] encode(Package pkg) {
        if (pkg.getErr() != null) {
            Exception err = pkg.getErr();
            String msg = "Intern server Myerror!";
            if (err.getMessage() != null) {
                msg = err.getMessage();
            }
            return Bytes.concat(new byte[]{1}, msg.getBytes());
        } else {
            return Bytes.concat(new byte[]{0}, pkg.getData());
        }
    }

    public Package decode(byte[] data) throws Exception {
        if (data.length < 1) {
            throw MyError.InvalidPkgDataException;
        }
        if (data[0] == 0) {
            return new Package(Arrays.copyOfRange(data, 1, data.length), null);
        } else if (data[0] == 1) {
            return new Package(null, new RuntimeException(new String(Arrays.copyOfRange(data, 1, data.length))));
        } else {
            throw MyError.InvalidPkgDataException;
        }
    }
}
