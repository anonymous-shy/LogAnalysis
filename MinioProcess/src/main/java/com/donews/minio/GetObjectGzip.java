package com.donews.minio;

import java.io.*;

public class GetObjectGzip {
    public static void main(String[] args) throws IOException {


        byte[] bytes = inputStream2ByteArray("F:\\donews\\minio2es\\minio-process\\src\\main\\resources\\1.txt");


        int key = "1ea474c9daff3871dc2f71036bded142".getBytes()[0];

        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) (bytes[i] ^ key);
        }
        FileOutputStream fos = new FileOutputStream("F:\\donews\\minio2es\\minio-process\\src\\main\\resources\\1.gz");
        fos.write(bytes);
        //关闭流资源。
        fos.close();
    }

    private static byte[] inputStream2ByteArray(String filePath) throws IOException {

        InputStream in = new FileInputStream(filePath);
        byte[] data = toByteArray(in);
        in.close();

        return data;
    }

    private static byte[] toByteArray(InputStream in) throws IOException {

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024 * 4];
        int n = 0;
        while ((n = in.read(buffer)) != -1) {
            out.write(buffer, 0, n);
        }
        return out.toByteArray();
    }
}
