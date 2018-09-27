package com.donews.minio;

import io.minio.MinioClient;
import io.minio.errors.MinioException;
import org.xmlpull.v1.XmlPullParserException;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

public class GetObject {
    public static void main(String[] args)
            throws IOException, NoSuchAlgorithmException, InvalidKeyException, XmlPullParserException {
        try {

            MinioClient minioClient = new MinioClient("http://minio.xy.huijitrans.com", "2KF2PSf8fa",
                    "K6KG5CmrjrHB9LVu");


            minioClient.getObject("fingers", "/Android/10000086/e2808e22280bf2c4ce73ca0530e0ec65/47f98ed02e168866769c959dc262dffc/20180922082315.txt", "20180922082315.txt");

        } catch (MinioException e) {
            System.out.println("Error occurred: " + e);
        }
    }
}
