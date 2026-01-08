package com.example;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

public class HashUtils {
    public static long hash64(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-1");
            byte[] digest = md.digest(input.getBytes(StandardCharsets.UTF_8));

            // Take the first 8 bytes of the SHA-1 digest
            long value = 0;
            for (int i = 0; i < 8; i++) {
                value = (value << 8) | (digest[i] & 0xff);
            }
            return value & 0x7fffffffffffffffL; // make sure positive
        } catch (Exception e) {
            throw new RuntimeException("Error computing hash", e);
        }
    }

    public static long hashfileId(String fileName) {
        return hash64(fileName);
    }

    public static long hashnodeId(String ip, int port, String version) {
        String timestamp = String.valueOf(System.currentTimeMillis());
        String input = ip + ":" + port + ":" + version;
        return hash64(input);
    }

    //  test
    public static void main(String[] args) {
        System.out.println("File hash: " + hashfileId("hello.txt"));
        System.out.println("Node hash: " + hashnodeId("127.0.0.1", 8080, "v1"));
    }
}
