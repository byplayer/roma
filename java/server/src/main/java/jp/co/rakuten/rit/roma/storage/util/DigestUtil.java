package jp.co.rakuten.rit.roma.storage.util;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Random;

public class DigestUtil {
    public static final int getRandom(int n) {
        Random random = null;
        try {
            random = SecureRandom.getInstance("SHA1");
        } catch (NoSuchAlgorithmException e) {
            random = new Random(0);
        }
        return random.nextInt(n);
    }
}
