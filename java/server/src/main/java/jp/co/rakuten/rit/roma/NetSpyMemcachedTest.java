package jp.co.rakuten.rit.roma;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import net.spy.memcached.ConnectionFactory;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.MemcachedClient;

public class NetSpyMemcachedTest {

    private void big_loop(int numOfThreads, final MemcachedClient c) {
        Thread[] threads = new Thread[numOfThreads];
        for (int i = 0; i < threads.length; ++i) {
            threads[i] = new Thread(new Runnable() {
                public void run() {
                    try {
                        if (c == null) {
                            ConnectionFactoryBuilder cfb = new ConnectionFactoryBuilder();
                            cfb.setOpTimeout(3 * 1000L);
                            ConnectionFactory cf = cfb.build();

                            List<InetSocketAddress> addresses = new ArrayList<InetSocketAddress>();
                            addresses.add(new InetSocketAddress("10.160.56.171", 11211));
                            MemcachedClient memc = new MemcachedClient(cf,
                                    addresses);
                            small_loop0(memc);
//                            small_loop(memc);
                            memc.shutdown();
                        } else {
                            small_loop(c);
                            small_loop(c);
                            c.shutdown();
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        }
        for (int i = 0; i < threads.length; ++i) {
            threads[i].start();
        }
    }

    private static String dummy;

    public static String createDummy(int len) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < len; ++i) {
            sb.append('x');
        }
        sb.append('-');
        return sb.toString();
    }

    private void small_loop0(MemcachedClient c) throws Exception {
        String s = Thread.currentThread().toString();
        int count = 10000;
        
        for (int i = 0; i < count; ++i) {
            if (i % 1000 == 0) {
                System.out.println("put count: " + i);
            }
            ConnectionFactoryBuilder cfb = new ConnectionFactoryBuilder();
            cfb.setOpTimeout(3 * 1000L);
            ConnectionFactory cf = cfb.build();

            List<InetSocketAddress> addresses = new ArrayList<InetSocketAddress>();
            addresses.add(new InetSocketAddress("10.160.56.171", 11211));
            MemcachedClient memc = new MemcachedClient(cf,
                    addresses);
            String k = s;
            try {
                Object obj = memc.get(k);
                if (obj != null) {
                    memc.incr(k, 1);
                } else {
                    memc.set(k, 0, 1).get();
                }
            } catch (Exception e) {
                throw e;
            }
            memc.shutdown();
            Thread.sleep(100);
        }
    }

    private void small_loop(MemcachedClient c) throws Exception {
        String s = Thread.currentThread().toString();
        int count = 10000;

        long time1 = System.currentTimeMillis();
        for (int i = 0; i < count; ++i) {
            if (i % 1000 == 0) {
                System.out.println("put count: " + i);
            }
            String k = s + "-k-" + i;
            String v = s + "-v-" + dummy + i;
            try {
                Boolean b = c.set(k, 0, v).get();
            } catch (Exception e) {
                System.out.println("set error key: " + k + ", val: " + v);
                System.out.println("reason: " + e.getMessage());
                throw e;
            }
            // System.out.println("set: k: " + k + ", v: " + v);
            // Thread.sleep(1);
        }
        time1 = System.currentTimeMillis() - time1;
        double t1 = ((double) (count / (double) time1)) * 1000;
        System.out.println("put qps: " + t1);

        long time = System.currentTimeMillis();
        for (int i = 0; i < count; ++i) {
            if (i % 1000 == 0) {
                System.out.println("get count: " + i);
            }
            String k = s + "-k-" + i;
            String v = s + "-v-" + dummy + i;
            String v2 = null;
            try {
                v2 = (String) c.get(k);
            } catch (Exception e) {
                System.out.println("get error key: " + k + ", "
                        + e.getMessage());
                throw e;
            }

            if (!v.equals(v2)) {
                System.out.println("key: " + k + ", v1: " + v + ", v2: " + v2);
            }
            // System.out.println("get: k: " + k);
            // Thread.sleep(1);
        }
        time = System.currentTimeMillis() - time;
        double t = ((double) (count / (double) time)) * 1000;
        System.out.println("get qps: " + t);
    }

    public static void main(String[] args) {
        dummy = createDummy(4);

        NetSpyMemcachedTest t = new NetSpyMemcachedTest();
        MemcachedClient memc = null;
        try {
            ConnectionFactoryBuilder cfb = new ConnectionFactoryBuilder();
            cfb.setOpTimeout(3 * 1000L);
            ConnectionFactory cf = cfb.build();

            List<InetSocketAddress> addresses = new ArrayList<InetSocketAddress>();
            addresses.add(new InetSocketAddress("10.168.56.171", 11211));
            memc = new MemcachedClient(cf, addresses);

            // t.big_loop(1, memc);
            t.big_loop(1, null);

            while (true) {
                Thread.sleep(5 * 1000);
            }
        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
        } finally {
            memc.shutdown();
        }
    }
}
