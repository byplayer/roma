package jp.co.rakuten.rit.roma.storage;

import junit.framework.TestCase;

public class BasicStoragePerfTest extends TestCase {
    public static int SMALL_LOOP_COUNT = 1000;

    // public static int BIG_LOOP_COUNT = 100;

    public static int SIZE_OF_DATA = 1024;

    public static int NUM_OF_THREADS = 5;

    public static long PERIOD_OF_SLEEP = 1;

    public static long PERIOD_OF_TIMEOUT = 5000;

    BasicStorage STORAGE;

    public BasicStoragePerfTest() {
        super();
    }

    public void setUp() {
        STORAGE = new BasicStorage();
        STORAGE.setStorageNameAndPath("./");
        STORAGE.setDivisionNumber(10);
        STORAGE.setOption("");
        STORAGE.setDataStoreFactory(new DataStoreFactory());
        STORAGE.setLogicalClockFactory(new LogicalClockFactory());
        STORAGE.setDataEntryFactory(new DataEntryFactory());
        STORAGE.setVirtualNodeIDs(new long[] { 0 });
        try {
            STORAGE.openDataStores();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void tearDown() {
        try {
            STORAGE.closeDataStores();
        } catch (Exception e) {
            e.printStackTrace();
        }
        STORAGE = null;
    }

    public void testDummy() {
    }

    public void XtestSet() throws Exception {
        big_loop();
    }

    private void big_loop() throws Exception {
        int count = 0;
        // while (count < BIG_LOOP_COUNT) {
        while (true) {
            small_loop(count);
            count++;
        }
    }

    private void small_loop(int big_count) throws Exception {
        int count = 0;
        int count_threshold = 0;
        int count_threshold1 = 0;
        long count_max = 0;
        long count_min = 100000;
        long time0 = System.currentTimeMillis();
        while (count < SMALL_LOOP_COUNT) {
            try {
                long time = System.currentTimeMillis();
                small_loop0();
                time = System.currentTimeMillis() - time;
                if (time > PERIOD_OF_TIMEOUT) {
                    count_threshold++;
                }
                if (time > count_max) {
                    count_max = time;
                }
                if (time < count_min) {
                    count_min = time;
                }
            } catch (Exception e) {
                count_threshold1++;
                System.out.println(e.getMessage());
                // e.printStackTrace();
            } finally {
                // Thread.sleep(PERIOD_OF_SLEEP);
                count++;
            }
        }
        time0 = System.currentTimeMillis() - time0;

        StringBuilder sb = new StringBuilder();
        sb.append("qps: ").append(
                (int) (((double) (SMALL_LOOP_COUNT * 1000)) / time0)).append(
                " ").append("(timeout count: ").append(count_threshold).append(
                ", ").append(count_threshold1).append(")").append(" max = ")
                .append(count_max / 1000).append(", min = ").append(
                        count_min / 1000);
        System.out.println(sb.toString());
        count_min = 0;
        count_max = 0;
    }

    private void small_loop0() throws Exception {
        int queryID = (int) (Math.random() * 12);
        int index = (int) (Math.random() * SMALL_LOOP_COUNT);

        DataEntry entry = STORAGE.createDataEntry(
                new Integer(index).toString(), 0, DataEntry.getNow(), 0, 0,
                (DUMMY_PREFIX + index).getBytes());
        STORAGE.execSetCommand(entry);
    }

    private static final char A = 'b';

    private static final String DUMMY_PREFIX = makeDummyPrefix();

    private static String makeDummyPrefix() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < SIZE_OF_DATA; ++i) {
            sb.append(A);
        }
        sb.append("::");
        return sb.toString();
    }

    public static void main(final String[] args) throws Exception {
        BasicStoragePerfTest test = new BasicStoragePerfTest();
        test.setUp();
        // test.testSet();
        test.tearDown();
    }
}
