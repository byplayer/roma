package jp.co.rakuten.rit.roma.storage;

import java.math.BigInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BasicStorage extends AbstractStorage {
    private static final Logger LOG = LoggerFactory
            .getLogger(BasicStorage.class);

    private double vnodeDumpSleepTime = 0.001;

    private int vnodeDumpSleepCount = 100;

    private double vnodeCleanupSleepTime = 0.01;

    private long logicalClockExpireTime = 300;

    public BasicStorage() {
        setStorageNameAndPath("./");
        setDivisionNumber(10);
        setOption("");
        setDataStoreFactory(new DataStoreFactory());
        setLogicalClockFactory(new LogicalClockFactory());
        setDataEntryFactory(new DataEntryFactory());
    }

    public double getVnodeDumpSleepTime() {
        return vnodeDumpSleepTime;
    }

    public int getVnodeDumpSleepCount() {
        return vnodeDumpSleepCount;
    }

    public double getVnodeCleanupSleepTime() {
        return vnodeCleanupSleepTime;
    }

    public long getLogicalClockExpireTime() {
        return logicalClockExpireTime;
    }

    public void open() throws StorageException {
        createStoragePath();
        createVirtualNodeIDMap();
        createDataStores();
    }

    public void close() throws StorageException {
        for (int i = 0; i < getDivisionNumber(); ++i) {
            DataStore ds = getDataStoreFromIndex(i);
            ds.close();
        }
    }

    public DataEntry execSetCommand(DataEntry entry) throws StorageException {
        DataStore ds = getDataStoreFromVNodeID(entry.getVNodeID());
        DataEntry prev = ds.get(entry.getKey());
        LogicalClock lclock;
        if (prev != null) {
            lclock = prev.getLClock();
            lclock.incr();
        } else {
            lclock = getLogicalClockFactory().newLogicalClock(0L);
        }

        entry.setCurrentPClock();
        entry.setLClock(lclock);
        DataEntry ret = ds.put(entry.getKey(), entry);
        if (ret != null) {
            return entry;
        } else {
            return null;
        }
    }

    public DataEntry execRSetCommand(DataEntry entry) throws StorageException {
        DataStore ds = getDataStoreFromVNodeID(entry.getVNodeID());
        DataEntry prev = ds.get(entry.getKey());
        long t = DataEntry.getNow();
        if (prev != null) {
            if ((t - prev.getPClock() < getLogicalClockExpireTime())
                    && (compare(entry.getLClock(), prev.getLClock()) <= 0)) {
                return null;
            }
        }

        entry.setPClock(t);
        DataEntry ret = ds.put(entry.getKey(), entry);
        if (ret != null) {
            return entry;
        } else {
            return null;
        }
    }

    public DataEntry execAddCommand(DataEntry entry) throws StorageException {
        DataStore ds = getDataStoreFromVNodeID(entry.getVNodeID());
        DataEntry prev = ds.get(entry.getKey());
        if (prev != null) {
            long t = DataEntry.getNow();
            if (t <= prev.getExpire()) {
                return null;
            }
            entry.setLClock(prev.getLClock().incr());
        }

        // not exist
        DataEntry ret = ds.put(entry.getKey(), entry);
        if (ret != null) {
            return entry;
        } else {
            return null;
        }
    }

    public DataEntry execReplaceCommand(DataEntry entry)
            throws StorageException {
        DataStore ds = getDataStoreFromVNodeID(entry.getVNodeID());
        DataEntry prev = ds.get(entry.getKey());
        if (prev == null) {
            return null;
        }

        // if buf is not null, then ...
        long t = DataEntry.getNow();
        if (t > prev.getExpire()) {
            return null;
        }
        entry.setLClock(prev.getLClock().incr());

        DataEntry ret = ds.put(entry.getKey(), entry);
        if (ret != null) {
            return entry;
        } else {
            return null;
        }
    }

    public DataEntry execAppendCommand(DataEntry entry) throws StorageException {
        DataStore ds = getDataStoreFromVNodeID(entry.getVNodeID());
        DataEntry prev = ds.get(entry.getKey());
        if (prev == null) {
            return null;
        }

        // if buf is not null
        long t = DataEntry.getNow();
        if (t > prev.getExpire()) {
            return null;
        }
        entry.setLClock(prev.getLClock().incr());
        byte[] b = appendValues(prev.getData(), entry.getData());
        entry.setData(b);
        DataEntry ret = ds.put(entry.getKey(), entry);
        if (ret != null) {
            return entry;
        } else {
            return null;
        }
    }

    public DataEntry execPrependCommand(DataEntry entry)
            throws StorageException {
        DataStore ds = getDataStoreFromVNodeID(entry.getVNodeID());
        DataEntry prev = ds.get(entry.getKey());
        if (prev == null) {
            return null;
        }

        // if buf is not null
        long t = DataEntry.getNow();
        if (t > prev.getExpire()) {
            return null;
        }
        entry.setLClock(prev.getLClock().incr());
        byte[] b = appendValues(entry.getData(), prev.getData());
        entry.setData(b);
        DataEntry ret = ds.put(entry.getKey(), entry);
        if (ret != null) {
            return entry;
        } else {
            return null;
        }
    }

    public DataEntry execGetCommand(DataEntry entry) throws StorageException {
        DataStore ds = getDataStoreFromVNodeID(entry.getVNodeID());
        DataEntry prev = ds.get(entry.getKey());
        if (prev != null) {
            if (!prev.isExpired()) {
                return prev;
            } else {
                return null;
            }
        } else {
            return null;
        }
    }

    public DataEntry execDeleteCommand(DataEntry entry) throws StorageException {
        DataStore ds = getDataStoreFromVNodeID(entry.getVNodeID());
        DataEntry prev = ds.get(entry.getKey());
        if (prev != null) {
            if (prev.getExpire() == 0) {
                prev.setData(null);
                return prev;
            }
            entry.setLClock(prev.getLClock().incr());
            if (prev.getData() != null && prev.getData().length != 0
                    && DataEntry.getNow() <= prev.getExpire()) {
                entry.setData(prev.getData());
            } else {
                entry.setData(new byte[0]);
            }
        }
        entry.setPClock(DataEntry.getNow());
        DataEntry ret = ds.put(entry.getKey(), entry);
        if (ret != null) {
            return entry;
        } else {
            return null;
        }
    }

    public DataEntry execRDeleteCommand(DataEntry entry)
            throws StorageException {
        DataStore ds = getDataStoreFromVNodeID(entry.getVNodeID());
        DataEntry prev = ds.get(entry.getKey());
        long t = DataEntry.getNow();
        if (prev != null) {
            if ((t - prev.getPClock() < getLogicalClockExpireTime())
                    && (compare(entry.getLClock(), prev.getLClock()) <= 0)) {
                return null;
            }
        }

        entry.setPClock(t);
        DataEntry ret = ds.put(entry.getKey(), entry);
        if (ret != null) {
            return entry;
        } else {
            return null;
        }
    }

    public DataEntry execOutCommand(DataEntry entry) throws StorageException {
        DataStore ds = getDataStoreFromVNodeID(entry.getVNodeID());
        return ds.remove(entry.getKey());
    }

    private static BigInteger MASK_INTEGER = (new BigInteger("2").pow(64))
            .subtract(new BigInteger("1"));

    public DataEntry execIncrCommand(DataEntry entry) throws StorageException {
        DataStore ds = getDataStoreFromVNodeID(entry.getVNodeID());
        DataEntry prev = ds.get(entry.getKey());
        if (prev == null) {
            return null;
        }

        // if buf is not null
        long t = DataEntry.getNow();
        if (t > prev.getExpire()) {
            return null;
        }
        entry.setLClock(prev.getLClock().incr());
        entry.setPClock(t);
        entry.setExpire(prev.getExpire());
        BigInteger v2 = new BigInteger(new String(prev.getData()));
        BigInteger v = new BigInteger(new String(entry.getData()));
        v = v2.add(v);
        if (v.signum() == -1) {
            v = BigInteger.ZERO;
        }
        v = v.and(MASK_INTEGER);
        entry.setData(v.toString().getBytes());
        DataEntry ret = ds.put(entry.getKey(), entry);
        if (ret != null) {
            return entry;
        } else {
            return null;
        }
    }

    public DataEntry execDecrCommand(DataEntry entry) throws StorageException {
        DataStore ds = getDataStoreFromVNodeID(entry.getVNodeID());
        DataEntry prev = ds.get(entry.getKey());
        if (prev == null) {
            return null;
        }

        // if buf is not null
        long t = DataEntry.getNow();
        if (t > prev.getExpire()) {
            return null;
        }
        entry.setLClock(prev.getLClock().incr());
        entry.setPClock(t);
        entry.setExpire(prev.getExpire());
        BigInteger v2 = new BigInteger(new String(prev.getData()));
        BigInteger v = new BigInteger(new String(entry.getData()));
        v = v2.subtract(v);
        if (v.signum() == -1) {
            v = BigInteger.ZERO;
        }
        v = v.and(MASK_INTEGER);
        entry.setData(v.toString().getBytes());
        DataEntry ret = ds.put(entry.getKey(), entry);
        if (ret != null) {
            return entry;
        } else {
            return null;
        }
    }

    public static byte[] appendValues(byte[] left, byte[] right) {
        int len = left.length + right.length;
        byte[] ret = new byte[len];
        System.arraycopy(left, 0, ret, 0, left.length);
        System.arraycopy(right, 0, ret, left.length, right.length);
        return ret;
    }

    public static void sleepSilently(long t) {
        if (t > 0) {
            try {
                Thread.sleep(t);
            } catch (InterruptedException e) {
            }
        }
    }
}