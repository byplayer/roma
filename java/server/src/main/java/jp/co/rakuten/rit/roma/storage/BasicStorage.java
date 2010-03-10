package jp.co.rakuten.rit.roma.storage;

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

    public DataEntry execIncrCommand(DataEntry entry) throws StorageException {
        // TODO
        throw new UnsupportedOperationException();
    }

    public DataEntry execDecrCommand(DataEntry entry) throws StorageException {
        // TODO
        throw new UnsupportedOperationException();
    }

    // public DataEntry add(DataEntry entry) throws StorageException {
    // DataStore node = getDataStore(entry.getVNodeID());
    //
    // /* get old entry */
    // long logicalClock = 0L;
    // byte[] entryData = node.get(entry.getKey());
    // if (entryData != null) {
    // DataEntry oldEntry = new DataEntry();
    // oldEntry.loadEntryData(entryData);
    //
    // if (!oldEntry.isExpired()) {
    // return null;
    // }
    // logicalClock++;
    // }
    //
    // /* regist new entry */
    // entry.setLClock(logicalClock);
    // entry.setCurrentPClock();
    // if (!node.put(entry.getKey(), entry.getEntryData())) {
    // return null;
    // }
    // return entry;
    // }
    //
    // public DataEntry replace(DataEntry entry) throws StorageException {
    // DataStore node = getDataStore(entry.getVNodeID());
    //
    // /* get old entry */
    // byte[] entryData = node.get(entry.getKey());
    // if (entryData == null) {
    // return null;
    // }
    //
    // /* load entry data */
    // DataEntry oldEntry = new DataEntry();
    // oldEntry.loadEntryData(entryData);
    // if (oldEntry.isExpired()) {
    // return null;
    // }
    //
    // /* regist new entry */
    // entry.setLClock(oldEntry.getLClock() + 1);
    // entry.setCurrentPClock();
    // if (!node.put(entry.getKey(), entry.getEntryData())) {
    // return null;
    // }
    // return entry;
    // }
    //
    // public DataEntry append(DataEntry entry) throws StorageException {
    // DataStore node = getDataStore(entry.getVNodeID());
    //
    // /* get old entry */
    // byte[] entryData = node.get(entry.getKey());
    // if (entryData == null) {
    // return null;
    // }
    //
    // /* load entry data */
    // DataEntry oldEntry = new DataEntry();
    // oldEntry.loadEntryData(entryData);
    // if (oldEntry.isExpired()) {
    // return null;
    // }
    //
    // /* append entry */
    // entry.setLClock(oldEntry.getLClock() + 1);
    // entry.setCurrentPClock();
    // entry.prependData(oldEntry.getValue());
    // if (!node.put(entry.getKey(), entry.getEntryData())) {
    // return null;
    // }
    // return entry;
    // }
    //
    // public DataEntry prepend(DataEntry entry) throws StorageException {
    // DataStore node = getDataStore(entry.getVNodeID());
    //
    // /* get old entry */
    // byte[] entryData = node.get(entry.getKey());
    // if (entryData == null) {
    // return null;
    // }
    //
    // /* load entry data */
    // DataEntry oldEntry = new DataEntry();
    // oldEntry.loadEntryData(entryData);
    // if (oldEntry.isExpired()) {
    // return null;
    // }
    //
    // /* prepend entry */
    // entry.setLClock(oldEntry.getLClock() + 1);
    // entry.setCurrentPClock();
    // entry.appendData(oldEntry.getValue());
    // if (!node.put(entry.getKey(), entry.getEntryData())) {
    // return null;
    // }
    // return entry;
    // }
    //
    // public DataEntry delete(DataEntry entry) throws StorageException {
    // DataStore node = getDataStore(entry.getVNodeID());
    //
    // /* get old entry */
    // long logicalClock = 0L;
    // byte[] data = new byte[0];
    // byte[] entryData = node.get(entry.getKey());
    // if (entryData != null) {
    // DataEntry oldEntry = new DataEntry();
    // oldEntry.loadEntryData(entryData);
    // if (!oldEntry.isExpired()) {
    // data = oldEntry.getValue();
    // }
    //
    // logicalClock++;
    // }
    //
    // /* regist new entry */
    // entry.setCurrentPClock();
    // entry.setLClock(logicalClock);
    // entry.setExpireTime(0L);
    // entry.setValue(data);
    // if (!node.put(entry.getKey(), entry.getEntryData())) {
    // return null;
    // }
    // return entry;
    // }
    //
    // public DataEntry out(DataEntry entry) throws StorageException {
    // DataStore node = getDataStore(entry.getVNodeID());
    //
    // /* remove entry */
    // return node.remove(entry.getKey()) ? entry : null;
    // }
    //
    // public void load(DataInput in) throws StorageException, IOException {
    // }
    //
    // public byte[] dumpVirtualNode(long virtualNodeId) throws
    // StorageException,
    // IOException {
    // /* create byte array */
    // ByteArrayOutputStream barray = new ByteArrayOutputStream();
    //
    // /* dump to byte array */
    // DataOutputStream out = new DataOutputStream(barray);
    // dumpVirtualNode(out, virtualNodeId);
    // out.close();
    //
    // return barray.toByteArray();
    // }

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