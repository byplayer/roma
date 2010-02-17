package jp.co.rakuten.rit.roma.storage;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class BasicStorage {
    private DataStoreFactory dsFactory = new DataStoreFactory();

    private LogicalClockFactory lcFactory = new LogicalClockFactory();

    private DataEntryFactory deFactory = new DataEntryFactory();

    private String fileExtensionName;

    private String storagePathName;

    private int divisionNumber;

    private long[] virtualNodeIDs;

    private String option;

    private Map<Long, Integer> virtualNodeIDMap;

    private DataStore[] dataStores;

    public BasicStorage() {
        setFileExtensionName("db");
        setStoragePathName("./");
        setVirtualNodeIDs(new long[0]);
        setDivisionNumber(10);
        setOption("");
    }

    public void setDataStoreFactory(DataStoreFactory factory) {
        dsFactory = factory;
    }

    public void setLogicalClockFactory(LogicalClockFactory factory) {
        lcFactory = factory;
    }

    public void setDataEntryFactory(DataEntryFactory factory) {
        deFactory = factory;
    }

    public void setFileExtensionName(String name) {
        fileExtensionName = name;
    }

    public String getFileExtensionName() {
        return fileExtensionName;
    }

    public void setStoragePathName(String pathName) {
        storagePathName = pathName;
    }

    public String getStoragePathName() {
        return storagePathName;
    }

    public DataStore[] getDataStores() {
        return dataStores;
    }

    public void setDataStores(DataStore[] dataStores) {
        this.dataStores = dataStores;
    }

    public void setVirtualNodeIDs(long[] virtualNodeIDs) {
        this.virtualNodeIDs = virtualNodeIDs;
    }

    public long[] getVirtualNodeIDs() {
        return virtualNodeIDs;
    }

    public void setDivisionNumber(int divnum) {
        divisionNumber = divnum;

    }

    public int getDivisionNumber() {
        return divisionNumber;
    }

    public void setOption(String option) {
        this.option = option;
    }

    public String getOption() {
        return option;
    }

    public boolean isFileBaseStorage() {
        if (dataStores != null) {
            return dataStores[0].isFileBaseDataStore();
        } else {
            return false;
        }
    }

    public void open() throws StorageException {
        createVirtualNodeIDMap();
        createDataStores();
    }

    protected void createVirtualNodeIDMap() {
        virtualNodeIDMap = new HashMap<Long, Integer>();
        Random random;
        try {
            random = SecureRandom.getInstance("SHA1");
        } catch (NoSuchAlgorithmException e) {
            random = new Random();
        }
        for (long vnID : virtualNodeIDs) {
            random.setSeed(vnID);
            int index = random.nextInt(divisionNumber);
            virtualNodeIDMap.put(vnID, index);
        }
    }

    protected void createDataStores() throws StorageException {
        dataStores = new DataStore[divisionNumber];
        for (int i = 0; i < dataStores.length; ++i) {
            dataStores[i] = dsFactory.newDataStore(getStoragePathName(),
                    getFileExtensionName(), getOption(), deFactory, lcFactory);
            dataStores[i].open();
        }
    }

    protected DataStore getDataStoreFromVNodeID(long virtualNodeID) {
        Integer i = virtualNodeIDMap.get(virtualNodeID);
        if (i == null) {
            return null;
        } else {
            return getDataStoreFromIndex(i);
        }
    }

    protected DataStore getDataStoreFromIndex(int index) {
        if (0 <= index && index < dataStores.length) {
            return dataStores[index];
        } else {
            return null;
        }
    }

    public void close() throws StorageException {
        for (int i = 0; i < divisionNumber; ++i) {
            DataStore ds = dataStores[i];
            ds.close();
        }
    }

    public DataEntry createDataEntry(String key, long vnodeID, long pClock,
            long lClock, long expire, byte[] value) {
        return deFactory.newDataEntry(key, vnodeID, pClock, lcFactory
                .newLogicalClock(lClock), expire, value);
    }

    public int compareLogicalClock(long lc1, long lc2) {
        LogicalClock lc1obj = lcFactory.newLogicalClock(lc1);
        LogicalClock lc2obj = lcFactory.newLogicalClock(lc2);
        return lc1obj.compareTo(lc2obj);
    }

    public DataEntry getDataEntry(DataEntry entry) throws StorageException {
        DataStore ds = getDataStoreFromVNodeID(entry.getVNodeID());
        if (ds == null) {
            throw new StorageException(
                    "Not found a data store specified by vnode: "
                            + entry.getVNodeID());
        }
        return ds.get(entry.getKey());
    }

    public DataEntry execSetCommand(DataEntry entry) throws StorageException {
        DataStore ds = getDataStoreFromVNodeID(entry.getVNodeID());
        if (ds == null) {
            throw new StorageException(
                    "Not found a data store specified by vnode: "
                            + entry.getVNodeID());
        }
        DataEntry prev = ds.get(entry.getKey());
        LogicalClock lclock;
        if (prev != null) {
            lclock = prev.getLClock();
            lclock.incr();
        } else {
            lclock = lcFactory.newLogicalClock(0L);
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

    public DataEntry execGetCommand(DataEntry entry) throws StorageException {
        DataStore ds = getDataStoreFromVNodeID(entry.getVNodeID());
        if (ds == null) {
            throw new StorageException(
                    "Not found a data store specified by vnode: "
                            + entry.getVNodeID());
        }
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

    private static void sleepSilently(long t) {
        if (t > 0) {
            try {
                Thread.sleep(t);
            } catch (InterruptedException e) {
            }
        }
    }

    public static void main(String[] args) {
        System.out.println(Long.MAX_VALUE);
        System.out.println(Integer.MAX_VALUE);
    }
}
