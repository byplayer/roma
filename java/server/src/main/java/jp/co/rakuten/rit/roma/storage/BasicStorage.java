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

    // TODO

    private long dumpSleepIgnoreCount;

    private long dumpSleepTime;

    public BasicStorage() {
        setFileExtensionName("db");
        setStoragePathName("./");
        setVirtualNodeIDs(new long[0]);
        setDivisionNumber(10);
        setOption("");

        this.dumpSleepIgnoreCount = 100L;
        this.dumpSleepTime = 1L;
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
        //        
        // /* create storage directory */
        // if (storagePath != null && !storagePath.isDirectory()
        // && !storagePath.mkdirs()) {
        // throw new StorageException("Can not create storage directory: "
        // + storagePath);
        // }
        //
        // /* open real node (HashDB instance) */
        // String dirName = storagePath != null ? storagePath.getAbsolutePath()
        // + "/" : "";
        // realNodes = new HashDB[divisionNumber];
        // for (int i = 0; i < divisionNumber; i++) {
        // realNodes[i] = openNode(dirName + i + "." + extensionName);
        // }
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

    protected void createDataStores() {
        dataStores = new DataStore[divisionNumber];
        for (int i = 0; i < dataStores.length; ++i) {
            dataStores[i] = dsFactory.newDataStore(getStoragePathName(),
                    getFileExtensionName());
            dataStores[i].open();
        }
    }

    protected DataStore getDataStore(long virtualNodeID) {
        Integer i = virtualNodeIDMap.get(virtualNodeID);
        if (i == null) {
            return null;
        } else {
            return dataStores[i];
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
        return deFactory.newDataEntry(key, vnodeID, pClock,
                lcFactory.newLogicalClock(lClock), expire, value);
    }

    public int compareLogicalClock(long lc1, long lc2) {
        LogicalClock lc1obj = lcFactory.newLogicalClock(lc1);
        LogicalClock lc2obj = lcFactory.newLogicalClock(lc2);
        return lc1obj.compareTo(lc2obj);
    }
    
    public byte[] getRowData(DataEntry entry) throws StorageException {
        DataStore ds = getDataStore(entry.getVNodeID());
        if (ds == null) {
            throw new StorageException(
                    "Not found a data store specified by vnode: "
                            + entry.getVNodeID());
        }
        return ds.get(entry.getKey());
    }

    public DataEntry getDataEntry(DataEntry entry) throws StorageException {
        byte[] rawData = getRowData(entry);
        if (rawData != null) {
            return DataEntry.toDataEntry(deFactory, entry.getKey(), rawData, lcFactory);
        } else {
            return null;
        }
    }

    public DataEntry execSetCommand(DataEntry entry) throws StorageException {
        DataStore ds = getDataStore(entry.getVNodeID());
        if (ds == null) {
            throw new StorageException(
                    "Not found a data store specified by vnode: "
                            + entry.getVNodeID());
        }
        byte[] rawData = ds.get(entry.getKey());
        LogicalClock lclock;
        if (rawData != null) {
            DataEntry prev = DataEntry.toDataEntry(deFactory, 
                    entry.getKey(), rawData, lcFactory);
            lclock = prev.getLClock();
            lclock.incr();
        } else {
            lclock = lcFactory.newLogicalClock(0L);
        }

        entry.setCurrentPClock();
        entry.setLClock(lclock);
        if (ds.put(entry.getKey(), DataEntry.toByteArray(entry))) {
            return entry;
        } else {
            return null;
        }
    }

    public DataEntry execGetCommand(DataEntry entry) throws StorageException {
        DataStore node = getDataStore(entry.getVNodeID());
        DataStore ds = getDataStore(entry.getVNodeID());
        if (ds == null) {
            throw new StorageException(
                    "Not found a data store specified by vnode: "
                            + entry.getVNodeID());
        }
        byte[] rawData = ds.get(entry.getKey());
        if (rawData != null) {
            DataEntry prev = DataEntry.toDataEntry(deFactory, 
                    entry.getKey(), rawData, lcFactory);
            if (!prev.isExpired()) {
                return prev;
            } else {
                return null;
            }
        } else {
            return null;
        }
    }
    
//    public DataEntry add(DataEntry entry) throws StorageException {
//        DataStore node = getDataStore(entry.getVNodeID());
//
//        /* get old entry */
//        long logicalClock = 0L;
//        byte[] entryData = node.get(entry.getKey());
//        if (entryData != null) {
//            DataEntry oldEntry = new DataEntry();
//            oldEntry.loadEntryData(entryData);
//
//            if (!oldEntry.isExpired()) {
//                return null;
//            }
//            logicalClock++;
//        }
//
//        /* regist new entry */
//        entry.setLClock(logicalClock);
//        entry.setCurrentPClock();
//        if (!node.put(entry.getKey(), entry.getEntryData())) {
//            return null;
//        }
//        return entry;
//    }
//
//    public DataEntry replace(DataEntry entry) throws StorageException {
//        DataStore node = getDataStore(entry.getVNodeID());
//
//        /* get old entry */
//        byte[] entryData = node.get(entry.getKey());
//        if (entryData == null) {
//            return null;
//        }
//
//        /* load entry data */
//        DataEntry oldEntry = new DataEntry();
//        oldEntry.loadEntryData(entryData);
//        if (oldEntry.isExpired()) {
//            return null;
//        }
//
//        /* regist new entry */
//        entry.setLClock(oldEntry.getLClock() + 1);
//        entry.setCurrentPClock();
//        if (!node.put(entry.getKey(), entry.getEntryData())) {
//            return null;
//        }
//        return entry;
//    }
//
//    public DataEntry append(DataEntry entry) throws StorageException {
//        DataStore node = getDataStore(entry.getVNodeID());
//
//        /* get old entry */
//        byte[] entryData = node.get(entry.getKey());
//        if (entryData == null) {
//            return null;
//        }
//
//        /* load entry data */
//        DataEntry oldEntry = new DataEntry();
//        oldEntry.loadEntryData(entryData);
//        if (oldEntry.isExpired()) {
//            return null;
//        }
//
//        /* append entry */
//        entry.setLClock(oldEntry.getLClock() + 1);
//        entry.setCurrentPClock();
//        entry.prependData(oldEntry.getValue());
//        if (!node.put(entry.getKey(), entry.getEntryData())) {
//            return null;
//        }
//        return entry;
//    }
//
//    public DataEntry prepend(DataEntry entry) throws StorageException {
//        DataStore node = getDataStore(entry.getVNodeID());
//
//        /* get old entry */
//        byte[] entryData = node.get(entry.getKey());
//        if (entryData == null) {
//            return null;
//        }
//
//        /* load entry data */
//        DataEntry oldEntry = new DataEntry();
//        oldEntry.loadEntryData(entryData);
//        if (oldEntry.isExpired()) {
//            return null;
//        }
//
//        /* prepend entry */
//        entry.setLClock(oldEntry.getLClock() + 1);
//        entry.setCurrentPClock();
//        entry.appendData(oldEntry.getValue());
//        if (!node.put(entry.getKey(), entry.getEntryData())) {
//            return null;
//        }
//        return entry;
//    }
//
//    public DataEntry delete(DataEntry entry) throws StorageException {
//        DataStore node = getDataStore(entry.getVNodeID());
//
//        /* get old entry */
//        long logicalClock = 0L;
//        byte[] data = new byte[0];
//        byte[] entryData = node.get(entry.getKey());
//        if (entryData != null) {
//            DataEntry oldEntry = new DataEntry();
//            oldEntry.loadEntryData(entryData);
//            if (!oldEntry.isExpired()) {
//                data = oldEntry.getValue();
//            }
//
//            logicalClock++;
//        }
//
//        /* regist new entry */
//        entry.setCurrentPClock();
//        entry.setLClock(logicalClock);
//        entry.setExpireTime(0L);
//        entry.setValue(data);
//        if (!node.put(entry.getKey(), entry.getEntryData())) {
//            return null;
//        }
//        return entry;
//    }
//
//    public DataEntry out(DataEntry entry) throws StorageException {
//        DataStore node = getDataStore(entry.getVNodeID());
//
//        /* remove entry */
//        return node.remove(entry.getKey()) ? entry : null;
//    }
//
//    public void load(DataInput in) throws StorageException, IOException {
//    }
//
//    public byte[] dumpVirtualNode(long virtualNodeId) throws StorageException,
//            IOException {
//        /* create byte array */
//        ByteArrayOutputStream barray = new ByteArrayOutputStream();
//
//        /* dump to byte array */
//        DataOutputStream out = new DataOutputStream(barray);
//        dumpVirtualNode(out, virtualNodeId);
//        out.close();
//
//        return barray.toByteArray();
//    }
//
//    public void dumpVirtualNode(DataOutput out, long virtualNodeId)
//            throws StorageException, IOException {
//        DataStore node = getDataStore(virtualNodeId);
//
//        /* dump virtual node to byte array */
//        Iterator<byte[]> keyItr = node.keyIterator();
//        long ignoreCount = 0L;
//        while (keyItr.hasNext()) {
//            /* load entry */
//            byte[] key = keyItr.next();
//            byte[] entryData = node.get(key);
//
//            DataEntry entry = new DataEntry();
//            entry.setKey(key);
//            entry.loadEntryData(entryData);
//
//            /* dump entry */
//            if (entry.getVNodeID() == virtualNodeId && !entry.isExpired()) {
//                entry.dump(out);
//                sleep(dumpSleepTime);
//            } else if (++ignoreCount % dumpSleepIgnoreCount == 0) {
//                sleep(dumpSleepTime);
//            }
//        }
//    }
//
//    public void dump(File outputDir) throws StorageException, IOException {
//        dump(outputDir, null);
//    }
//
//    public void dump(File outputDir, long[] exceptVirtualNodeIds)
//            throws StorageException, IOException {
//        /* create output directory */
//        if (!outputDir.isDirectory() && !outputDir.mkdirs()) {
//            throw new StorageException("Can not create output directory: "
//                    + outputDir);
//        }
//
//        /* prepare except virtual node ids */
//        Set<Long> exceptIdSet = new HashSet<Long>();
//        for (long virtualNodeId : exceptVirtualNodeIds) {
//            exceptIdSet.add(virtualNodeId);
//        }
//
//        /* each real nodes */
//        for (int i = 0; i < divisionNumber; ++i) {
//            DataStore node = dataStores[i];
//
//            /* dump real node to file */
//            File dumpFile = new File(outputDir.getAbsolutePath() + "/" + i
//                    + ".dump");
//            DataOutputStream out = new DataOutputStream(new FileOutputStream(
//                    dumpFile));
//
//            long ignoreCount = 0L;
//            Iterator<byte[]> keyItr = node.keyIterator();
//            while (keyItr.hasNext()) {
//                /* load entry */
//                byte[] key = keyItr.next();
//                byte[] entryData = node.get(key);
//
//                DataEntry entry = new DataEntry();
//                entry.setKey(key);
//                entry.loadEntryData(entryData);
//
//                /* dump entry */
//                if (!exceptIdSet.contains(entry.getVNodeID())
//                        && !entry.isExpired()) {
//                    entry.dump(out);
//                    sleep(dumpSleepTime);
//                } else if (++ignoreCount % dumpSleepIgnoreCount == 0) {
//                    sleep(dumpSleepTime);
//                }
//            }
//
//            out.close();
//        }
//
//        /* write time at end of dump to eod file */
//        File eodFile = new File(outputDir.getAbsoluteFile() + "/eod");
//        try {
//            BufferedWriter out = new BufferedWriter(new FileWriter(eodFile));
//            out.write(new Date().toString());
//            out.close();
//        } catch (IOException e) {
//            throw new StorageException("Can not write end of dump: "
//                    + eodFile.getAbsolutePath(), e);
//        }
//    }
//
//    private static void sleep(long milliSecond) {
//        if (milliSecond > 0) {
//            try {
//                Thread.sleep(milliSecond);
//            } catch (InterruptedException e) {
//            }
//        }
//    }
}
