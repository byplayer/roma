package jp.co.rakuten.rit.roma.storage;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public abstract class AbstractStorage {
    private DataStoreFactory dsFactory = new DataStoreFactory();

    private LogicalClockFactory lcFactory = new LogicalClockFactory();

    private DataEntryFactory deFactory = new DataEntryFactory();

    private String rootStoragePathName;

    private String fileExtensionName;

    private int divisionNumber;

    private long[] virtualNodeIDs;

    private long logicalClockExpireTime = 300;

    private String option;

    private Map<Long, Integer> virtualNodeIDMap;

    private DataStore[] dataStores;

    public DataStoreFactory getDataStoreFactory() {
        return dsFactory;
    }

    public void setDataStoreFactory(DataStoreFactory factory) {
        dsFactory = factory;
    }

    public LogicalClockFactory getLogicalClockFactory() {
        return lcFactory;
    }

    public void setLogicalClockFactory(LogicalClockFactory factory) {
        lcFactory = factory;
    }

    public DataEntryFactory getDataEntryFactory() {
        return deFactory;
    }

    public void setDataEntryFactory(DataEntryFactory factory) {
        deFactory = factory;
    }

    public void setFileExtensionName(String name) {
        fileExtensionName = name;
        // for (int i = 0; i < dataStores.length; ++i) {
        // dataStores[i].setFileExtensionName(name);
        // }
    }

    public String getFileExtensionName() {
        return fileExtensionName;
        // return dataStores[0].getFileExtensionName();
    }

    public void setStoragePathName(String name) {
        rootStoragePathName = name;
        // for (int i = 0; i < dataStores.length; ++i) {
        // dataStores[i].setStoragePathName(name);
        // }
    }

    public String getStoragePathName() {
        return rootStoragePathName;
        // return dataStores[0].getStoragePathName();
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

    public abstract void open() throws StorageException;

    public void createVirtualNodeIDMap() {
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

    public void createDataStores() throws StorageException {
        dataStores = new DataStore[getDivisionNumber()];
        for (int i = 0; i < dataStores.length; ++i) {
            String fileName = getStoragePathName() + "/" + i + "."
                    + getFileExtensionName();
            dataStores[i] = dsFactory.newDataStore(fileName,
                    getFileExtensionName(), getOption(), deFactory, lcFactory);
            dataStores[i].open();
        }
    }

    public DataStore getDataStoreFromVNodeID(long virtualNodeID) {
        Integer i = virtualNodeIDMap.get(virtualNodeID);
        if (i == null) {
            return null;
        } else {
            return getDataStoreFromIndex(i);
        }
    }

    public DataStore getDataStoreFromIndex(int index) {
        if (0 <= index && index < dataStores.length) {
            return dataStores[index];
        } else {
            return null;
        }
    }

    public abstract void close() throws StorageException;

    public DataEntry createDataEntry(String key, long vnodeID, long pClock,
            long lClock, long expire, byte[] value) {
        return deFactory.newDataEntry(key, vnodeID, pClock, lcFactory
                .newLogicalClock(lClock), expire, value);
    }

    public int compare(LogicalClock lc1, LogicalClock lc2) {
        return lc1.compareTo(lc2);
    }

    public int compareLogicalClock(long lc1, long lc2) {
        LogicalClock lc1obj = lcFactory.newLogicalClock(lc1);
        LogicalClock lc2obj = lcFactory.newLogicalClock(lc2);
        return lc1obj.compareTo(lc2obj);
    }

    public long getLogicalClockExpireTime() {
        return logicalClockExpireTime;
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
}
