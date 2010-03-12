package jp.co.rakuten.rit.roma.storage;

import java.io.File;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

public abstract class AbstractStorage {
    private DataStoreFactory dsFactory = new DataStoreFactory();

    private LogicalClockFactory lcFactory = new LogicalClockFactory();

    private DataEntryFactory deFactory = new DataEntryFactory();

    private String storageName;

    private String rootStoragePathName;

    private String fileExtensionName;

    private int divisionNumber;

    private long[] virtualNodeIDs;

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

    public void setStorageNameAndPath(String name) {
        int lastIndex = name.lastIndexOf('/');
        setStoragePathName(name.substring(0, lastIndex));
        setStorageName(name.substring(lastIndex + 1, name.length()));
    }

    public void setStorageName(String name) {
        storageName = name;
    }

    public String getStorageName() {
        return storageName;
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

    public void createStoragePath() {
        File storagePathDir = new File(getStoragePathName());
        if (!storagePathDir.exists()) {
            storagePathDir.mkdir();
        }
        File storageDir = new File(storagePathDir, getStorageName());
        if (!storageDir.exists()) {
            storageDir.mkdir();
        }
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
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("SHA1");
        } catch (NoSuchAlgorithmException e) {
            // ignore
        }
        for (long vnodeID : virtualNodeIDs) {
            String vn = "" + vnodeID;
            ByteBuffer digest = ByteBuffer.wrap(md.digest(vn.getBytes()));
            digest.position(12);
            byte[] b = new byte[8];
            for (int i = 0; i < b.length; ++i) {
                b[i] = digest.get(i + 12);
            }
            long l = digest.getLong() << 32 >>> 32;
            int i = (int) (l % getDivisionNumber());
            virtualNodeIDMap.put(vnodeID, i);
        }
    }

    public void createDataStores() throws StorageException {
        dataStores = new DataStore[getDivisionNumber()];
        StringBuilder sb = new StringBuilder();
        sb.append(getStoragePathName());
        sb.append("/");
        sb.append(getStorageName());
        sb.append("/");
        String path = sb.toString();
        for (int i = 0; i < dataStores.length; ++i) {
            String fileName = path + i + "." + getFileExtensionName();
            dataStores[i] = dsFactory.newDataStore(fileName,
                    getFileExtensionName(), getOption(), deFactory, lcFactory);
            dataStores[i].open();
        }
    }

    public DataStore getDataStoreFromVNodeID(long virtualNodeID)
            throws StorageException {
        Integer i = virtualNodeIDMap.get(virtualNodeID);
        if (i == null) {
            i = 0;
        }
        if (i < 0 || getDivisionNumber() <= i) {
            throw new StorageException(
                    "Not found a data store specified by vnode: "
                            + virtualNodeID + " to index: " + i);
        }
        return getDataStoreFromIndex(i);
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
