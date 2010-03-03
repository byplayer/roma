package jp.co.rakuten.rit.roma.storage;

public class DataEntryFactory {

    public DataEntry newDataEntry(String key, long vnodeID, long pClock,
            LogicalClock lClock, long expire, byte[] value) {
        preInitDataEntry();
        DataEntry entry = initDataEntry(key, vnodeID, pClock, lClock, expire,
                value);
        postInitDataEntry(entry);
        return entry;
    }

    public void preInitDataEntry() {
    }

    public DataEntry initDataEntry(String key, long vnodeID, long pClock,
            LogicalClock lClock, long expire, byte[] value) {
        return new DataEntry(key, vnodeID, pClock, lClock, expire, value);
    }

    public void postInitDataEntry(DataEntry entry) {
    }
}
