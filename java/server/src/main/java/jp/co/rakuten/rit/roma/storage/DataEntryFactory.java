package jp.co.rakuten.rit.roma.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataEntryFactory {

    private static Logger LOG = LoggerFactory.getLogger(DataEntryFactory.class);

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