package jp.co.rakuten.rit.roma.storage;

import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataEntry implements Entry<String, DataEntry> {
    private static final Logger LOG = LoggerFactory.getLogger(DataEntry.class);

    private static final int METADATA_SIZE = 4 + 4 + 4 + 4;

    private String key;

    private long hash; // long

    private long vnodeID; // long

    private long pClock; // long

    private LogicalClock lClock; // long

    private long expire; // long

    private byte[] data;

    public DataEntry(String key, long vnodeID, long pClock,
            LogicalClock lClock, long expire, byte[] data) {
        this.key = key;
        this.vnodeID = vnodeID;
        this.pClock = pClock;
        this.lClock = lClock;
        this.expire = expire;
        this.data = data;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    public void setHash(long hash) {
        this.hash = hash;
    }

    public long getHash() {
        return hash;
    }

    public void setVNodeID(long vnodeID) {
        this.vnodeID = vnodeID;
    }

    public long getVNodeID() {
        return vnodeID;
    }

    public boolean isExpired() {
        return expire < getNow();
    }

    public void setCurrentPClock() {
        pClock = getNow();
    }

    public void setPClock(long pClock) {
        this.pClock = pClock;
    }

    public long getPClock() {
        return pClock;
    }

    public void setLClock(LogicalClock lClock) {
        this.lClock = lClock;
    }

    public LogicalClock getLClock() {
        return lClock;
    }

    public void setExpire(long expire) {
        this.expire = expire;
    }

    public long getExpire() {
        return expire;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    public byte[] getData() {
        return data;
    }

    public DataEntry getValue() {
        return this;
    }

    public DataEntry value() {
        return this;
    }

    public DataEntry setValue(DataEntry entry) {
        throw new UnsupportedOperationException();
    }

    public static long getNow() {
        return System.currentTimeMillis() / 1000;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[").append("key=").append(getKey()).append(",").append(
                "vnodeID=").append(getVNodeID()).append(",").append("pClock=")
                .append(getPClock()).append(",").append("lClock=").append(
                        getLClock()).append(",").append("expire=").append(
                        getExpire()).append(",").append("value=").append(
                        new String(getData())).append("]");
        return sb.toString();
    }

    public static byte[] toByteArray(DataEntry entry) {
        byte[] rawData = new byte[METADATA_SIZE + entry.getData().length];
        write32bits(rawData, 0, entry.getVNodeID());
        write32bits(rawData, 4, entry.getPClock());
        write32bits(rawData, 8, entry.getLClock().getRaw());
        write32bits(rawData, 12, entry.getExpire());
        System.arraycopy(entry.getData(), 0, rawData, METADATA_SIZE, entry
                .getData().length);
        return rawData;
    }

    public static DataEntry toDataEntry(DataEntryFactory deFactory, String key,
            byte[] rawData, LogicalClockFactory lcFactory) {
        long vnodeID = read32bits(rawData, 0);
        long pClock = read32bits(rawData, 4);
        long lClock = read32bits(rawData, 8);
        long expire = read32bits(rawData, 12);
        byte[] value = new byte[rawData.length - METADATA_SIZE];
        System.arraycopy(rawData, METADATA_SIZE, value, 0, value.length);
        DataEntry entry = deFactory.newDataEntry(key, vnodeID, pClock,
                lcFactory.newLogicalClock(lClock), expire, value);
        return entry;
    }

    public static void write32bits(byte[] data, int offset, long val) {
        data[offset + 0] = (byte) ((val >> 24) & 0xFF);
        data[offset + 1] = (byte) ((val >> 16) & 0xFF);
        data[offset + 2] = (byte) ((val >> 8) & 0xFF);
        data[offset + 3] = (byte) ((val >> 0) & 0xFF);
    }

    private static long read32bits(byte[] data, int offset) {
        long value = 0L;
        value |= (data[offset + 0] & 0xFFL) << 24;
        value |= (data[offset + 1] & 0xFFL) << 16;
        value |= (data[offset + 2] & 0xFFL) << 8;
        value |= (data[offset + 3] & 0xFFL) << 0;
        return value;
    }

    private static byte[] merge(byte[] d1, byte[] d2) {
        byte[] data = new byte[d1.length + d2.length];
        System.arraycopy(d1, 0, data, 0, d1.length);
        System.arraycopy(d2, 0, data, d1.length, d2.length);
        return data;
    }
}
