package jp.co.rakuten.rit.roma.storage;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * This class represents entry of roma.
 *
 */
public class DataEntry {
    /**
     * Header informations.
     *
     * If you want to change here, you must see
     * {@link #serializeHeaderValue(long, byte[], int)} and
     * {@link #unserializeHeaderValue(byte[], int)}
     */
    private static final int HEADER_LENGTH = 16;
    private static final int VIRTUAL_NODE_ID_OFFSET = 0;
    private static final int PHYSICAL_CLOCK_OFFSET = 4;
    private static final int LOGICAL_CLOCK_OFFSET = 8;
    private static final int EXPIRE_TIME_OFFSET = 12;

    private byte[] key;

    private long virtualNodeId;
    private long physicalClock;
    private long logicalClock;
    private long expireTime;
    private byte[] data;

    DataEntry() {
        this(new byte[0], 0L, getNow(), 0L, 0L, new byte[0]);
    }

    public DataEntry(long virtualNodeId) {
        this(new byte[0], virtualNodeId, getNow(), 0L, 0L, new byte[0]);
    }

    public DataEntry(long virtualNodeId, long physicalClock, long logicalClock,
            long expireTime) {
        this(new byte[0], virtualNodeId, physicalClock, logicalClock,
                expireTime, new byte[0]);
    }

    public DataEntry(byte[] key, long virtualNodeId, long physicalClock,
            long logicalClock, long expireTime, byte[] data) {
        this.key = key;

        this.virtualNodeId = virtualNodeId;
        this.physicalClock = physicalClock;
        this.logicalClock = logicalClock;
        this.expireTime = expireTime;
        this.data = data;
    }

    public byte[] getEntryData() {
        byte[] entryData = new byte[HEADER_LENGTH + data.length];

        serializeHeaderValue(virtualNodeId, entryData, VIRTUAL_NODE_ID_OFFSET);
        serializeHeaderValue(physicalClock, entryData, PHYSICAL_CLOCK_OFFSET);
        serializeHeaderValue(logicalClock, entryData, LOGICAL_CLOCK_OFFSET);
        serializeHeaderValue(expireTime, entryData, EXPIRE_TIME_OFFSET);

        System.arraycopy(data, 0, entryData, HEADER_LENGTH, data.length);

        return entryData;
    }

    public boolean isExpired() {
        return expireTime < getNow();
    }

    public void setPhysicalClockToNow() {
        this.physicalClock = getNow();
    }

    public void loadEntryData(byte[] entryData) throws DataEntryFormatException {
        /* check entry data */
        if (entryData.length < HEADER_LENGTH) {
            throw new DataEntryFormatException("Entry must be greater than "
                    + HEADER_LENGTH + " bytes");
        }

        /* read entry data */
        this.virtualNodeId = unserializeHeaderValue(entryData,
                VIRTUAL_NODE_ID_OFFSET);
        this.physicalClock = unserializeHeaderValue(entryData,
                PHYSICAL_CLOCK_OFFSET);
        this.logicalClock = unserializeHeaderValue(entryData,
                LOGICAL_CLOCK_OFFSET);
        this.expireTime = unserializeHeaderValue(entryData, EXPIRE_TIME_OFFSET);

        this.data = new byte[entryData.length - HEADER_LENGTH];
        System.arraycopy(entryData, HEADER_LENGTH, data, 0, data.length);
    }

    public void load(DataInput in) throws IOException, DataEntryFormatException {
        /* load key */
        key = new byte[in.readInt()];
        in.readFully(key);

        /* read entry data */
        byte[] entryData = new byte[in.readInt()];
        in.readFully(entryData);
        loadEntryData(entryData);
    }

    public void dump(DataOutput out) throws IOException {
        out.writeInt(key.length);
        out.write(key);
        out.writeInt(HEADER_LENGTH + data.length);
        out.write(getEntryData());
    }

    /* getters and setters */
    public void setVirtualNodeId(long virtualNodeId) {
        this.virtualNodeId = virtualNodeId;
    }

    public long getVirtualNodeId() {
        return virtualNodeId;
    }

    public void setPhysicalClock(long physicalClock) {
        this.physicalClock = physicalClock;
    }

    public long getPhysicalClock() {
        return physicalClock;
    }

    public void setLogicalClock(long logicalClock) {
        this.logicalClock = logicalClock;
    }

    public long getLogicalClock() {
        return logicalClock;
    }

    public void setExpireTime(long expireTime) {
        this.expireTime = expireTime;
    }

    public long getExpireTime() {
        return expireTime;
    }

    /* キーとデータは色々な型をサポートしようね！！ */
    public void setKey(byte[] key) {
        this.key = key;
    }

    public void setKey(String key) {
        this.key = key.getBytes();
    }

    public byte[] getKey() {
        return key;
    }

    public String getKeyAsString() {
        return new String(key);
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    public void setData(String value) {
        this.data = value.getBytes();
    }

    public void prependData(byte[] data) {
        this.data = merge(data, this.data);
    }

    public void appendData(byte[] data) {
        this.data = merge(this.data, data);
    }

    public byte[] getData() {
        return data;
    }

    public String getDataAsString() {
        return new String(data);
    }

    public String toString() {
        return "[" + "virtualNodeId=" + virtualNodeId + ", " + "hysicalClock="
                + physicalClock + ", " + "logicalClock=" + logicalClock + ", "
                + "expireTime=" + expireTime + ", " + "key=\""
                + getKeyAsString() + "\", " + "data=\"" + getDataAsString()
                + "\"" + "]";
    }

    /* private methods */
    private static long getNow() {
        return System.currentTimeMillis() / 1000;
    }

    private static void serializeHeaderValue(long key, byte[] data, int offset) {
        data[offset + 0] = (byte) ((key >> 24) & 0xFF);
        data[offset + 1] = (byte) ((key >> 16) & 0xFF);
        data[offset + 2] = (byte) ((key >> 8) & 0xFF);
        data[offset + 3] = (byte) ((key >> 0) & 0xFF);
    }

    private static long unserializeHeaderValue(byte[] data, int offset) {
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
