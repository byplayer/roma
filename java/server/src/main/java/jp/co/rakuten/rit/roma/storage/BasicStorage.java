package jp.co.rakuten.rit.roma.storage;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * Basic class of ROMA storage.
 *
 */
public abstract class BasicStorage {
    private long[] virtualNodeIds;
    private HashDB[] realNodes;

    protected String extensionName;

    private Map<Long, Integer> nodeMap;

    private File storagePath;
    private int divisionNumber;

    private long dumpSleepIgnoreCount;
    private long dumpSleepTime;

    public BasicStorage() {
        this.nodeMap = new HashMap<Long, Integer>();

        this.extensionName = "db";
        this.divisionNumber = 10;

        this.dumpSleepIgnoreCount = 100L;
        this.dumpSleepTime = 1L;
    }

    public void setStoragePath(File storagePath) {
        this.storagePath = storagePath;
    }

    public void setVirtualNodeIds(long[] virtualNodeIds) {
        this.virtualNodeIds = virtualNodeIds;
    }

    public void setDivisionNumber(int divisionNumber) {
        this.divisionNumber = divisionNumber;
    }

    public void open() throws StorageException {
        /* initialize node map */
        nodeMap.clear();
        createNodeMap();

        /* create storage directory */
        if (storagePath != null && !storagePath.isDirectory()
                && !storagePath.mkdirs()) {
            throw new StorageException("Can not create storage directory: "
                    + storagePath);
        }

        /* open real node (HashDB instance) */
        String dirName = storagePath != null ? storagePath.getAbsolutePath()
                + "/" : "";
        realNodes = new HashDB[divisionNumber];
        for (int i = 0; i < divisionNumber; i++) {
            realNodes[i] = openNode(dirName + i + "." + extensionName);
        }
    }

    public DataEntry set(DataEntry entry) throws StorageException {
        /* specify real node */
        HashDB node = getNode(entry.getVirtualNodeId());

        /* get old entry */
        long logicalClock = 0L;
        byte[] entryData = node.get(entry.getKey());
        if (entryData != null) {
            DataEntry oldEntry = new DataEntry();
            oldEntry.loadEntryData(entryData);

            logicalClock++;
        }

        /* regist new entry */
        entry.setLogicalClock(logicalClock);
        entry.setPhysicalClockToNow();
        if (!node.put(entry.getKey(), entry.getEntryData())) {
            return null;
        }
        return entry;
    }

    public DataEntry add(DataEntry entry) throws StorageException {
        /* specify real node */
        HashDB node = getNode(entry.getVirtualNodeId());

        /* get old entry */
        long logicalClock = 0L;
        byte[] entryData = node.get(entry.getKey());
        if (entryData != null) {
            DataEntry oldEntry = new DataEntry();
            oldEntry.loadEntryData(entryData);

            if (!oldEntry.isExpired()) {
                return null;
            }
            logicalClock++;
        }

        /* regist new entry */
        entry.setLogicalClock(logicalClock);
        entry.setPhysicalClockToNow();
        if (!node.put(entry.getKey(), entry.getEntryData())) {
            return null;
        }
        return entry;
    }

    public DataEntry replace(DataEntry entry) throws StorageException {
        /* specify real node */
        HashDB node = getNode(entry.getVirtualNodeId());

        /* get old entry */
        byte[] entryData = node.get(entry.getKey());
        if (entryData == null) {
            return null;
        }

        /* load entry data */
        DataEntry oldEntry = new DataEntry();
        oldEntry.loadEntryData(entryData);
        if (oldEntry.isExpired()) {
            return null;
        }

        /* regist new entry */
        entry.setLogicalClock(oldEntry.getLogicalClock() + 1);
        entry.setPhysicalClockToNow();
        if (!node.put(entry.getKey(), entry.getEntryData())) {
            return null;
        }
        return entry;
    }

    public DataEntry append(DataEntry entry) throws StorageException {
        /* specify real node */
        HashDB node = getNode(entry.getVirtualNodeId());

        /* get old entry */
        byte[] entryData = node.get(entry.getKey());
        if (entryData == null) {
            return null;
        }

        /* load entry data */
        DataEntry oldEntry = new DataEntry();
        oldEntry.loadEntryData(entryData);
        if (oldEntry.isExpired()) {
            return null;
        }

        /* append entry */
        entry.setLogicalClock(oldEntry.getLogicalClock() + 1);
        entry.setPhysicalClockToNow();
        entry.prependData(oldEntry.getData());
        if (!node.put(entry.getKey(), entry.getEntryData())) {
            return null;
        }
        return entry;
    }

    public DataEntry prepend(DataEntry entry) throws StorageException {
        /* specify real node */
        HashDB node = getNode(entry.getVirtualNodeId());

        /* get old entry */
        byte[] entryData = node.get(entry.getKey());
        if (entryData == null) {
            return null;
        }

        /* load entry data */
        DataEntry oldEntry = new DataEntry();
        oldEntry.loadEntryData(entryData);
        if (oldEntry.isExpired()) {
            return null;
        }

        /* prepend entry */
        entry.setLogicalClock(oldEntry.getLogicalClock() + 1);
        entry.setPhysicalClockToNow();
        entry.appendData(oldEntry.getData());
        if (!node.put(entry.getKey(), entry.getEntryData())) {
            return null;
        }
        return entry;
    }

    public DataEntry get(DataEntry entry) throws StorageException {
        /* specify real node */
        HashDB node = getNode(entry.getVirtualNodeId());

        /* get entry */
        byte[] key = entry.getKey();
        byte[] entryData = node.get(entry.getKey());
        if (entryData == null) {
            return null;
        }
        entry = new DataEntry();
        entry.setKey(key);
        entry.loadEntryData(entryData);

        /* check entry */
        if (entry.isExpired()) {
            return null;
        }

        return entry;
    }

    public DataEntry delete(DataEntry entry) throws StorageException {
        /* specify real node */
        HashDB node = getNode(entry.getVirtualNodeId());

        /* get old entry */
        long logicalClock = 0L;
        byte[] data = new byte[0];
        byte[] entryData = node.get(entry.getKey());
        if (entryData != null) {
            DataEntry oldEntry = new DataEntry();
            oldEntry.loadEntryData(entryData);
            if (!oldEntry.isExpired()) {
                data = oldEntry.getData();
            }

            logicalClock++;
        }

        /* regist new entry */
        entry.setPhysicalClockToNow();
        entry.setLogicalClock(logicalClock);
        entry.setExpireTime(0L);
        entry.setData(data);
        if (!node.put(entry.getKey(), entry.getEntryData())) {
            return null;
        }
        return entry;
    }

    public DataEntry out(DataEntry entry) throws StorageException {
        /* specify real node */
        HashDB node = getNode(entry.getVirtualNodeId());

        /* remove entry */
        return node.remove(entry.getKey()) ? entry : null;
    }

    public void close() throws StorageException {
        for (HashDB node : realNodes) {
            closeNode(node);
        }
    }

    public void load(DataInput in) throws StorageException, IOException {
    }

    public byte[] dumpVirtualNode(long virtualNodeId) throws StorageException,
            IOException {
        /* create byte array */
        ByteArrayOutputStream barray = new ByteArrayOutputStream();

        /* dump to byte array */
        DataOutputStream out = new DataOutputStream(barray);
        dumpVirtualNode(out, virtualNodeId);
        out.close();

        return barray.toByteArray();
    }

    public void dumpVirtualNode(DataOutput out, long virtualNodeId)
            throws StorageException, IOException {
        /* specify real node */
        HashDB node = getNode(virtualNodeId);

        /* dump virtual node to byte array */
        Iterator<byte[]> keyItr = node.keyIterator();
        long ignoreCount = 0L;
        while (keyItr.hasNext()) {
            /* load entry */
            byte[] key = keyItr.next();
            byte[] entryData = node.get(key);

            DataEntry entry = new DataEntry();
            entry.setKey(key);
            entry.loadEntryData(entryData);

            /* dump entry */
            if (entry.getVirtualNodeId() == virtualNodeId && !entry.isExpired()) {
                entry.dump(out);
                sleep(dumpSleepTime);
            } else if (++ignoreCount % dumpSleepIgnoreCount == 0) {
                sleep(dumpSleepTime);
            }
        }
    }

    public void dump(File outputDir) throws StorageException, IOException {
        dump(outputDir, null);
    }

    public void dump(File outputDir, long[] exceptVirtualNodeIds)
            throws StorageException, IOException {
        /* create output directory */
        if (!outputDir.isDirectory() && !outputDir.mkdirs()) {
            throw new StorageException("Can not create output directory: "
                    + outputDir);
        }

        /* prepare except virtual node ids */
        Set<Long> exceptIdSet = new HashSet<Long>();
        for (long virtualNodeId : exceptVirtualNodeIds) {
            exceptIdSet.add(virtualNodeId);
        }

        /* each real nodes */
        for (int i = 0; i < divisionNumber; i++) {
            HashDB node = realNodes[i];

            /* dump real node to file */
            File dumpFile = new File(outputDir.getAbsolutePath() + "/" + i
                    + ".dump");
            DataOutputStream out = new DataOutputStream(new FileOutputStream(
                    dumpFile));

            long ignoreCount = 0L;
            Iterator<byte[]> keyItr = node.keyIterator();
            while (keyItr.hasNext()) {
                /* load entry */
                byte[] key = keyItr.next();
                byte[] entryData = node.get(key);

                DataEntry entry = new DataEntry();
                entry.setKey(key);
                entry.loadEntryData(entryData);

                /* dump entry */
                if (!exceptIdSet.contains(entry.getVirtualNodeId())
                        && !entry.isExpired()) {
                    entry.dump(out);
                    sleep(dumpSleepTime);
                } else if (++ignoreCount % dumpSleepIgnoreCount == 0) {
                    sleep(dumpSleepTime);
                }
            }

            out.close();
        }

        /* write time at end of dump to eod file */
        File eodFile = new File(outputDir.getAbsoluteFile() + "/eod");
        try {
            BufferedWriter out = new BufferedWriter(new FileWriter(eodFile));
            out.write(new Date().toString());
            out.close();
        } catch (IOException e) {
            throw new StorageException("Can not write end of dump: "
                    + eodFile.getAbsolutePath(), e);
        }
    }

    protected void createNodeMap() {
        Random random;
        try {
            random = SecureRandom.getInstance("SHA1");
        } catch (NoSuchAlgorithmException e) {
            random = new Random();
        }

        for (long virtualNodeId : virtualNodeIds) {
            random.setSeed(virtualNodeId);
            int index = random.nextInt(divisionNumber);
            nodeMap.put(virtualNodeId, index);
        }
    }

    protected abstract HashDB openNode(String path) throws StorageException;

    protected abstract void closeNode(HashDB node) throws StorageException;

    protected HashDB getNode(long virtualNodeId) throws UnknownVirtualNode {
        /* check virtual node */
        if (!nodeMap.containsKey(virtualNodeId)) {
            throw new UnknownVirtualNode(virtualNodeId, storagePath);
        }

        /* specify real node */
        int index = nodeMap.get(virtualNodeId);
        return realNodes[index];
    }

    private static void sleep(long milliSecond) {
        if (milliSecond > 0) {
            try {
                Thread.sleep(milliSecond);
            } catch (InterruptedException e) {
            }
        }
    }

    protected static abstract class HashDB {
        public abstract boolean put(byte[] key, byte[] value)
                throws StorageException;

        public abstract byte[] get(byte[] key) throws StorageException;

        public abstract boolean remove(byte[] key) throws StorageException;

        public abstract Iterator<byte[]> keyIterator() throws StorageException;
    }
}
