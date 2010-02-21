package jp.co.rakuten.rit.roma.storage;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HashMapDataStore extends HashMap<String, DataEntry> implements
        DataStore {
    private static final Logger LOG = LoggerFactory
            .getLogger(HashMapDataStore.class);

    private String storagePathName;

    private String fileExtensionName;

    private String option;

    private DataEntryFactory deFactory;

    private LogicalClockFactory lcFactory;

    public HashMapDataStore(final String storagePathName,
            final String fileExtensionName, final String options,
            final DataEntryFactory deFactory,
            final LogicalClockFactory lcFactory) {
        this.storagePathName = storagePathName;
        this.fileExtensionName = fileExtensionName;
        this.option = option;
        this.deFactory = deFactory;
        this.lcFactory = lcFactory;
    }

    public void setStoragePathName(String name) {
        storagePathName = name;
    }

    public String getStoragePathName() {
        return storagePathName;
    }

    public void setFileExtensionName(String name) {
        fileExtensionName = name;
    }

    public String getFileExtensionName() {
        return fileExtensionName;
    }

    public void setOption(String option) {
        this.option = option;
    }

    public String getOption() {
        return option;
    }

    public void open() throws StorageException {
    }

    public void close() throws StorageException {
    }

    public boolean isFileBaseDataStore() {
        return false;
    }

    @Override
    public DataEntry get(Object key) {
        return super.get(key);
    }

    @Override
    public DataEntry put(final String key, final DataEntry value) {
        DataEntry ret = super.put(key, value);
        if (ret == null) {
            ret = value;
        }
        return ret;
    }

    @Override
    public DataEntry remove(Object key) {
        return super.remove(key);
    }

    @Override
    public void clear() {
        super.clear();
    }

    @Override
    public boolean containsKey(Object key) {
        return super.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return super.containsValue(value);
    }

    @Override
    public Set<java.util.Map.Entry<String, DataEntry>> entrySet() {
        return super.entrySet();
    }

    @Override
    public boolean isEmpty() {
        return super.isEmpty();
    }

    @Override
    public Set<String> keySet() {
        return super.keySet();
    }

    @Override
    public void putAll(Map<? extends String, ? extends DataEntry> m) {
        super.putAll(m);
    }

    @Override
    public int size() {
        return super.size();
    }

    @Override
    public Collection<DataEntry> values() {
        return super.values();
    }
}