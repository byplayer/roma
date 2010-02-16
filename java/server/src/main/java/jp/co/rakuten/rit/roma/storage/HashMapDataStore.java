package jp.co.rakuten.rit.roma.storage;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class HashMapDataStore implements DataStore {

    private String storagePathName;

    private String fileExtensionName;

    private Map<String, byte[]> values;

    public HashMapDataStore(String storagePathName, String fileExtensionName) {
        this.storagePathName = storagePathName;
        this.fileExtensionName = fileExtensionName;
    }

    public void open() {
        values = new HashMap<String, byte[]>();
    }

    public void close() {
    }

    public boolean isFileBaseDataStore() {
        return false;
    }

    public byte[] get(String key) {
        return values.get(key);
    }

    public Iterator<String> keyIterator() {
        return new Iterator<String>() {
            private Iterator<String> iter = values.keySet().iterator();

            @Override
            public boolean hasNext() {
                return iter.hasNext();
            }

            @Override
            public String next() {
                return iter.next();
            }

            @Override
            public void remove() {
                iter.remove();
            }
        };
    }

    public boolean put(String key, byte[] value) {
        values.put(key, value);
        return true;
    }

    public boolean remove(String key) throws StorageException {
        return values.remove(key) != null;
    }
}
