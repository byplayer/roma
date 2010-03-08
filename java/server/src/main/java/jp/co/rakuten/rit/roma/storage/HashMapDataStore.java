package jp.co.rakuten.rit.roma.storage;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HashMapDataStore implements DataStore {

    private static final Logger LOG = LoggerFactory
            .getLogger(HashMapDataStore.class);

    private String storagePathName;

    private String fileExtensionName;

    private String option;

    private DataEntryFactory deFactory;

    private LogicalClockFactory lcFactory;

    private HashMap<String, DataEntry> hashMap;

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
        hashMap = new HashMap<String, DataEntry>();
    }

    public void close() throws StorageException {
    }

    public boolean isFileBaseDataStore() {
        return false;
    }

    @Override
    public DataEntry get(Object key) {
        return hashMap.get(key);
    }

    @Override
    public DataEntry put(final String key, final DataEntry value) {
        DataEntry ret = hashMap.put(key, value);
        if (ret == null) {
            ret = value;
        }
        return ret;
    }

    @Override
    public DataEntry remove(Object key) {
        return hashMap.remove(key);
    }

    @Override
    public void clear() {
        hashMap.clear();
    }

    @Override
    public boolean containsKey(Object key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsValue(Object value) {
        throw new UnsupportedOperationException();
    }

    public static class HMEntry implements
            java.util.Map.Entry<String, DataEntry> {
        private DataEntry entry;

        public HMEntry(DataEntry entry) {
            this.entry = entry;
        }

        @Override
        public String getKey() {
            return entry.getKey();
        }

        @Override
        public DataEntry getValue() {
            return entry;
        }

        @Override
        public DataEntry setValue(DataEntry value) {
            throw new UnsupportedOperationException();
        }

    }

    @Override
    public Set<java.util.Map.Entry<String, DataEntry>> entrySet() {
        return new Set<java.util.Map.Entry<String, DataEntry>>() {
            @Override
            public boolean add(java.util.Map.Entry<String, DataEntry> e) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean addAll(
                    Collection<? extends java.util.Map.Entry<String, DataEntry>> c) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void clear() {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean contains(Object o) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean containsAll(Collection<?> c) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean isEmpty() {
                throw new UnsupportedOperationException();
            }

            @Override
            public Iterator<java.util.Map.Entry<String, DataEntry>> iterator() {
                return new Iterator<java.util.Map.Entry<String, DataEntry>>() {

                    Iterator<String> iter = hashMap.keySet().iterator();

                    byte[] key;

                    @Override
                    public boolean hasNext() {
                        if (!iter.hasNext()) {
                            return false;
                        }
                        key = iter.next().getBytes();
                        return key != null;
                    }

                    @Override
                    public java.util.Map.Entry<String, DataEntry> next() {
                        DataEntry entry = get(new String(key));
                        if (entry != null) {
                            return new HMEntry(entry);
                        } else {
                            return null;
                        }
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();

                    }
                };
            }

            @Override
            public boolean remove(Object o) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean removeAll(Collection<?> c) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean retainAll(Collection<?> c) {
                throw new UnsupportedOperationException();
            }

            @Override
            public int size() {
                throw new UnsupportedOperationException();
            }

            @Override
            public Object[] toArray() {
                throw new UnsupportedOperationException();
            }

            @Override
            public <T> T[] toArray(T[] a) {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public boolean isEmpty() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<String> keySet() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putAll(Map<? extends String, ? extends DataEntry> m) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
        return hashMap.size();
    }

    @Override
    public Collection<DataEntry> values() {
        throw new UnsupportedOperationException();
    }
}