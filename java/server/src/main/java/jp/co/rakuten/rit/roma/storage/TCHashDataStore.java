package jp.co.rakuten.rit.roma.storage;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tokyocabinet.HDB;

public class TCHashDataStore implements DataStore {

    private static final Logger LOG = LoggerFactory
            .getLogger(TCHashDataStore.class);

    private String storagePathName;

    private String fileExtensionName;

    private String option;

    private DataEntryFactory deFactory;

    private LogicalClockFactory lcFactory;

    private HDB tchdb;

    public TCHashDataStore(final String storagePathName,
            final String fileExtensionName, final String option,
            final DataEntryFactory deFactory,
            final LogicalClockFactory lcFactory) {
        this.storagePathName = storagePathName;
        this.fileExtensionName = fileExtensionName;
        this.option = option;
        this.deFactory = deFactory;
        this.lcFactory = lcFactory;
    }

    public String getStoragePathName() {
        return storagePathName;
    }

    public String getFileExtensionName() {
        return fileExtensionName;
    }

    public String getOption() {
        return option;
    }

    public void open() throws StorageException {
        tchdb = new HDB();

        // set options
        String opt = getOption();
        String[] opts = opt.split("#");
        for (int i = 0; i < opts.length; ++i) {
            parseAndSetOption(opts[i]);
        }

        //        
        // /* create storage directory */
        // if (storagePath != null && !storagePath.isDirectory()
        // && !storagePath.mkdirs()) {
        // throw new StorageException("Can not create storage directory: "
        // + storagePath);
        // }
        //
        // /* open real node (HashDB instance) */
        // String dirName = storagePath != null ? storagePath.getAbsolutePath()
        // + "/" : "";
        // realNodes = new HashDB[divisionNumber];
        // for (int i = 0; i < divisionNumber; i++) {
        // realNodes[i] = openNode(dirName + i + "." + extensionName);
        // }

        // open
        if (!tchdb.open(getStoragePathName(), HDB.OWRITER | HDB.OCREAT
                | HDB.ONOLCK)) {
            int ecode = tchdb.ecode();
            throw new StorageException("open error: " + tchdb.errmsg(ecode));
        }
    }

    private void parseAndSetOption(String opt) {
        String[] keyAndValue = opt.split("=");
        String key = keyAndValue[0];
        String val = keyAndValue[1];
        if (key.equals("xmsiz")) {
            tchdb.setxmsiz(Long.parseLong(val));
        } else if (key.equals("dfunit")) {
            tchdb.setdfunit(Integer.parseInt(val));
        } else if (key.equals("rcnum")) {
            tchdb.setcache(Integer.parseInt(val));
        } else if (key.equals("bnum")) {
            tchdb.tune(Long.parseLong(val), 4, 10, HDB.TBZIP);
        }
    }

    public void close() throws StorageException {
        if (!tchdb.close()) {
            int ecode = tchdb.ecode();
            throw new StorageException("close error: " + tchdb.errmsg(ecode));
        }
    }

    public boolean isFileBaseDataStore() {
        return false;
    }

    @Override
    public DataEntry get(Object key) {
        byte[] rawData = tchdb.get(((String) key).getBytes());
        if (rawData != null) {
            return DataEntry.toDataEntry(deFactory, (String) key, rawData,
                    lcFactory);
        } else {
            return null;
        }
    }

    @Override
    public DataEntry put(final String key, final DataEntry value) {
        byte[] rawData = DataEntry.toByteArray(value);
        if (!tchdb.put(key.getBytes(), rawData)) {
            return null;
        } else {
            return value;
        }
    }

    @Override
    public DataEntry remove(Object key) {
        byte[] rawData = tchdb.get(((String) key).getBytes());
        if (!tchdb.out((String) key)) {
            return null;
        } else {
            if (rawData != null) {
                return DataEntry.toDataEntry(deFactory, (String) key, rawData,
                        lcFactory);
            } else {
                return null;
            }
        }
    }

    @Override
    public void clear() {
        LOG.info("clear");
    }

    @Override
    public boolean containsKey(Object key) {
        LOG.info("containsKey");
        return false;
    }

    @Override
    public boolean containsValue(Object value) {
        LOG.info("containsValue");
        return false;
    }

    @Override
    public Set<java.util.Map.Entry<String, DataEntry>> entrySet() {
        LOG.info("entrySet");
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
                    @Override
                    public boolean hasNext() {
                        // TODO Auto-generated method stub
                        return false;
                    }

                    @Override
                    public java.util.Map.Entry<String, DataEntry> next() {
                        // TODO Auto-generated method stub
                        return null;
                    }

                    @Override
                    public void remove() {
                        // TODO Auto-generated method stub

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
        LOG.info("isEmpty");
        return false;
    }

    @Override
    public Set<String> keySet() {
        LOG.info("keySet");
        // TODO
        return null;
    }

    @Override
    public void putAll(Map<? extends String, ? extends DataEntry> m) {
        LOG.info("putAll");
    }

    @Override
    public int size() {
        LOG.info("size");
        return 0;
    }

    @Override
    public Collection<DataEntry> values() {
        LOG.info("values");
        return null;
    }
}