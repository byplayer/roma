package jp.co.rakuten.rit.roma.storage;

import java.util.Collection;
import java.util.HashMap;
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
        tchdb = new HDB();

        // set options
        parseAndSetOption();

        // open
        if (!tchdb.open(getStoragePathName(), HDB.OWRITER | HDB.OCREAT
                | HDB.ONOLCK)) {
            int ecode = tchdb.ecode();
            throw new StorageException("open error: " + HDB.errmsg(ecode));
        }
    }

    private void parseAndSetOption() {
        // parse
        String str = getOption();
        String[] opts = str.split("#");
        HashMap<String, String> props = new HashMap<String, String>();
        for (int i = 0; i < opts.length; ++i) {
            String[] kv = opts[i].split("=");
            props.put(kv[0], kv[1]);
        }

        // validate
        for (Iterator<String> keys = props.keySet().iterator(); keys.hasNext();) {
            String key = keys.next();
            if (!(key.equals("bnum") || key.equals("apow")
                    || key.equals("fpow") || key.equals("opts")
                    || key.equals("xmsiz") || key.equals("rcnum") || key
                    .equals("dfunit"))) {
                throw new RuntimeException("syntax error, unexpected option: "
                        + key);
            }
        }

        // set
        int o = 0;
        if (props.containsKey("opts")) {
            String v = props.get("opts");
            if (v.indexOf('l') != -1) {
                o |= HDB.TLARGE;
            }
            if (v.indexOf('d') != -1) {
                o |= HDB.TDEFLATE;
            }
            if (v.indexOf('b') != -1) {
                o |= HDB.TBZIP;
            }
            if (v.indexOf('t') != -1) {
                o |= HDB.TTCBS;
            }
        }

        long bnum = 131071;
        if (props.containsKey("bnum")) {
            bnum = Long.parseLong(props.get("bnum"));
        }
        int apow = 4;
        if (props.containsKey("anum")) {
            apow = Integer.parseInt(props.get("apow"));
        }
        int fpow = 10;
        if (props.containsKey("fnum")) {
            fpow = Integer.parseInt(props.get("fpow"));
        }
        tchdb.tune(bnum, apow, fpow, o);

        if (props.containsKey("xmsiz")) {
            tchdb.setxmsiz(Long.parseLong(props.get("xmsiz")));
        }
        if (props.containsKey("dfunit")) {
            tchdb.setdfunit(Integer.parseInt(props.get("dfunit")));
        }
        if (props.containsKey("rcnum")) {
            tchdb.setcache(Integer.parseInt(props.get("rcnum")));
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
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsKey(Object key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsValue(Object value) {
        throw new UnsupportedOperationException();
    }
    
    public static class TCEntry implements java.util.Map.Entry<String, DataEntry> {
        private DataEntry entry;
        
        public TCEntry(DataEntry entry) {
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
                    boolean iteratable = tchdb.iterinit();
                    
                    byte[] key;

                    @Override
                    public boolean hasNext() {
                        if (!iteratable) {
                            return false;
                        }
                        key = tchdb.iternext();
                        return key != null;
                    }

                    @Override
                    public java.util.Map.Entry<String, DataEntry> next() {
                        DataEntry entry = get(new String(key));
                        if (entry != null) {
                            return new TCEntry(entry);
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
        return (int) tchdb.rnum();
    }

    @Override
    public Collection<DataEntry> values() {
        throw new UnsupportedOperationException();
    }
}