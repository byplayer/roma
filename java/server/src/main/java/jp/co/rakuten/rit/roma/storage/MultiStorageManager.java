package jp.co.rakuten.rit.roma.storage;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiStorageManager {

    private static final Logger LOG = LoggerFactory
            .getLogger(MultiStorageManager.class);

    private Map<String, BasicStorage> storages = null;

    public MultiStorageManager() {
        storages = new HashMap<String, BasicStorage>();
    }

    public void open() throws StorageException {
        LOG.info("start a MultiStorageManager service");
        Iterator<BasicStorage> storageIter = storages.values().iterator();
        while (storageIter.hasNext()) {
            BasicStorage storage = storageIter.next();
            storage.open();
        }
    }

    public void close() throws StorageException {
        LOG.info("finish a MultiStorageManager service");
        Iterator<BasicStorage> storageIter = storages.values().iterator();
        while (storageIter.hasNext()) {
            BasicStorage storage = storageIter.next();
            storage.close();
        }
    }

    public void setStorage(String name, BasicStorage storage) {
        LOG.info("add a new storage: name: " + name);
        storages.put(name, storage);
    }
}