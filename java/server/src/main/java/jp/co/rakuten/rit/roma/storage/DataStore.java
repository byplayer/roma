package jp.co.rakuten.rit.roma.storage;

import java.util.Iterator;

public interface DataStore {

    void open();

    void close();

    boolean isFileBaseDataStore();

    boolean put(String key, byte[] value) throws StorageException;

    byte[] get(String key) throws StorageException;

    boolean remove(String key) throws StorageException;

    Iterator<String> keyIterator() throws StorageException;
}
