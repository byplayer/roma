package jp.co.rakuten.rit.roma.storage;

import java.util.Map;

public interface DataStore extends Map<String, DataEntry> {

    String getStoragePathName();

    String getFileExtensionName();

    String getOption();

    void open() throws StorageException;

    void close() throws StorageException;

    boolean isFileBaseDataStore();
}