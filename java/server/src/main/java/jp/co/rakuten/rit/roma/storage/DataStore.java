package jp.co.rakuten.rit.roma.storage;

import java.util.Map;

public interface DataStore extends Map<String, DataEntry> {

    void setStoragePathName(String name);

    String getStoragePathName();

    void setFileExtensionName(String name);

    String getFileExtensionName();

    void setOption(String name);

    String getOption();

    void open() throws StorageException;

    void close() throws StorageException;

    boolean isFileBaseDataStore();
}