package jp.co.rakuten.rit.roma.storage;

public class DataStoreFactory {

    public DataStore newDataStore(
            final String storagePathName,
            final String fileExtensionName) {
            
        preDataStoreInit();
        DataStore dataStore = initDataStore(
                storagePathName, fileExtensionName);
        postDataStoreInit(dataStore);
        return dataStore;
    }

    public void preDataStoreInit() {
    }

    public DataStore initDataStore(
            final String storagePathName,
            final String fileExtensionName) {
        return new HashMapDataStore(
                storagePathName, fileExtensionName);
    }

    public void postDataStoreInit(DataStore dataStore) {
    }
}
