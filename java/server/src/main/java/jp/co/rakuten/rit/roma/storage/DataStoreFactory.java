package jp.co.rakuten.rit.roma.storage;

public class DataStoreFactory {

    public DataStore newDataStore(final String storagePathName,
            final String fileExtensionName, final String options,
            final DataEntryFactory deFactory,
            final LogicalClockFactory lcFactory) {

        preDataStoreInit();
        DataStore dataStore = initDataStore(storagePathName, fileExtensionName,
                options, deFactory, lcFactory);
        postDataStoreInit(dataStore);
        return dataStore;
    }

    public void preDataStoreInit() {
    }

    public DataStore initDataStore(final String storagePathName,
            final String fileExtensionName, final String options,
            final DataEntryFactory deFactory,
            final LogicalClockFactory lcFactory) {
        return new HashMapDataStore(storagePathName, fileExtensionName,
                options, deFactory, lcFactory);
    }

    public void postDataStoreInit(DataStore dataStore) {
    }
}
