package jp.co.rakuten.rit.roma.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataStoreFactory {

    private static Logger LOG = LoggerFactory.getLogger(DataStoreFactory.class);

    public DataStore newDataStore(final String storagePathName,
            final String fileExtensionName, final String options,
            final DataEntryFactory deFactory,
            final LogicalClockFactory lcFactory) {
        StringBuilder sb = new StringBuilder();
        sb.append("start initializing a new storage: ");
        sb.append("path: " + storagePathName + ", ");
        sb.append("opts: " + options + ", ");
        sb.append("dataentry fact: " + deFactory + ", ");
        sb.append("lclock fact: " + lcFactory);
        LOG.info(sb.toString());

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
