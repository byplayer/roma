package jp.co.rakuten.rit.roma.storage;

@SuppressWarnings("serial")
public class StorageException extends Exception {

    public StorageException(final String msg) {
        super(msg);
    }

    public StorageException(final String msg, final Exception cause) {
        super(msg, cause);
    }
}
