package jp.co.rakuten.rit.roma.storage;

public class StorageException extends Exception {

    private static final long serialVersionUID = 7668692953357915110L;

    public StorageException(String message) {
        super(message);
    }

    public StorageException(String message, Exception cause) {
        super(message, cause);
    }
}
