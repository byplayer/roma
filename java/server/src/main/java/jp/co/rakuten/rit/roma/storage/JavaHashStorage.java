package jp.co.rakuten.rit.roma.storage;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Simple implementation of ROMA storage.
 *
 * This class implements ROMA storage using {@link java.util.HashMap}.
 */
public class JavaHashStorage extends BasicStorage {
    protected HashDB openNode(String path) throws StorageException {
        return new JavaHashDB();
    }

    protected void closeNode(HashDB node) throws StorageException {
    }

    private static class JavaHashDB extends HashDB {
        private Map<KeyWrapper, byte[]> values;

        public JavaHashDB() {
            values = new HashMap<KeyWrapper, byte[]>();
        }

        @Override
        public byte[] get(byte[] key) {
            return values.get(new KeyWrapper(key));
        }

        @Override
        public Iterator<byte[]> keyIterator() {
            return new Iterator<byte[]>() {
                private Iterator<KeyWrapper> itr = values.keySet().iterator();

                @Override
                public boolean hasNext() {
                    return itr.hasNext();
                }

                @Override
                public byte[] next() {
                    return itr.next().key;
                }

                @Override
                public void remove() {
                    itr.remove();
                }
            };
        }

        @Override
        public boolean put(byte[] key, byte[] value) {
            values.put(new KeyWrapper(key), value);
            return true;
        }

        @Override
        public boolean remove(byte[] key) throws StorageException {
            return values.remove(new KeyWrapper(key)) != null;
        }
    }

    private static class KeyWrapper {
        private byte[] key;

        public KeyWrapper(byte[] key) {
            this.key = key;
        }

        public int hashCode() {
            return Arrays.hashCode(key);
        }

        public boolean equals(Object o) {
            if (o instanceof KeyWrapper) {
                return Arrays.equals(key, ((KeyWrapper) o).key);
            }
            return false;
        }
    }
}
