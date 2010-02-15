package jp.co.rakuten.rit.roma.storage;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.io.OutputStream;

public class Serializer {
    public void writeTo(Object o, DataOutput out) {
        try {
            ByteArrayOutputStream barray = new ByteArrayOutputStream();
            ObjectOutputStream outStream = new NoHeaderObjectOutputStream(
                    barray);
            outStream.close();
            System.out.println(barray.toByteArray().length);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static class NoHeaderObjectOutputStream extends ObjectOutputStream {
        public NoHeaderObjectOutputStream(OutputStream out) throws IOException {
            super(out);
        }

        protected void writeStreamHeader() {
        }
    }

    public static void main(String[] args) {
        new Serializer().writeTo(new String(), null);
    }
}
