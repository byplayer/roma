package jp.co.rakuten.rit.roma.event;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A connection to a ROMA process.
 * 
 */
public class Connection {
    private static Logger LOG = LoggerFactory.getLogger(Connection.class);

    private static final byte[] CRLF = new byte[] { '\r', '\n' };

    private static final int TMP_BUF_SIZE = 512;

    private static byte[] one_byte = new byte[1];

    public Socket sock;

    public InputStream in;

    public OutputStream out;

    public Connection(final Socket sock) throws IOException {
        this.sock = sock;
        in = new BufferedInputStream(sock.getInputStream());
        out = new BufferedOutputStream(sock.getOutputStream());
    }

    public byte[] readBytes(int len) throws IOException {
        byte[] bytes = new byte[len];
        int offset = 0;
        while (offset < len) {
            int n = in.read(bytes, offset, len - offset);
            offset += n;
        }
        return bytes;
    }

    public String blockingReadLine() throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(TMP_BUF_SIZE);
        byte[] b1 = new byte[1];
        byte[] b2 = new byte[1];
        int len = 0;
        in.read(b1);
        while (true) {
            in.read(b2);
            if (b1[0] == CRLF[0] && b2[0] == CRLF[1]) {
                break;
            } else {
                buf.put(b1[0]);
                b1[0] = b2[0];
                len++;
            }
        }
        return bufferToString(buf, len);
    }

    public String gets() throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(TMP_BUF_SIZE);
        byte[] b1 = new byte[1];
        byte[] b2 = new byte[1];
        int len = 0;
        in.read(b1);
        while (true) {
            in.read(b2);
            if (b1[0] == CRLF[0] && b2[0] == CRLF[1]) {
                buf.put(CRLF[0]);
                buf.put(CRLF[1]);
                len = len + 2;
                break;
            } else {
                buf.put(b1[0]);
                b1[0] = b2[0];
                len++;
            }
        }
        return bufferToString(buf, len);
    }

    private static String bufferToString(ByteBuffer buf, int len) {
        byte[] bytes = new byte[len];
        for (int i = 0; i < len; ++i) {
            bytes[i] = buf.get(i);
        }
        return new String(bytes);
    }

    public void writeBytes(byte[] bytes) throws IOException {
        out.write(bytes);
        out.flush();
    }

    public void writeString(String data) throws IOException {
        writeBytes(data.getBytes());
    }

    /**
     * Close this connection.
     * 
     * @throws IOException -
     *             if closing this connection fails
     */
    public void close() throws IOException {
        if (in != null) {
            in.close();
        }
        if (out != null) {
            out.close();
        }
        if (sock != null) {
            sock.close();
        }
    }
}