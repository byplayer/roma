package jp.co.rakuten.rit.roma.command;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Session {
    private static final Logger LOG = LoggerFactory.getLogger(Session.class);

    private static final byte[] CRLF = new byte[] { '\r', '\n' };

    private static final int TMP_BUF_SIZE = 512;

    private SocketChannel channel;

    private OutputStream out;

    private ByteBuffer tmpBuf;

    private String[] commands;

    public Session(SocketChannel channel) {
        this.channel = channel;
        tmpBuf = ByteBuffer.allocate(TMP_BUF_SIZE);
        commands = null;
        try {
            out = new BufferedOutputStream(getSocketChannel().socket().getOutputStream());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public SocketChannel getSocketChannel() {
        return channel;
    }

    public void setCommands(String cmd) {
        commands = cmd.split(" ");
    }

    public String[] getCommands() {
        return commands;
    }

    public byte[] readBytes(int len) throws IOException {
        byte[] ret = new byte[len];
        int offset = tmpBuf.position();
        if (offset != 0) {
            if (offset >= len) {
                for (int i = 0; i < len; ++i) {
                    ret[i] = tmpBuf.get(i);
                }
                updateTmpBuffer(len);
            } else {
                for (int i = 0; i < offset; i++) {
                    ret[i] = tmpBuf.get(i);
                }
                tmpBuf.clear();
            }
        }
        InputStream in = getSocketChannel().socket().getInputStream();
        while (offset < len) {
            int n = in.read(ret, offset, len - offset);
            offset += n;
        }
        return ret;
    }

    public String readLine() throws IOException {
        int count = getSocketChannel().read(tmpBuf);
        if (count < 0) {
            throw new IOException("Cannot read any bytes");
        } else if (count == 0) {
            throw new IOException("Cannot read any bytes");
        }
        for (int i = 0; i < tmpBuf.position() - 1; i++) {
            if (tmpBuf.get(i) == CRLF[0] && tmpBuf.get(i + 1) == CRLF[1]) {
                String cmd = bufferToString(tmpBuf, i);
                updateTmpBuffer(i + 2);
                return cmd;
            }
        }
        return null;
    }

    public String blockingReadLine() throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(TMP_BUF_SIZE);
        InputStream in = getSocketChannel().socket().getInputStream();
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
        InputStream in = getSocketChannel().socket().getInputStream();
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

    private void updateTmpBuffer(int len) {
        ByteBuffer newBuf = ByteBuffer.allocate(TMP_BUF_SIZE);
        for (int i = len; i < tmpBuf.position(); ++i) {
            newBuf.put(tmpBuf.get(i));
        }
        tmpBuf = newBuf;
    }

    public void writeBytes(byte[] bytes) throws IOException {
        out.write(bytes);
        out.flush();
    }

    public void writeBytesNotFlush(byte[] bytes) throws IOException {
        out.write(bytes);
    }

    public void writeString(String data) throws IOException {
        writeBytes(data.getBytes());
    }

    public void writeStringNotFlush(String data) throws IOException {
        writeBytesNotFlush(data.getBytes());
    }

    public void close() throws IOException {
        if (channel != null) {
            channel.close();
        }
        tmpBuf.clear();
        tmpBuf = null;
    }
}
