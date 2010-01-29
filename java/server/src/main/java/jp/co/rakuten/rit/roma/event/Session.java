package jp.co.rakuten.rit.roma.event;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.logging.Logger;

public class Session {
    private static final Logger LOG = Logger.getLogger(Session.class.getName());

    private static final byte[] CRLF = new byte[] { '\r', '\n' };

    private static final int TMP_BUF_SIZE = 512;

    private SocketChannel channel;

    private ByteBuffer tmpBuf;

    private String[] commands;

    public Session(SocketChannel channel) {
        this.channel = channel;
        tmpBuf = ByteBuffer.allocate(TMP_BUF_SIZE);
        commands = null;
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

    public String readLine() throws IOException {
        int count = getSocketChannel().read(tmpBuf);
        // if (count < 0) {
        // key.interestOps(key.interestOps() & ~SelectionKey.OP_READ);
        // LOG.info("close connection: " + getSocketChannel());
        // EventHandler.removeSession(key);
        // close();
        // return null;
        // } else if (count == 0) {
        // key.interestOps(key.interestOps() & ~SelectionKey.OP_READ);
        // return null;
        // }
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
                // System.out.println("b1: " + (char)b1[0]);
                len++;
            }
        }
        String s = bufferToString(buf, len);
        System.out.println("#### s: " + s);
        return s;
        // return bufferToString(buf, len);
    }

    private static String bufferToString(ByteBuffer buf, int len) {
        byte[] bytes = new byte[len];
        for (int i = 0; i < len; ++i) {
            // System.out.println("i: " + i + ", b: " + buf.get(i));
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

    public void writeBytes(byte[] bytes) throws IOException {
        OutputStream out = getSocketChannel().socket().getOutputStream();
        out.write(bytes);
        // out.flush();
    }

    public void writeString(String data) throws IOException {
        writeBytes(data.getBytes());
    }

    public void close() throws IOException {
        if (channel != null) {
            channel.close();
        }
        tmpBuf.clear();
        tmpBuf = null;
    }
}