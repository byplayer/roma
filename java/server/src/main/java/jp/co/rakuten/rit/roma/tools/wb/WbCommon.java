package jp.co.rakuten.rit.roma.tools.wb;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

public class WbCommon {
    /*
     * FIELD INDEX
     */
    public static final int IDX_FIELD_TIME = 0;
    public static final int IDX_FIELD_CMD = 1;
    public static final int IDX_FIELD_KEY = 2;
    public static final int IDX_FIELD_VALUE = 3;

    /*
     * COMMAND
     */
    public static final String CMD_INSERT = "1";
    public static final String CMD_DELETE = "2";
    public static final String CMD_CLEAR = "3";

    private FileInputStream fis = null;
    private BufferedInputStream bis = null;

    private int recNo;

    /**
     * @param wb_file
     * @throws IllegalArgumentException
     * @throws FileNotFoundException
     */
    public WbCommon(final String wb_file) throws IllegalArgumentException,
            FileNotFoundException {
        File infile = new File(wb_file);

        if (infile.isFile()) {
            fromFile(infile);
        }
    }

    /**
     * @param wb_file
     *            WriteBehind data
     * @throws IllegalArgumentException
     * @throws FileNotFoundException
     */
    public WbCommon(final File wb_file) throws IllegalArgumentException,
            FileNotFoundException {
        fromFile(wb_file);
    }

    /**
     * @param wb_file
     *            WriteBehind data
     * @throws IllegalArgumentException
     * @throws FileNotFoundException
     */
    protected void fromFile(final File wb_file)
            throws IllegalArgumentException, FileNotFoundException {
        try {
            if (wb_file == null) {
                throw new IllegalArgumentException("argument is null.");
            }
            fis = new FileInputStream(wb_file);
            bis = new BufferedInputStream(fis);
            recNo = 0;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            throw e;
        }
    }

    /**
     * @return items(time,cmd,key,value)
     * @throws IOException
     */
    public final String[] next() throws IOException {
        String[] ret = new String[4];
        long last;
        int cmd;
        int klen, vlen;
        byte[] bBuf = new byte[4];
        byte[] keyBuf = null;
        byte[] vBuf = null;

        if (bis == null) {
            throw new IllegalStateException("argument is null.");
        }
        try {
            if (bis.read(bBuf, 0, 4) != 4) {
                throw new IOException("failed to read last-field.");
            }
            last = byte2Long(bBuf, 4);
            ret[0] = new Long(last).toString();

            if (bis.read(bBuf, 0, 2) != 2) {
                throw new IOException("failed to read last-field.");
            }
            cmd = byte2Int(bBuf, 2);
            ret[1] = new Integer(cmd).toString();

            if (bis.read(bBuf, 0, 4) != 4) {
                throw new IOException("failed to read last-field.");
            }
            klen = byte2Int(bBuf, 4);

            keyBuf = new byte[klen];
            if (klen != bis.read(keyBuf, 0, klen)) {
                throw new IOException("failed to read key.");
            }
            ret[2] = new String(keyBuf);

            if (bis.read(bBuf, 0, 4) != 4) {
                throw new IOException("failed to read last-field.");
            }
            vlen = byte2Int(bBuf, 4);
            vBuf = new byte[vlen];
            if (vlen != bis.read(vBuf, 0, vlen)) {
                throw new IOException("failed to read value.");
            }
            ret[3] = new String(vBuf);
            recNo++;
        } catch (IOException e) {
            // TODO
            e.printStackTrace();
            throw e;
        }
        return ret;
    }

    /**
     * @return
     * @throws IOException
     */
    public boolean available() throws IOException {
        try {
            if (bis != null && bis.available() > 0) {
                return true;
            }
        } catch (IOException e) {
            e.printStackTrace();
            destroy();
            throw e;
        }
        return false;
    }

    /**
     * @return current record-no.
     */
    public final int getRecoerdNo() {
        return recNo;
    }

    /**
     * 
     */
    public final void destroy() {
        if (fis != null) {
            try {
                fis.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            fis = null;
        }
        if (bis != null) {
            try {
                bis.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            bis = null;
        }
    }

    /*
     * unsigned bytes to int.
     */
    /**
     * @param bBuf
     *            unsigned int byte data(Big Endians)
     * @param len
     *            length
     * @return
     */
    protected static int byte2Int(byte[] bBuf, int len) {
        int ret = 0;

        for (int i = 0; i < len; i++) {
            for (int j = 7; j >= 0; j--) {
                ret = (ret << 1);
                if (((bBuf[i] >> j) & 1) == 1) {
                    ret = ret | 1;
                }
            }
        }
        return ret;
    }

    /*
     * unsigned bytes to long.
     */
    /**
     * @param bBuf
     *            unsigned long byte data(Big Endians)
     * @param len
     * @return long value
     */
    protected static long byte2Long(final byte[] bBuf, final int len) {
        long ret = 0;

        for (int i = 0; i < len; i++) {
            for (int j = 7; j >= 0; j--) {
                ret = (ret << 1);
                if (((bBuf[i] >> j) & 1) == 1) {
                    ret = ret | 1;
                }
            }
        }
        return ret;
    }

    /*
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "WbCommon [fis=[" + bis.toString() + "], toString()="
                + super.toString() + "]";
    }
}