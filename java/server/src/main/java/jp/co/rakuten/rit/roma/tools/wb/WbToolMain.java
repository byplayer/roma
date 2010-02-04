package jp.co.rakuten.rit.roma.tools.wb;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Calendar;

import jp.co.rakuten.rit.roma.tools.wb.WbCommon;

public class WbToolMain {
    public void run(String[] args) throws IllegalArgumentException,
            FileNotFoundException {
        String wbFile = args[0];
        File inFile = new File(wbFile);

        if (inFile.isFile()) {
            proc(inFile);
        } else if (inFile.isDirectory()) {
            File[] dirEnt = inFile.listFiles();

            for (File entFile : dirEnt) {
                if (entFile.isFile()) {
                    System.out.println("\n" + entFile.getName());
                    proc(entFile);
                }
            }
        } else {
            System.err.println("Can't Open " + inFile);
            return;
        }
    }

    private void proc(File wbFile) throws IllegalArgumentException,
            FileNotFoundException {
        WbCommon wb = new WbCommon(wbFile);
        String oFile = wbFile.getName() + ".out";
        FileOutputStream ofs = new FileOutputStream(oFile);

        String wbRec[] = null;

        try {
            final Calendar cal = Calendar.getInstance();
            while (wb.available()) {
                StringBuffer sBuff = new StringBuffer();
                wbRec = wb.next();
                if (wbRec != null && wbRec.length == 4) {

                    String value = wbRec[WbCommon.IDX_FIELD_VALUE];

                    if (value != null) {
                        String[] param = value.split("\\|");

                        if ("2".equals(param[0])
                                && WbCommon.CMD_INSERT
                                        .equals(wbRec[WbCommon.IDX_FIELD_CMD])) {
                            cal
                                    .setTimeInMillis(Long
                                            .parseLong(wbRec[WbCommon.IDX_FIELD_TIME]) * 1000);

                            sBuff.append(wbRec[WbCommon.IDX_FIELD_TIME] + ","
                                    + wbRec[WbCommon.IDX_FIELD_CMD] + ","
                                    + wbRec[WbCommon.IDX_FIELD_KEY] + ","
                                    + wbRec[WbCommon.IDX_FIELD_VALUE] + "\n");
                            ofs.write(sBuff.toString().getBytes());
                        }
                    }
                }
            }
            System.out.println("Total Record is " + wb.getRecoerdNo() + ".");
        } catch (IOException e) {
            // TODO
            e.printStackTrace();
        } finally {
            if (ofs != null) {
                try {
                    ofs.close();
                } catch (IOException e) {
                    //
                    e.printStackTrace();
                }
            }
        }
        wb.destroy();
    }

    public static void main(String[] args) {
        WbToolMain main = new WbToolMain();

        if (args.length == 1) {
            try {
                main.run(args);
            } catch (IllegalArgumentException e) {
                // TODO
                e.printStackTrace();
            } catch (FileNotFoundException e) {
                // TODO
                System.out.println("Error.");
                e.printStackTrace();
            }
        } else {
            System.out.println(
                    "Usage: java ToolMain ${write behind log file}");
        }
    }
}