package com.me.seraphcxl.utils;

import com.me.seraphcxl.Param;
import java.io.*;
import org.apache.commons.lang3.StringUtils;

/**
 * @author xiaoliangchen
 */
public class FileUtils {
    public static int saveResultToFile(String fileName, String context) {
        int result = -1;
        do {
            if (StringUtils.isBlank(fileName)
                || StringUtils.isBlank(context)
                || StringUtils.isBlank(Param.odpsWorkSpaceName)
                || StringUtils.isBlank(Param.bizName)
                || StringUtils.isBlank(Param.tableName)
            ) {
                break;
            }

            try {
                String dir = "ETLScript/";
                File file = new File(dir);
                if (!file.exists()) {
                    file.mkdir();;
                }

                dir += String.format("%s.%s.%s/"
                    , Param.odpsWorkSpaceName, Param.bizName, Param.tableName);
                file = new File(dir);
                if (!file.exists()) {
                    file.mkdir();;
                }

                fileName = dir + fileName;
                file = new File(fileName);
                if (file.exists()) {
                    file.delete();
                }
                file.createNewFile();
                writeFile(file, context);
            } catch (IOException e) {
                e.printStackTrace();
            }
            result = 0;
        } while (false);
        return result;
    }

    public static void readFile(File file) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF-8"));
        String line = null;
        while ((line = br.readLine()) != null) {
            System.out.println(line);
        }
    }

    public static void writeFile(File file, String content) throws IOException {
        FileOutputStream fos = new FileOutputStream(file);
        OutputStreamWriter osw = new OutputStreamWriter(fos, "UTF-8");
        osw.write(content);
        osw.flush();
    }

    public static void appendFile(File file, String content) throws IOException {
        // true to append
        OutputStreamWriter out = new OutputStreamWriter(
            new FileOutputStream(file, true),
            "UTF-8"
        );
        out.write(content);
        out.close();
    }
}
