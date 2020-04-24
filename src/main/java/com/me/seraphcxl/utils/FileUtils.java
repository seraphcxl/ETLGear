package com.me.seraphcxl.utils;

import com.me.seraphcxl.Param;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import org.apache.commons.lang3.StringUtils;

/**
 * @author xiaoliangchen
 */
public class FileUtils {
    public static int saveDataXJsonToFile(String fileName, String context) {
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
                FileWriter fileWriter = new FileWriter(file.getAbsoluteFile());
                BufferedWriter bw = new BufferedWriter(fileWriter);
                bw.write(context);
                bw.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            result = 0;
        } while (false);
        return result;
    }

    public static int saveDataXJsonToFile(String fileName, StringBuilder strBuilder) {
        int result = -1;
        do {
            if (strBuilder == null || StringUtils.isBlank(fileName)) {
                break;
            }
            String context = strBuilder.toString();

            result = saveDataXJsonToFile(fileName, context);
        } while (false);
        return result;
    }
}
