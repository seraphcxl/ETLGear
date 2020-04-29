package com.me.seraphcxl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.me.seraphcxl.etlscriptgenerator.EtlScriptGenerator;
import com.me.seraphcxl.etlscriptgenerator.EtlScriptGeneratorFactory;
import com.me.seraphcxl.utils.JsonUtils;
import java.io.File;

/**
 * @author xiaoliangchen
 */
public class App {
    public static void main( String[] args ) {
        System.out.println( "Welcome to ETL gear!" );
        String jsonFilePath = "";

        jsonFilePath = App.class.getClassLoader().getResource("demo_block_merge_p3.json").getPath();
//        jsonFilePath = App.class.getClassLoader().getResource("demo_block_stream_p3.json").getPath();
//        jsonFilePath = App.class.getClassLoader().getResource("demo_full_merge_p3.json").getPath();

        if (args.length >= 1) {
            if (!jsonFilePath.contains(File.separator)) {
                // 如果是相对路径，补充成绝对路径
                jsonFilePath = System.getProperty("user.dir") + File.separator + args[0];
            }
            System.out.println(jsonFilePath);
        } else {
            System.out.println( "you should input a json file, we will show a demo." );
        }

        String s = JsonUtils.readJsonFile(jsonFilePath);
        JSONObject jsonObj = JSON.parseObject(s);
        String prettyJsonStr = JSON.toJSONString(jsonObj, SerializerFeature.PrettyFormat, SerializerFeature.WriteMapNullValue,
            SerializerFeature.WriteDateUseDateFormat);
        System.out.println(prettyJsonStr);

        do {
            if (Param.parseJobParam(jsonObj) != 0) {
                break;
            }

            if (Param.checkJsonParam(jsonObj) != 0) {
                break;
            }

            EtlScriptGenerator etlScriptGenerator = EtlScriptGeneratorFactory.createETLScriptGenerator(Param.jobType);
            int tmpResult = etlScriptGenerator.generateScript(jsonObj);
            if (tmpResult != 0) {
                System.out.println( "ETL gear ERROR!!!" );
            }
        } while (false);

        System.out.println( "ETL gear CLOSE!!!" );
    }
}
