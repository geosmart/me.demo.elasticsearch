package me.demo.elasticsearch.test; /**
 * Created by Think on 2016/7/25.
 */

import com.google.common.base.Charsets;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import javax.inject.Inject;

import me.demo.elasticsearch.Application;
import me.demo.elasticsearch.service.ESService;

/**
 * Test class for ES RepeatDta
 * Created by geomart on 2016/7/25.
 *
 * @see ESService
 */
@SpringApplicationConfiguration(classes = Application.class)
@RunWith(SpringJUnit4ClassRunner.class)
@ActiveProfiles({"dev"})
@Configuration
public class ESDuplicateDocTest {
    @Value("${elasticsearch.indexName}")
    private String indexName;

    @Value("${elasticsearch.typeName}")
    private String typeName;

    @Inject
    ESService esService;


    @Before
    public void setup() {
        System.out.println("test begin...");
        test_deleteIndex();
    }

    /**
     * 根据Event_LOG_ID相同的数据去重
     * 获取Event_LOG_ID重复的ID-->删除重复数据
     */
    @Test
    public void removeDuplicateDoc() {
        List<String> ids = esService.queryDuplicateDoc(indexName, typeName, "name");
        for (String id : ids) {
            esService.deleteDocument(indexName, typeName, id);
        }
    }
    
    /**
     * 从CSV导入数据
     */
    @Test
    public void importDocFromCSV() throws IOException {
        String csvFile="D://cygwin/home//es-data.csv";
        String line="";
        try (BufferedReader br=new BufferedReader(new FileReader(csvFile))){
            while ((line=br.readLine())!=null){
                esService.createDocument_bulkProcess(indexName,typeName,line);
            }
        }
        //[110]: index [cloudx_web_v3], type [T_EVENT_LOG], id [AVYvkEs-RHdp3au7DoBL],
        // message [MapperParsingException[failed to parse];
        // nested: IllegalArgumentException[Malformed content, found extra data after parsing: START_OBJECT];]

//        List lines = FileUtils.readLines(new File("D://cygwin/home//es-data.csv"), Charsets.UTF_8.toString());
    }

    @Test
    public void test_deleteIndex() {
        esService.deleteIndex(indexName);
    }

    @After
    public void Teardown() {
        System.out.println("test stop...");
    }
}
