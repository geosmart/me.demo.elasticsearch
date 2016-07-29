package me.demo.elasticsearch.test; /**
 * Created by geosmart on 2016/7/25.
 */

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
        esService.removeDuplicateDoc("event_log_id");
    }

    /**
     * 从CSV导入数据
     */
    @Test
    public void importDocFromCSV() {
        String csvFile = "D://cygwin/home//es-data.csv";
        esService.importDocFromCSV(csvFile);
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
