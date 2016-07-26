/**
 * Created by Think on 2016/7/25.
 */

import com.alibaba.fastjson.JSON;

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

import java.io.File;
import java.io.IOException;
import java.util.List;

import javax.inject.Inject;

import me.demo.elasticsearch.Application;
import me.demo.elasticsearch.service.ESService;

import static org.apache.commons.io.FileUtils.readFileToString;

/**
 * Test class for ESService
 * Created by geomart on 2016/7/25.
 *
 * @see me.demo.elasticsearch.service.ESService
 */
@SpringApplicationConfiguration(classes = Application.class)
@RunWith(SpringJUnit4ClassRunner.class)
@ActiveProfiles({"dev"})
@Configuration
public class ESServiceTest { 
    @Value("${elasticsearch.indexName}")
    private String indexName;

    @Value("${elasticsearch.typeName}")
    private String typeName;


    @Value("${elasticsearch.port}")
    private String  port;

    @Inject
    ESService esService;

    int loop = 10000;

    @Before
    public void setup() {
        System.out.println("test begin...");
//        test_deleteIndex();
    }

    @Test
    public void test_getConfig() {
        System.out.println(indexName);
        System.out.println(port);
    }

    @Test
    public void test_createIndex() {
        esService.createIndex(indexName, null);
    }

    @Test
    public void test_setIndexMapping() {
        esService.setIndexMapping(indexName,typeName,getMappingJson());
    }

    /**
     * api插入10万条,7m18s
     */
    @Test
    public void test_createDocument() {
        List<Object> array = JSON.parseArray(getEventLogJson());
        for (int i = 0; i < loop; i++) {
            esService.createDocument(indexName, typeName, array);
        }
    }

    /**
     * bulkRequest插入10万条,1m10s
     */
    @Test
    public void test_createDocument_bulkRequest() {
        List<Object> array = JSON.parseArray(getEventLogJson());
        for (int i = 0; i < loop; i++) {
            esService.createDocument_bulkRequest(indexName, typeName, array);
        }
    }

    /**
     * bulkProcess插入10万条（13M）,1线程7.5s；4线程4s
     */
    @Test
    public void test_createDocument_bulkProcess() {
        List<Object> array = JSON.parseArray(getEventLogJson());
        for (int i = 0; i < loop; i++) {
            esService.createDocument_bulkProcess(indexName, typeName, array);
        }
    }

    @Test
    public void test_deleteDocument() {
        esService.deleteDocument("tutorial", "TEST", "AVYgJ7n2_fDrwGO2UeRH");
        esService.deleteDocument("tutorial", "TEST", "AVYgKKzc_fDrwGO2UeRJ");
    }
    @Test
    public void test_deleteIndex() {
        esService.deleteIndex(indexName); 
    }


    @Test
    public void getDefaultAnalyzerMapping() {
        esService.getDefaultAnalyzerMapping(indexName);
    }

    /**
     * 测试数据
     */
    private String getEventLogJson() {
        String path = System.getProperty("user.dir") + "//src//test//resources//data//eventlogs.json";
        File file = new File(path);
        String data = null;
        try {
            data = readFileToString(file, "UTF-8");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return data;
    }

    /**
     * 测试数据
     */
    private String getMappingJson() {
        String path = System.getProperty("user.dir") + "//src//test//resources//data//mapping.json";
        File file = new File(path);
        String data = null;
        try {
            data = readFileToString(file, "UTF-8");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return data;
    }
    @After
    public void Teardown() {
        System.out.println("test stop...");
    }
}
