package me.demo.elasticsearch.config;

/**
 * Created by geosmart on 5/6/16.
 */
public final class Constants {

    // for spring profile
    public static final String SPRING_PROFILE_DEVELOPMENT = "dev";
    public static final String SPRING_PROFILE_PRODUCTION = "prod";

    /**
     * 操作类型
     */
    public enum  OPERATION_TYPE{
        /**
         * 删除ES中重复Doc
         */
        REMOVE_DUPLICATE_DOC,
        /**
         * 导入MongoDB数据到ES
         */
        IMPORT_MONGODB_DATA
    };

    private Constants() {

    }
}
