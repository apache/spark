package io.radanalytics.operator;

public class Constants {

    public static String DEFAULT_SPARK_IMAGE = "quay.io/radanalyticsio/openshift-spark:2.4-latest";
    public static String DEFAULT_SPARK_APP_IMAGE = "quay.io/jkremser/openshift-spark:2.3-latest";
    public static final String OPERATOR_TYPE_UI_LABEL = "ui";
    public static final String OPERATOR_TYPE_MASTER_LABEL = "master";
    public static final String OPERATOR_TYPE_WORKER_LABEL = "worker";

    public static String getDefaultSparkImage() { 
        String ret = DEFAULT_SPARK_IMAGE;
        if (System.getenv("DEFAULT_SPARK_CLUSTER_IMAGE") != null) {
            ret = System.getenv("DEFAULT_SPARK_CLUSTER_IMAGE");
        }
        return ret;
    }

    public static String getDefaultSparkAppImage() { 
        String ret = DEFAULT_SPARK_APP_IMAGE;
        if (System.getenv("DEFAULT_SPARK_APP_IMAGE") != null) {
            ret = System.getenv("DEFAULT_SPARK_APP_IMAGE");
        }
        return ret;
    }
}
