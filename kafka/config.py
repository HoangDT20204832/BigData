from pyspark.sql import SparkSession

class Config:
    def __init__(self,
                elasticsearch_host,
                elasticsearch_port,
                elasticsearch_input_json,
                elasticsearch_nodes_wan_only,
                hdfs_namenode):
        # Cấu hình kết nối Elasticsearch
        self.elasticsearch_conf = {
            'es.nodes': elasticsearch_host,
            'es.port': elasticsearch_port,
            "es.input.json": elasticsearch_input_json,
            "es.nodes.wan.only": elasticsearch_nodes_wan_only
        }
        # Đường dẫn tới HDFS Namenode
        self.hdfs_namenode = hdfs_namenode
        self.spark_app = None

    def get_elasticsearch_conf(self):
        # Trả về cấu hình Elasticsearch
        return self.elasticsearch_conf

    def get_hdfs_namenode(self):
        # Trả về đường dẫn HDFS Namenode
        return self.hdfs_namenode

    def initialize_spark_session(self, appName):
        # Khởi tạo SparkSession nếu chưa tồn tại
        if self.spark_app is None:
            self.spark_app = (SparkSession
                        .builder
                        .appName(appName)
                        .config("spark.jars", "elasticsearch-hadoop-7.17.5.jar")
                        .config("spark.driver.extraClassPath", "elasticsearch-hadoop-7.17.5.jar")
                        .config("spark.es.nodes", self.elasticsearch_conf["es.nodes"])
                        .config("spark.es.port", self.elasticsearch_conf["es.port"])
                        .config("spark.es.nodes.wan.only", "true")
                        .getOrCreate())
        return self.spark_app
