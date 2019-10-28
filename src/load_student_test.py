# -*- coding: UTF-8 -*-

from configparser import ConfigParser
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from src.logger import Logger
import MySQLdb
import datetime
import json

config = ConfigParser(allow_no_value=True)
config.read("config/prod.conf")

build_index_log = Logger(config.get("logging", "load_stu_log"), level = 'info')
build_index_error_log = Logger(config.get("logging", "load_stu_err_log"), level = 'warning')
build_index_reload_log = Logger(config.get("logging", "load_stu_reload_log"), level = 'info')

title = [
    "id", "student_id", "studentno", "is_score", "shenGao", "shenGaoPingFen", "shenGaoDengJi", "tiZhong",
    "tiZhongPingFen", "tiZhongDengJi", "feiHuoLiang", "feiHuoLiangPingFen", "feiHuoLiangDengJi", "pao1000",
    "pao1000Miao", "pao1000PingFen", "pao1000DengJi", "pao800", "pao800Miao", "pao800PingFen", "pao800DengJi",
    "taiJie", "taiJiePingFen", "taiJieDengJi", "pao400", "pao400Miao", "pao400PingFen", "pao400DengJi",
    "pao50x8", "pao50x8Miao", "pao50x8PingFen", "pao50x8DengJi", "yangWoQiZuo", "yangWoQiZuoPingFen",
    "yangWoQiZuoDengJi", "zhiShiXinQiu", "zhiShiXinQiuPingFen", "zhiShiXinQiuDengJi", "touShaBao", 
    "touShaBaoPingFen", "touShaBaoDengJi", "zwtqq", "zwtqqPingFen", "zwtqqDengJi", "woLi", "woLiPingFen",
    "woLiDengJi", "pao50", "pao50PingFen", "pao50DengJi", "pao25x2", "pao25x2PingFen", "pao25x2DengJi",
    "liDingTiaoYuan", "liDingTiaoYuanPingFen", "liDingTiaoYuanDengJi", "tiaoSheng", "tiaoShengPingFen",
    "tiaoShengDengJi", "lanQiuYunQiu", "lanQiuYunQiuPingFen", "lanQiuYunQiuDengJi", "zuQiuYunQiu",
    "zuQiuYunQiuPingFen", "zuQiuYunQiuDengJi", "paiQiuDianQiu", "paiQiuDianQiuPingFen", "paiQiuDianQiuDengJi",
    "tiJianZi", "tiJianZiPingFen", "tiJianZiDengJi", "zuQiuDianQiu", "zuQiuDianQiuPingFen", "zuQiuDianQiuDengJi",
    "yinTiXiangShang", "yinTiXiangShangPingFen", "yinTiXiangShangDengJi", "shiLi", "shiLiPingFen",
    "shiLiDengJi", "jiangChengXiangMuBianHao", "standardScore", "additionalScore", "totalScore",
    "totalScoreDengJi", "BMI", "COMMENT", "pao800addscore", "pao1000addscore", "ywqzaddscore",
    "ytxsaddscore", "tiaoShengaddscore", "school_id", "school_code", "school_name", "grade_id",
    "class_id", "lyslz", "lysly", "lyslzDengJi", "lyslyDengJi", "cjjzz", "cjjzy", "cjjzzDengJi",
    "cjjzyDengJi", "qgbzz", "qgbzy", "qgbzzDengJi", "qgbzyDengJi", "GMT_CREATE", "GMT_MODIFY", "CREATER",
    "MODIFIER"
]


class MysqlParams(object):
    """
        mysql 参数配置
    """
    def __init__(self, config):
        """
            init
        """
        self.ip = config.get("mysqldb", "ip")
        self.port = int(config.get("mysqldb", "port"))
        self.username = config.get("mysqldb", "username")
        self.password = config.get("mysqldb", "password")
        self.dbname = config.get("mysqldb", "dbname")
        self.charset = config.get("mysqldb", "charset")


class ElasticParams(object):
    """
        es 参数配置
    """
    def __init__(self, config):
        """
            init
        """
        self.ip = config.get("elasticsearch", "ip").split(";")
        self.port = int(config.get("elasticsearch", "port"))
        self.username = config.get("elasticsearch", "username")
        self.password = config.get("elasticsearch", "password")


class DatetimeEncoder(json.JSONEncoder):
    def default(self, obj):
        try:
            return super(DatetimeEncoder, obj).default(obj)
        except TypeError:
            return str(obj)

def load_student_data(db, es):
    """
        导入学生测试数据
    """
    cursor = db.cursor()
    start = 0
    increase = 1000

    while True:
        cursor.execute("select * from test_data order by id limit %s, %s" % (start, increase))
        read_data = cursor.fetchall()
        if len(read_data) == 0:
            break
        start += increase

        insert_list = []
        for item in read_data:
            if len(item) != len(title):
                print("error: %s" % item)
                continue
            data_dict = dict()
            for index in range(0, len(item)):
                if title[index] == "id":
                    continue
                data_dict.update({title[index] : item[index]})
            data_dict.update({"sid" : "%s_%s" % (data_dict["school_id"], data_dict["studentno"])})

            insert_list.append(
                {
                    'update': {
                        '_index': 'education_student_info',
                        '_id': data_dict["sid"],
                        "_type" : "_doc",
                    }
                }
            )

            insert_list.append(
                {
                    'doc': {
                        'test_data': data_dict
                    }
                }
            )

        try:
            result = es.bulk(insert_list, index = "education_student_info")
            print(result)
        except Exception as e:
            build_index_error_log.logger.warning("%s" % e)
            build_index_reload_log.logger.info("%s" % json.dumps(insert_list, cls = DatetimeEncoder))

    cursor.close()
    return

def main():
    mysqlParams = MysqlParams(config)
    esParams = ElasticParams(config)

    db = MySQLdb.connect(host = mysqlParams.ip, port = mysqlParams.port,
            user = mysqlParams.username, passwd = mysqlParams.password, db = mysqlParams.dbname,
            charset = mysqlParams.charset)

    es = Elasticsearch(
        esParams.ip,
        http_auth = (esParams.username, esParams.password),
        port = esParams.port,
        verify_certs=True,
        timeout = 1000
    )

    load_student_data(db, es)
    db.close()


if __name__ == '__main__':
    start_time = datetime.datetime.now()
    main()
    end_time = datetime.datetime.now()
    build_index_log.logger.info("index: %s\tcosts: %s(s)" % (education_student_info, (end_time - start_time).total_seconds()))