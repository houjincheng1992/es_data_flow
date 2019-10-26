# -*- coding: UTF-8 -*-

from configparser import ConfigParser
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from src.logger import Logger
import MySQLdb
import datetime
import json


title = [
	"id", "class_id", "student_no", "name", "sex", "nationality_id", "student_source",
	"birthday", "identity_card", "address", "is_deleted", "gmt_create", "gmt_modify",
	"creator", "modifier", "is_import", "comment", "class_no", "class_name", "grade_id",
	"grade_no", "grade_name", "school_id", "school_code", "school_name"
]

config = ConfigParser(allow_no_value=True)
config.read("config/prod.conf")

build_index_log = Logger(config.get("logging", "build_index_log"), level = 'info')
build_index_error_log = Logger(config.get("logging", "build_index_error_log"), level = 'warning')
build_index_reload_log = Logger(config.get("logging", "build_index_reload_log"), level = 'info')


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


def build_es_data(db, es):
	"""
		构建es索引数据
	"""
	cursor = db.cursor()
	start = 0
	increase = 1000

	while True:
		cursor.execute("select * from student_info order by id limit %s, %s" % (start, increase))
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
			data_dict.update({"sid" : "%s_%s" % (data_dict["school_id"], data_dict["student_no"])})
			tmp_dict = {
				"_index" : "education_student_info",
				"_type" : "_doc",
				"_id" : data_dict["sid"],
				"_source" : data_dict
			}
			insert_list.append(tmp_dict)

		try:
			result = helpers.bulk(es, insert_list, index = "education_student_info", raise_on_error = True)
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

	build_es_data(db, es)
	db.close()


if __name__ == '__main__':
	start_time = datetime.datetime.now()
	main()
	end_time = datetime.datetime.now()
	build_index_log.logger.info("index: %s\tcosts: %s(s)" % (education_student_info, (end_time - start_time).total_seconds()))