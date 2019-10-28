# -*- coding: UTF-8 -*-

import src.build_es_data as build_es_data
import src.load_student_test as load_student_test
import sys

def main(argv):
    if len(argv) != 2:
        print("输入参数有误!!! [tips: python main.py [args]]")
        exit()

    start_time = datetime.datetime.now()
    if argv[1] == "build_es_data":
        build_es_data.main()
        end_time = datetime.datetime.now()
        build_index_log.logger.info("costs: %s(s)" % ((end_time - start_time).total_seconds()))
        exit()
    elif argv[1] == "load_student_test":
        load_student_test.main()
        end_time = datetime.datetime.now()
        build_index_log.logger.info("costs: %s(s)" % ((end_time - start_time).total_seconds()))
        exit()
    else:
        print("输入参数有误!!! [tips: python main.py [args]] \n args can be: build_es_data")
        exit()


if __name__ == '__main__':
    main(sys.argv)