# -*- coding: UTF-8 -*-

import src.build_es_data as build_es_data
import sys

def main(argv):
    if len(argv) != 2:
        print("输入参数有误!!! [tips: python main.py [args]]")
        exit()
    if argv[1] == "build_es_data":
        build_es_data.main()
        exit()
    else:
        print("输入参数有误!!! [tips: python main.py [args]] \n args can be: build_es_data")
        exit


if __name__ == '__main__':
    main(sys.argv)