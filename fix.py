#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import random
import traceback
from time import sleep

from tqdm import tqdm

import pymysql
import ConfigParser

from weibo import Weibo

"""读取配置文件"""
config_raw = ConfigParser.RawConfigParser()
config_raw.read('./weibo-crawler.conf')
# 读取数据库配置
dbinfo_host = config_raw.get('database', 'host')
dbinfo_user = config_raw.get('database', 'user')
dbinfo_password = config_raw.get('database', 'password')
dbinfo_db = config_raw.get('database', 'db')

"""建立数据库连接"""
conn = pymysql.connect(host=dbinfo_host, user=dbinfo_user, passwd=dbinfo_password, db=dbinfo_db)
cursor = conn.cursor()

""" 修复超过9张图片的微博 """
def fix_images_over_nine():
    try:
        wb = Weibo(1, '2010-01-01', 0, 0, 0, 0)

        count = 1
        tmp_count = 0
        random_pages = random.randint(1, 5)

        n = cursor.execute("SELECT WEIBO_ID FROM weibo_info WHERE LENGTH(PICS) - LENGTH( REPLACE(PICS,',','') ) = 8 AND CREATE_TIME >= '2019-09-01 00:00:00' AND CREATE_TIME < '2019-10-01 00:00:00' ORDER BY WEIBO_ID")
        if n:
            # for row in cursor.fetchall():
            for row in tqdm(cursor.fetchall(), desc='progress'):
                weibo_id = row[0]
                print '爬取微博id:' + weibo_id
                weibo = wb.get_long_weibo(weibo_id)
                print '微博内容：' + str(weibo)
                if weibo:
                    pics = weibo['pics']
                    if pics:
                        if pics.find(",") >= 0:
                            pics_arr = pics.split(",")
                            if pics_arr.__len__() > 9:
                                print "ok"
                                cursor.execute("UPDATE weibo_info SET PICS = %s WHERE WEIBO_ID = %s",
                                               (pics, weibo_id))
                if count - tmp_count == random_pages and count < n:
                    sleep(random.randint(1, 3))
                    tmp_count = count
                    random_pages = random.randint(7, 20)
                count = count + 1
        conn.commit()
        conn.close()
    except Exception as e:
        print('Error: ', e)
        traceback.print_exc()
        conn.rollback()
        conn.close()


if __name__ == '__main__':
    fix_images_over_nine()
