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


""" 修复超过9张图片的微博 """
def fix_images_over_nine():
    try:
        wb = Weibo(1, '2010-01-01', 0, 0, 0, 0)

        weibo_update_list = []

        # count = 1
        # tmp_count = 0
        # random_pages = random.randint(1, 5)
        """建立数据库连接"""
        conn = pymysql.connect(host=dbinfo_host, user=dbinfo_user, passwd=dbinfo_password, db=dbinfo_db)
        cursor = conn.cursor()
        n = cursor.execute("SELECT WEIBO_ID FROM weibo_info w JOIN weibo_user_info u ON w.USER_ID = u.USER_ID WHERE LENGTH(PICS) - LENGTH(REPLACE(PICS, ',', '')) = 8 AND w.CREATE_TIME >= '2019-10-01 00:00:00' AND w.CREATE_TIME < '2019-12-01 00:00:00' AND u.FLAG = '1'  AND u.STATUS = '1' AND u.BAN <> '1' ORDER BY WEIBO_ID")
        conn.close()
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
                                weibo_update = {'weibo_id': row[0], 'pics': pics}
                                weibo_update_list.append(weibo_update)
                sleep(random.randint(1, 5))
                # if count - tmp_count == random_pages and count < n:
                #     sleep(random.randint(1, 3))
                #     tmp_count = count
                #     random_pages = random.randint(7, 20)
                # count = count + 1

        print "需要更新微博数：", len(weibo_update_list)
        conn = pymysql.connect(host=dbinfo_host, user=dbinfo_user, passwd=dbinfo_password, db=dbinfo_db)
        cursor = conn.cursor()
        for weibo_update in weibo_update_list:
            weibo_id = weibo_update['weibo_id']
            pics = weibo_update['pics']
            cursor.execute("UPDATE weibo_info SET PICS = %s WHERE WEIBO_ID = %s", (pics, weibo_id))
        conn.commit()
        conn.close()
    except Exception as e:
        print('Error: ', e)
        traceback.print_exc()
        # conn.rollback()
        # conn.close()


if __name__ == '__main__':
    fix_images_over_nine()
