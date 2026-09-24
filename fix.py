#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import random
import traceback
from time import sleep

from tqdm import tqdm

import pymysql
import configparser

from weibo import Weibo

from datetime import datetime, timedelta

"""读取配置文件"""
config_raw = configparser.RawConfigParser()
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
        conn = pymysql.connect(host=dbinfo_host, user=dbinfo_user, password=dbinfo_password, database=dbinfo_db)
        cursor = conn.cursor()
        n = cursor.execute("SELECT WEIBO_ID FROM weibo_info w JOIN weibo_user_info u ON w.USER_ID = u.USER_ID WHERE LENGTH(PICS) - LENGTH(REPLACE(PICS, ',', '')) = 8 AND w.CREATE_TIME >= '2020-04-01 00:00:00' AND w.CREATE_TIME < '2020-05-15 00:00:00' AND u.FLAG = '1'  AND u.STATUS = '1' AND u.BAN <> '1' ORDER BY WEIBO_ID")
        conn.close()
        if n:
            # for row in cursor.fetchall():
            for row in tqdm(cursor.fetchall(), desc='progress'):
                weibo_id = row[0]
                print('爬取微博id:' + weibo_id)
                weibo = wb.get_long_weibo(weibo_id)
                print('微博内容：' + str(weibo))
                if weibo:
                    pics = weibo['pics']
                    if pics:
                        if pics.find(",") >= 0:
                            pics_arr = pics.split(",")
                            if pics_arr.__len__() > 9:
                                print("ok")
                                weibo_update = {'weibo_id': row[0], 'pics': pics}
                                weibo_update_list.append(weibo_update)
                sleep(random.randint(1, 5))
                # if count - tmp_count == random_pages and count < n:
                #     sleep(random.randint(1, 3))
                #     tmp_count = count
                #     random_pages = random.randint(7, 20)
                # count = count + 1

        print("需要更新微博数：", len(weibo_update_list))
        conn = pymysql.connect(host=dbinfo_host, user=dbinfo_user, password=dbinfo_password, database=dbinfo_db)
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


def test():
    try:
        # wb = Weibo(1, '2010-01-01', 0, 0, 0, 0)
        # wb.mysql_insert_sql("INSERT INTO test1_tab(NAME, INFO) VALUES(%s, %s)", ('gkd', '666'))
        connection = pymysql.connect(host=dbinfo_host, user=dbinfo_user, password=dbinfo_password, database=dbinfo_db)
        cursor = connection.cursor()
        screen_name = '测试啊'
        avatar_hd = '头像'
        error_since_weibo_id = 1234
        error_since_weibo_id = 'null'
        user_id = 0
        cursor.execute(
            "UPDATE weibo_user_info_test SET NICK_NAME = '%s', AVATAR_URL = '%s', ERROR_SINCE_WEIBO_ID = %s WHERE USER_ID = '%s'"
            % (screen_name, avatar_hd, error_since_weibo_id, user_id))
        connection.commit()
    except Exception as e:
        print('Error: ', e)
        traceback.print_exc()
        # conn.rollback()
        # conn.close()


def fix_weibo_time():
    try:
        wb = Weibo(1, '2010-01-01', 0, 0, 0, 0)

        weibo_update_list = []

        count = 1
        # tmp_count = 0
        # random_pages = random.randint(1, 5)
        """建立数据库连接"""
        conn = pymysql.connect(host=dbinfo_host, user=dbinfo_user, password=dbinfo_password, database=dbinfo_db)
        cursor = conn.cursor()
        # n = cursor.execute("SELECT WEIBO_ID FROM weibo_info w WHERE `CREATE_TIME` = '0000-00-00 00:00:00'")
        n = cursor.execute("SELECT WEIBO_ID FROM weibo_info w WHERE `DATA_ID` IN (155959,156002,156017,156041,156042)")
        conn.close()
        if n:
            # for row in cursor.fetchall():
            for row in tqdm(cursor.fetchall(), desc='progress'):
                weibo_id = row[0]
                print('爬取微博id:' + weibo_id)
                weibo = wb.get_long_weibo(weibo_id)
                print('微博内容：' + str(weibo))
                if weibo:
                    created_at = weibo['created_at']
                    if created_at:
                        created_at = datetime.strptime(created_at, "%a %b %d %H:%M:%S +0800 %Y").strftime("%Y-%m-%d %H:%M:%S")
                        print("ok")
                        weibo_update = {'weibo_id': row[0], 'create_time': created_at}
                        weibo_update_list.append(weibo_update)
                if count % 5 == 1:
                    sleep(random.randint(1, 2))
                # if count - tmp_count == random_pages and count < n:
                #     sleep(random.randint(1, 3))
                #     tmp_count = count
                #     random_pages = random.randint(7, 20)
                count = count + 1

        print("需要更新微博数：", len(weibo_update_list))
        conn = pymysql.connect(host=dbinfo_host, user=dbinfo_user, password=dbinfo_password, database=dbinfo_db)
        cursor = conn.cursor()
        for weibo_update in weibo_update_list:
            weibo_id = weibo_update['weibo_id']
            create_time = weibo_update['create_time']
            cursor.execute("UPDATE weibo_info SET CREATE_TIME = %s WHERE WEIBO_ID = %s", (create_time, weibo_id))
        conn.commit()
        conn.close()
    except Exception as e:
        print('Error: ', e)
        traceback.print_exc()
        # conn.rollback()
        # conn.close()


""" 修复微博信息 """
def fix_weibo_info(user_id):
    try:
        wb = Weibo(1, '2010-01-01', 0, 0, 0, 0)

        weibo_update_list = []

        count = 0
        """建立数据库连接"""
        conn = pymysql.connect(host=dbinfo_host, user=dbinfo_user, password=dbinfo_password, database=dbinfo_db)
        cursor = conn.cursor()
        n = cursor.execute("SELECT WEIBO_ID, PICS, VIDEO_URL, LIVE_PHOTO, PIC_INFO, PAGE_INFO, BID FROM weibo_info w where BID IS NULL "
                           "AND w.USER_ID = '" + str(user_id) + "' ORDER BY WEIBO_ID")
        # n = cursor.execute(
        #     "SELECT WEIBO_ID, PICS, VIDEO_URL, LIVE_PHOTO, PIC_INFO, PAGE_INFO, BID FROM weibo_info w where WEIBO_ID = '4620417207109487' ")
        rows = cursor.fetchall()
        conn.close()
        if n:
            for row in tqdm(rows, desc='progress'):
                # print("时间1:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                weibo_id = row[0]
                video_url_db = row[2]
                live_photo_db = row[3]
                pic_info_db = row[4]
                page_info_db = row[5]
                bid_db = row[6]
                # VIDEO_URL、LIVE_PHOTO、PIC_INFO、PAGE_INFO、BID都不为空的无需修复
                if video_url_db and live_photo_db and pic_info_db and page_info_db and bid_db:
                    continue
                print('爬取微博id:' + weibo_id)
                try:
                    # print("时间2:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                    weibo = wb.get_long_weibo_fast(weibo_id)
                    # print("时间3:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                    # weibo = None
                except Exception as e:
                    print('Error: ', e)
                    # sleep(random.randint(1, 5))
                    continue
                if weibo:
                    weibo_update = {'weibo_id': weibo_id}
                    # 只补空缺：库里为空且新数据非空才更新
                    for field, db_value in (('video_url', video_url_db), ('live_photo', live_photo_db),
                                            ('pic_info', pic_info_db), ('page_info', page_info_db),
                                            ('bid', bid_db)):
                        new_value = weibo.get(field)
                        if not db_value and new_value:
                            weibo_update[field] = new_value
                    # 编辑记录：新数据不为空即覆盖更新，不管库里原字段有没有值
                    edit_rec = weibo.get('edit_rec')
                    if edit_rec:
                        weibo_update['edit_rec'] = edit_rec
                    if len(weibo_update) > 1:
                        print('可更新内容：' + str(weibo_update))
                        weibo_update_list.append(weibo_update)
                # sleep(random.randint(1, 3))
                # sleep(1)
                count = count + 1
                # 每爬取5次，额外休眠2~5秒
                if count % 5 == 0:
                    sleep(random.randint(1, 3))

        print("需要更新微博数：", len(weibo_update_list))
        conn = pymysql.connect(host=dbinfo_host, user=dbinfo_user, password=dbinfo_password, database=dbinfo_db)
        cursor = conn.cursor()
        field_to_column = {'video_url': 'VIDEO_URL', 'live_photo': 'LIVE_PHOTO',
                           'pic_info': 'PIC_INFO', 'page_info': 'PAGE_INFO', 'bid': 'BID',
                           'edit_rec': 'EDIT_REC'}
        for weibo_update in weibo_update_list:
            weibo_id = weibo_update['weibo_id']
            set_parts, params = [], []
            for field, value in weibo_update.items():
                if field == 'weibo_id':
                    continue
                set_parts.append("%s = %%s" % field_to_column[field])
                params.append(value)
            sql = "UPDATE weibo_info SET " + ", ".join(set_parts) + " WHERE WEIBO_ID = %s"
            cursor.execute(sql, params + [weibo_id])
        conn.commit()
        conn.close()
    except Exception as e:
        print('Error: ', e)
        traceback.print_exc()
        # conn.rollback()
        # conn.close()


""" 修复微博信息 """
def fix_weibo_info_pro(user_id):
    try:
        wb = Weibo(1, '2010-01-01', 0, 0, 0, 0)

        # user_id = 3748917252
        since_weibo_id = ''

        conn = pymysql.connect(host=dbinfo_host, user=dbinfo_user, password=dbinfo_password, database=dbinfo_db)
        cursor = conn.cursor()
        n = cursor.execute("SELECT WEIBO_ID, CREATE_TIME, TEXT FROM weibo_info WHERE USER_ID = '" + str(user_id) + "' ORDER BY CREATE_TIME DESC LIMIT 1")
        rows = cursor.fetchall()
        conn.close()
        if n:
            row = rows[0]
            print('最新微博' + str(row))
            weibo_id = row[0]
            if weibo_id:
                since_weibo_id = weibo_id

        wb.initialize_info(user_id)

        wb.get_user_info()
        page_count = wb.get_page_count()
        wb.print_user_info()

        # 页码
        page = 1
        page1 = 0
        random_pages = random.randint(1, 5)

        with tqdm(total=page_count) as progress_bar:
            while True:
                result = wb.get_one_page(since_weibo_id, None, None, False)
                result_code = result['code']
                # 还有下一页微博需要爬
                if result_code == 1:
                    next_since_weibo_id = result['data']
                    if (next_since_weibo_id is not None) and (next_since_weibo_id != 0):
                        since_weibo_id = next_since_weibo_id
                    else:
                        break
                # 没有微博需要爬
                elif result_code == 0:
                    error_since_weibo_id = 'null'
                    break
                # 爬取微博出错，记录错误位置，下次爬取时，从该位置重试
                elif result_code == 2:
                    error_since_weibo_id = since_weibo_id
                    break

                # 通过加入随机等待避免被限制。爬虫速度过快容易被系统限制(一段时间后限
                # 制会自动解除)，加入随机等待模拟人的操作，可降低被系统限制的风险。默
                # 认是每爬取1到5页随机等待6到10秒，如果仍然被限，可适当增加sleep时间
                if page - page1 == random_pages and page < page_count:
                    sleep(random.randint(6, 15))
                    page1 = page
                    random_pages = random.randint(3, 10)

                progress_bar.update(1)
                page = page + 1


        write_info = wb.get_write_info(0)

        print(u'微博爬取完成，共爬取%d条微博' % wb.got_count)

        weibo_update_list = []
        weibo_add_list = []

        for w in tqdm(write_info, desc='deal weibo progress'):
            weibo_id = w['id']

            count = 0
            """建立数据库连接"""
            conn = pymysql.connect(host=dbinfo_host, user=dbinfo_user, password=dbinfo_password, database=dbinfo_db)
            cursor = conn.cursor()
            n = cursor.execute("SELECT WEIBO_ID, PICS, VIDEO_URL, LIVE_PHOTO, PIC_INFO, PAGE_INFO, BID FROM weibo_info w "
                               "where w.WEIBO_ID = " + str(weibo_id) + " LIMIT 1")
            rows = cursor.fetchall()
            conn.close()
            if n:
                row = rows[0]
                # print("时间1:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                weibo_id = row[0]
                video_url_db = row[2]
                live_photo_db = row[3]
                pic_info_db = row[4]
                page_info_db = row[5]
                bid_db = row[6]
                # VIDEO_URL、LIVE_PHOTO、PIC_INFO、PAGE_INFO、BID都不为空的无需修复
                # if video_url_db and live_photo_db and pic_info_db and page_info_db and bid_db:
                #     continue
                print('爬取微博id:' + weibo_id)
                weibo = w
                if weibo:
                    weibo_update = {'weibo_id': weibo_id}
                    # 只补空缺：库里为空且新数据非空才更新
                    for field, db_value in (('video_url', video_url_db), ('live_photo', live_photo_db),
                                            ('pic_info', pic_info_db), ('page_info', page_info_db),
                                            ('bid', bid_db)):
                        new_value = weibo.get(field)
                        if not db_value and new_value:
                            weibo_update[field] = new_value
                    # 编辑记录：新数据不为空即覆盖更新，不管库里原字段有没有值
                    edit_rec = weibo.get('edit_rec')
                    if edit_rec:
                        weibo_update['edit_rec'] = edit_rec
                    if len(weibo_update) > 1:
                        print('可更新内容：' + str(weibo_update))
                        weibo_update_list.append(weibo_update)
            else:
                weibo_add_list.append(w)

        print("需要更新微博数：", len(weibo_update_list))
        conn = pymysql.connect(host=dbinfo_host, user=dbinfo_user, password=dbinfo_password, database=dbinfo_db)
        cursor = conn.cursor()
        field_to_column = {'video_url': 'VIDEO_URL', 'live_photo': 'LIVE_PHOTO',
                           'pic_info': 'PIC_INFO', 'page_info': 'PAGE_INFO', 'bid': 'BID',
                           'edit_rec': 'EDIT_REC'}
        for weibo_update in weibo_update_list:
            weibo_id = weibo_update['weibo_id']
            set_parts, params = [], []
            for field, value in weibo_update.items():
                if field == 'weibo_id':
                    continue
                set_parts.append("%s = %%s" % field_to_column[field])
                params.append(value)
            sql = "UPDATE weibo_info SET " + ", ".join(set_parts) + " WHERE WEIBO_ID = %s"
            cursor.execute(sql, params + [weibo_id])
        conn.commit()
        conn.close()

        print("需要新增微博数：", len(weibo_add_list))
        if len(weibo_add_list) > 0:
            wb.weibo = weibo_add_list
            wb.write_db(None, None, None, None, True)

    except Exception as e:
        print('Error: ', e)
        traceback.print_exc()
        # conn.rollback()
        # conn.close()


if __name__ == '__main__':
    # fix_images_over_nine()
    # test()
    # fix_weibo_time()
    user_id = 0
    # fix_weibo_info_pro(user_id)
    # fix_weibo_info(user_id)
