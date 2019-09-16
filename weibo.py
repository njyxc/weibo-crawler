#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import codecs
import csv
import json
import math
import os
import random
import sys
import traceback
from collections import OrderedDict
from datetime import datetime, timedelta
from time import sleep

import requests
from lxml import etree
from requests.adapters import HTTPAdapter
from tqdm import tqdm

import pymysql
import ConfigParser

# 读取配置文件
config_raw = ConfigParser.RawConfigParser()
config_raw.read('./weibo-crawler.conf')

# 读取数据库配置
dbinfo_host = config_raw.get('database', 'host')
dbinfo_user = config_raw.get('database', 'user')
dbinfo_password = config_raw.get('database', 'password')
dbinfo_db = config_raw.get('database', 'db')

# 读取请求配置
cookie_str = config_raw.get('request', 'cookie')
user_agent = config_raw.get('request', 'user_agent')

conn = pymysql.connect(host=dbinfo_host, user=dbinfo_user, passwd=dbinfo_password, db=dbinfo_db)
cursor = conn.cursor()


class Weibo(object):
    # 将your cookie替换成自己的cookie
    cookie = {'Cookie': cookie_str}

    def __init__(self,
                 filter=0,
                 since_date='1900-01-01',
                 mongodb_write=0,
                 mysql_write=0,
                 pic_download=0,
                 video_download=0):
        """Weibo类初始化"""
        if filter != 0 and filter != 1:
            sys.exit(u'filter值应为数字0或1,请重新输入')
        if not self.is_date(since_date):
            sys.exit(u'since_date值应为yyyy-mm-dd形式,请重新输入')
        if mongodb_write != 0 and mongodb_write != 1:
            sys.exit(u'mongodb_write值应为0或1,请重新输入')
        if mysql_write != 0 and mysql_write != 1:
            sys.exit(u'mysql_write值应为0或1,请重新输入')
        if pic_download != 0 and pic_download != 1:
            sys.exit(u'pic_download值应为数字0或1,请重新输入')
        if video_download != 0 and video_download != 1:
            sys.exit(u'video_download值应为0或1,请重新输入')
        self.user_id = ''  # 用户id,如昵称为"Dear-迪丽热巴"的id为'1669879400'
        self.filter = filter  # 取值范围为0、1,程序默认值为0,代表要爬取用户的全部微博,1代表只爬取用户的原创微博
        self.since_date = since_date  # 起始时间，即爬取发布日期从该值到现在的微博，形式为yyyy-mm-dd
        self.mongodb_write = mongodb_write  # 值为0代表不将结果写入MongoDB数据库,1代表写入
        self.mysql_write = mysql_write  # 值为0代表不将结果写入MySQL数据库,1代表写入
        self.pic_download = pic_download  # 取值范围为0、1,程序默认值为0,代表不下载微博原始图片,1代表下载
        self.video_download = video_download  # 取值范围为0、1,程序默认为0,代表不下载微博视频,1代表下载
        self.weibo = []  # 存储爬取到的所有微博信息
        self.user = {}  # 存储目标微博用户信息
        self.got_count = 0  # 爬取到的微博数
        self.mysql_config = {
        }  # MySQL数据库连接配置，可以不填，当使用者的mysql用户名、密码等与本程序默认值不同时，需要通过mysql_config来自定义

    def is_date(self, since_date):
        """判断日期格式是否正确"""
        try:
            datetime.strptime(since_date, "%Y-%m-%d")
            return True
        except ValueError:
            return False

    def get_json(self, params):
        """获取网页中json数据"""
        url = 'https://m.weibo.cn/api/container/getIndex?'
        r = requests.get(url, cookies=self.cookie, params=params, headers={'User-Agent': user_agent})
        return r.json()

    def get_weibo_json(self, since_weibo_id):
        """获取网页中微博json数据"""
        params = {'containerid': '107603' + str(self.user_id), 'since_id': since_weibo_id}
        js = self.get_json(params)
        return js

    def get_user_info(self):
        """获取用户信息"""
        params = {'containerid': '100505' + str(self.user_id)}
        js = self.get_json(params)
        if js['ok']:
            info = js['data']['userInfo']
            if info.get('toolbar_menus'):
                del info['toolbar_menus']
            user_info = self.standardize_info(info)
            self.user = user_info
            return user_info

    def get_long_weibo(self, id):
        """获取长微博"""
        url = 'https://m.weibo.cn/detail/%s' % id
        html = requests.get(url).text
        html = html[html.find('"status":'):]
        html = html[:html.rfind('"hotScheme"')]
        html = html[:html.rfind(',')]
        html = '{' + html + '}'
        js = json.loads(html, strict=False)
        weibo_info = js.get('status')
        if weibo_info:
            weibo = self.parse_weibo(weibo_info)
            return weibo

    def get_pics(self, weibo_info):
        """获取微博原始图片url"""
        if weibo_info.get('pics'):
            pic_info = weibo_info['pics']
            pic_list = [pic['large']['url'] for pic in pic_info]
            pics = ','.join(pic_list)
        else:
            pics = ''
        return pics

    def get_video_url(self, weibo_info):
        """获取微博视频url"""
        video_url = ''
        if weibo_info.get('page_info'):
            if weibo_info['page_info'].get('media_info'):
                media_info = weibo_info['page_info']['media_info']
                video_url = media_info.get('mp4_720p_mp4')
                if not video_url:
                    video_url = media_info.get('mp4_hd_url')
                    if not video_url:
                        video_url = media_info.get('mp4_sd_url')
                        if not video_url:
                            video_url = ''
        return video_url

    def download_one_file(self, url, file_path, type, weibo_id):
        """下载单个文件(图片/视频)"""
        try:
            if not os.path.isfile(file_path):
                s = requests.Session()
                s.mount(url, HTTPAdapter(max_retries=5))
                downloaded = s.get(url, timeout=(5, 10))
                with open(file_path, 'wb') as f:
                    f.write(downloaded.content)
        except Exception as e:
            error_file = self.get_filepath(
                type) + os.sep + 'not_downloaded.txt'
            with open(error_file, 'ab') as f:
                url = str(weibo_id) + ':' + url + '\n'
                f.write(url.encode(sys.stdout.encoding))
            print('Error: ', e)
            traceback.print_exc()

    def download_files(self, type):
        """下载文件(图片/视频)"""
        try:
            if type == 'img':
                describe = u'图片'
                key = 'pics'
            else:
                describe = u'视频'
                key = 'video_url'
            print(u'即将进行%s下载' % describe)
            file_dir = self.get_filepath(type)
            for w in tqdm(self.weibo, desc=u'%s下载进度' % describe):
                if w[key]:
                    file_prefix = w['created_at'][:11].replace(
                        '-', '') + '_' + str(w['id'])
                    if type == 'img' and ',' in w[key]:
                        w[key] = w[key].split(',')
                        for j, url in enumerate(w[key]):
                            file_suffix = url[url.rfind('.'):]
                            file_name = file_prefix + '_' + str(
                                j + 1) + file_suffix
                            file_path = file_dir + os.sep + file_name
                            self.download_one_file(url, file_path, type,
                                                   w['id'])
                    else:
                        if type == 'video':
                            file_suffix = '.mp4'
                        else:
                            file_suffix = w[key][w[key].rfind('.'):]
                        file_name = file_prefix + file_suffix
                        file_path = file_dir + os.sep + file_name
                        self.download_one_file(w[key], file_path, type,
                                               w['id'])
            print(u'%s下载完毕,保存路径:' % describe)
            print(file_dir)
        except Exception as e:
            print('Error: ', e)
            traceback.print_exc()

    def get_location(self, selector):
        """获取微博发布位置"""
        location_icon = 'timeline_card_small_location_default.png'
        span_list = selector.xpath('//span')
        location = ''
        for i, span in enumerate(span_list):
            if span.xpath('img/@src'):
                if location_icon in span.xpath('img/@src')[0]:
                    location = span_list[i + 1].xpath('string(.)')
                    break
        return location

    def get_topics(self, selector):
        """获取参与的微博话题"""
        span_list = selector.xpath("//span[@class='surl-text']")
        topics = ''
        topic_list = []
        for span in span_list:
            text = span.xpath('string(.)')
            if len(text) > 2 and text[0] == '#' and text[-1] == '#':
                topic_list.append(text[1:-1])
        if topic_list:
            topics = ','.join(topic_list)
        return topics

    def get_at_users(self, selector):
        """获取@用户"""
        a_list = selector.xpath('//a')
        at_users = ''
        at_list = []
        for a in a_list:
            if '@' + a.xpath('@href')[0][3:] == a.xpath('string(.)'):
                at_list.append(a.xpath('string(.)')[1:])
        if at_list:
            at_users = ','.join(at_list)
        return at_users

    def string_to_int(self, string):
        """字符串转换为整数"""
        if not string:
            return 0
        if isinstance(string, int):
            return string
        elif string.endswith(u'万+'):
            string = int(string[:-2] + '0000')
        elif string.endswith(u'万'):
            string = int(string[:-1] + '0000')
        return int(string)

    def standardize_date(self, created_at):
        """标准化微博发布时间"""
        if u"刚刚" in created_at:
            created_at = datetime.now().strftime("%Y-%m-%d")
        elif u"分钟" in created_at:
            minute = created_at[:created_at.find(u"分钟")]
            minute = timedelta(minutes=int(minute))
            created_at = (datetime.now() - minute).strftime("%Y-%m-%d")
        elif u"小时" in created_at:
            hour = created_at[:created_at.find(u"小时")]
            hour = timedelta(hours=int(hour))
            created_at = (datetime.now() - hour).strftime("%Y-%m-%d")
        elif u"昨天" in created_at:
            day = timedelta(days=1)
            created_at = (datetime.now() - day).strftime("%Y-%m-%d")
        elif created_at.count('-') == 1:
            year = datetime.now().strftime("%Y")
            created_at = year + "-" + created_at
        return created_at

    def standardize_info(self, weibo):
        """标准化信息，去除乱码"""
        for k, v in weibo.items():
            if 'int' not in str(type(v)) and 'long' not in str(
                    type(v)) and 'bool' not in str(type(v)):
                weibo[k] = v.replace(u"\u200b", "").encode(
                    sys.stdout.encoding, "ignore").decode(sys.stdout.encoding)
        return weibo

    def parse_weibo(self, weibo_info):
        weibo = OrderedDict()
        if weibo_info['user']:
            weibo['user_id'] = weibo_info['user']['id']
            weibo['screen_name'] = weibo_info['user']['screen_name']
        else:
            weibo['user_id'] = ''
            weibo['screen_name'] = ''
        weibo['id'] = int(weibo_info['id'])
        text_body = weibo_info['text']
        selector = etree.HTML(text_body)
        weibo['text'] = etree.HTML(text_body).xpath('string(.)')
        weibo['pics'] = self.get_pics(weibo_info)
        weibo['video_url'] = self.get_video_url(weibo_info)
        weibo['location'] = self.get_location(selector)
        weibo['created_at'] = weibo_info['created_at']
        weibo['source'] = weibo_info['source']
        weibo['attitudes_count'] = self.string_to_int(
            weibo_info.get('attitudes_count'))
        weibo['comments_count'] = self.string_to_int(
            weibo_info.get('comments_count'))
        weibo['reposts_count'] = self.string_to_int(
            weibo_info.get('reposts_count'))
        weibo['topics'] = self.get_topics(selector)
        weibo['at_users'] = self.get_at_users(selector)
        return self.standardize_info(weibo)

    def print_user_info(self):
        """打印用户信息"""
        print('+' * 100)
        print(u'用户信息')
        print(u'用户id：%d' % self.user['id'])
        print(u'用户昵称：%s' % self.user['screen_name'])
        gender = u'女' if self.user['gender'] == 'f' else u'男'
        print(u'性别：%s' % gender)
        print(u'微博数：%d' % self.user['statuses_count'])
        print(u'粉丝数：%d' % self.user['followers_count'])
        print(u'关注数：%d' % self.user['follow_count'])
        if self.user.get('verified_reason'):
            print(self.user['verified_reason'])
        print(self.user['description'])
        print('+' * 100)

    def print_one_weibo(self, weibo):
        """打印一条微博"""
        print(u'微博id：%d' % weibo['id'])
        print(u'微博正文：%s' % weibo['text'])
        print(u'原始图片url：%s' % weibo['pics'])
        print(u'微博位置：%s' % weibo['location'])
        print(u'发布时间：%s' % weibo['created_at'])
        print(u'发布工具：%s' % weibo['source'])
        print(u'点赞数：%d' % weibo['attitudes_count'])
        print(u'评论数：%d' % weibo['comments_count'])
        print(u'转发数：%d' % weibo['reposts_count'])
        print(u'话题：%s' % weibo['topics'])
        print(u'@用户：%s' % weibo['at_users'])

    def print_weibo(self, weibo):
        """打印微博，若为转发微博，会同时打印原创和转发部分"""
        if weibo.get('retweet'):
            print('*' * 100)
            print(u'转发部分：')
            self.print_one_weibo(weibo['retweet'])
            print('*' * 100)
            print(u'原创部分：')
        self.print_one_weibo(weibo)
        print('-' * 120)

    def get_one_weibo(self, info):
        """获取一条微博的全部信息"""
        try:
            weibo_info = info['mblog']
            weibo_id = weibo_info['id']
            retweeted_status = weibo_info.get('retweeted_status')
            is_long = weibo_info['isLongText']
            if retweeted_status:  # 转发
                retweet_id = retweeted_status['id']
                is_long_retweet = retweeted_status.get('isLongText')
                if is_long_retweet is None:
                    is_long_retweet = False
                if is_long:
                    weibo = self.get_long_weibo(weibo_id)
                    if not weibo:
                        weibo = self.parse_weibo(weibo_info)
                else:
                    weibo = self.parse_weibo(weibo_info)
                if is_long_retweet:
                    retweet = self.get_long_weibo(retweet_id)
                    if not retweet:
                        retweet = self.parse_weibo(retweeted_status)
                else:
                    retweet = self.parse_weibo(retweeted_status)
                retweet['created_at'] = self.standardize_date(
                    retweeted_status['created_at'])
                weibo['retweet'] = retweet
            else:  # 原创
                if is_long:
                    weibo = self.get_long_weibo(weibo_id)
                    if not weibo:
                        weibo = self.parse_weibo(weibo_info)
                else:
                    weibo = self.parse_weibo(weibo_info)
            weibo['created_at'] = self.standardize_date(
                weibo_info['created_at'])
            return weibo
        except Exception as e:
            print("Error: ", e)
            traceback.print_exc()

    def is_pin(self, info):
        """判断微博是否为置顶微博"""
        weibo_info = info['mblog']
        title = weibo_info.get('title')
        if title and title.get('text') == u'置顶':
            return True
        else:
            return False

    def get_one_page(self, since_weibo_id, latest_weibo_time, update_time, recovery):
        """获取一页的全部微博"""
        result = {}
        try:
            js = self.get_weibo_json(since_weibo_id)
            if js['ok'] == 1:
                weibos = js['data']['cards']
                for w in weibos:
                    if w['card_type'] == 9:
                        wb = self.get_one_weibo(w)
                        if wb:
                            if not recovery:
                                if update_time is not None:
                                    update_time_zero = update_time.replace(hour=0, minute=0, second=0)
                                    weibo_created_at = datetime.strptime(wb['created_at'], "%Y-%m-%d")
                                    if update_time_zero > weibo_created_at:
                                        if self.is_pin(w):
                                            continue
                                        else:
                                            time_delta = update_time_zero - weibo_created_at
                                            if time_delta.days != 1 or update_time.hour >= 1:
                                                result['code'] = 0
                                                return result
                                if latest_weibo_time is not None:
                                    if latest_weibo_time > datetime.strptime(wb['created_at'], "%Y-%m-%d"):
                                        if self.is_pin(w):
                                            continue
                                        else:
                                            result['code'] = 0
                                            return result
                            created_at = datetime.strptime(
                                wb['created_at'], "%Y-%m-%d")
                            since_date = datetime.strptime(
                                self.since_date, "%Y-%m-%d")
                            if created_at < since_date:
                                if self.is_pin(w):
                                    continue
                                else:
                                    result['code'] = 0
                                    return result
                            if (not self.filter) or (
                                    'retweet' not in wb.keys()):
                                self.weibo.append(wb)
                                self.got_count = self.got_count + 1
                                self.print_weibo(wb)
                since_id = js['data']['cardlistInfo'].get('since_id')
                if since_id:
                    result['code'] = 1
                    result['data'] = since_id
                    return result
                else:
                    result['code'] = 0
                    return result
            else:
                result['code'] = 2
                return result
        except Exception as e:
            print("Error: ", e)
            traceback.print_exc()
            result['code'] = 2
            return result

    def get_page_count(self):
        """获取微博页数"""
        weibo_count = self.user['statuses_count']
        page_count = int(math.ceil(weibo_count / 10.0))
        return page_count

    def get_write_info(self, wrote_count):
        """获取要写入的微博信息"""
        write_info = []
        for w in self.weibo[wrote_count:]:
            wb = OrderedDict()
            for k, v in w.items():
                if k not in ['user_id', 'screen_name', 'retweet']:
                    if 'unicode' in str(type(v)):
                        v = v.encode('utf-8')
                    wb[k] = v
            if not self.filter:
                if w.get('retweet'):
                    wb['is_original'] = False
                    for k2, v2 in w['retweet'].items():
                        if 'unicode' in str(type(v2)):
                            v2 = v2.encode('utf-8')
                        wb['retweet_' + k2] = v2
                else:
                    wb['is_original'] = True
            else:
                wb['is_original'] = True
            write_info.append(wb)
        return write_info

    def get_filepath(self, type):
        """获取结果文件路径"""
        try:
            file_dir = os.path.split(
                os.path.realpath(__file__)
            )[0] + os.sep + 'weibo' + os.sep + self.user['screen_name']
            if type == 'img' or type == 'video':
                file_dir = file_dir + os.sep + type
            if not os.path.isdir(file_dir):
                os.makedirs(file_dir)
            if type == 'img' or type == 'video':
                return file_dir
            file_path = file_dir + os.sep + self.user_id + '.' + type
            return file_path
        except Exception as e:
            print('Error: ', e)
            traceback.print_exc()

    def get_result_headers(self):
        """获取要写入结果文件的表头"""
        result_headers = [
            'id', '正文', '原始图片url', '视频url', '位置', '日期', '工具', '点赞数', '评论数',
            '转发数', '话题', '@用户'
        ]
        if not self.filter:
            result_headers2 = ['是否原创', '源用户id', '源用户昵称']
            result_headers3 = ['源微博' + r for r in result_headers]
            result_headers = result_headers + result_headers2 + result_headers3
        return result_headers

    def write_csv(self, wrote_count):
        """将爬到的信息写入csv文件"""
        write_info = self.get_write_info(wrote_count)
        result_headers = self.get_result_headers()
        result_data = [w.values() for w in write_info]
        if sys.version < '3':  # python2.x
            with open(self.get_filepath('csv'), 'ab') as f:
                f.write(codecs.BOM_UTF8)
                writer = csv.writer(f)
                if wrote_count == 0:
                    writer.writerows([result_headers])
                writer.writerows(result_data)
        else:  # python3.x
            with open(self.get_filepath('csv'),
                      'a',
                      encoding='utf-8-sig',
                      newline='') as f:
                writer = csv.writer(f)
                if wrote_count == 0:
                    writer.writerows([result_headers])
                writer.writerows(result_data)
        print(u'%d条微博写入csv文件完毕,保存路径:' % self.got_count)
        print(self.get_filepath('csv'))

    def write_mongodb(self, wrote_count):
        """将爬取的信息写入MongoDB数据库"""
        from pymongo import MongoClient

        client = MongoClient()
        db = client['weibo']
        collection = db['weibo']
        for w in self.weibo[wrote_count:]:
            if not collection.find_one({'id': w['id']}):
                collection.insert_one(w)
            else:
                collection.update_one({'id': w['id']}, {'$set': w})
        print(u'%d条微博写入MongoDB数据库完毕' % self.got_count)

    def change_mysql_config(self, mysql_config):
        """修改MySQL数据库连接配置"""
        self.mysql_config = mysql_config

    def mysql_create(self, connection, sql):
        """创建MySQL数据库或表"""
        try:
            with connection.cursor() as cursor:
                cursor.execute(sql)
        finally:
            connection.close()

    def mysql_create_database(self, mysql_config, sql):
        """创建MySQL数据库"""
        import pymysql

        if self.mysql_config:
            mysql_config = self.mysql_config
        connection = pymysql.connect(**mysql_config)
        self.mysql_create(connection, sql)

    def mysql_create_table(self, mysql_config, sql):
        """创建MySQL表"""
        import pymysql

        if self.mysql_config:
            mysql_config = self.mysql_config
        mysql_config['db'] = 'weibo'
        connection = pymysql.connect(**mysql_config)
        self.mysql_create(connection, sql)

    def mysql_insert(self, mysql_config, table, data_list):
        """向MySQL表插入或更新数据"""
        import pymysql

        if len(data_list) > 0:
            keys = ', '.join(data_list[0].keys())
            values = ', '.join(['%s'] * len(data_list[0]))
            if self.mysql_config:
                mysql_config = self.mysql_config
            mysql_config['db'] = 'weibo'
            connection = pymysql.connect(**mysql_config)
            cursor = connection.cursor()
            sql = """INSERT INTO {table}({keys}) VALUES ({values}) ON
                     DUPLICATE KEY UPDATE""".format(table=table,
                                                    keys=keys,
                                                    values=values)
            update = ','.join([
                " {key} = values({key})".format(key=key)
                for key in data_list[0]
            ])
            sql += update
            try:
                cursor.executemany(
                    sql, [tuple(data.values()) for data in data_list])
                connection.commit()
            except Exception as e:
                connection.rollback()
                print('Error: ', e)
                traceback.print_exc()
            finally:
                connection.close()

    def write_mysql(self, wrote_count):
        """将爬取的信息写入MySQL数据库"""
        mysql_config = {
            'host': 'localhost',
            'port': 3306,
            'user': 'root',
            'password': '123456',
            'charset': 'utf8mb4'
        }
        # 创建'weibo'数据库
        create_database = """CREATE DATABASE IF NOT EXISTS weibo DEFAULT
                         CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"""
        self.mysql_create_database(mysql_config, create_database)
        # 创建'weibo'表
        create_table = """
                CREATE TABLE IF NOT EXISTS weibo (
                id varchar(20) NOT NULL,
                user_id varchar(20),
                screen_name varchar(20),
                text varchar(2000),
                topics varchar(200),
                at_users varchar(200),
                pics varchar(1000),
                video_url varchar(300),
                location varchar(100),
                created_at DATETIME,
                source varchar(30),
                attitudes_count INT,
                comments_count INT,
                reposts_count INT,
                retweet_id varchar(20),
                PRIMARY KEY (id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"""
        self.mysql_create_table(mysql_config, create_table)
        weibo_list = []
        retweet_list = []
        for w in self.weibo[wrote_count:]:
            if 'retweet' in w:
                w['retweet']['retweet_id'] = ''
                retweet_list.append(w['retweet'])
                w['retweet_id'] = w['retweet']['id']
                del w['retweet']
            else:
                w['retweet_id'] = ''
            weibo_list.append(w)
        # 在'weibo'表中插入或更新微博数据
        self.mysql_insert(mysql_config, 'weibo', retweet_list)
        self.mysql_insert(mysql_config, 'weibo', weibo_list)
        print(u'%d条微博写入MySQL数据库完毕' % self.got_count)

    def write_data(self, wrote_count):
        """将爬到的信息写入文件或数据库"""
        if self.got_count > wrote_count:
            self.write_db(wrote_count)
            if self.mysql_write:
                self.write_mysql(wrote_count)
            if self.mongodb_write:
                self.write_mongodb(wrote_count)

    def write_db(self, wrote_count):
        """将爬到的信息写入csv文件"""
        write_info = self.get_write_info(wrote_count)
        for w in write_info:
            weibo_id = w['id']
            text = w['text']
            pics = w['pics']
            video_url = w['video_url']
            created_at = w['created_at']
            retweet_weibo_id = ''
            is_original = w['is_original']
            db_is_original = '0'
            if is_original:
                db_is_original = '1'
            if not is_original:
                retweet_weibo_id = w['retweet_id']
            n = cursor.execute("SELECT DATA_ID from weibo_info where WEIBO_ID = '%s'" % (weibo_id))
            if n == 0:
                cursor.execute("INSERT INTO weibo_info (USER_ID, WEIBO_ID, IS_ORIGINAL, TEXT, PICS, VIDEO_URL, CREATE_TIME, RETWEET_WEIBO_ID) " \
                     "VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')" \
                     % (self.user['id'], weibo_id, db_is_original, text, pics, video_url, created_at, retweet_weibo_id))

            if not is_original:
                retweet_weibo_id = w['retweet_id']
                retweet_text = w['retweet_text']
                retweet_pics = w['retweet_pics']
                retweet_video_url = w['retweet_video_url']
                retweet_created_at = w['retweet_created_at']
                n = cursor.execute("SELECT DATA_ID from weibo_info where WEIBO_ID = '%s'" % (retweet_weibo_id))
                if n == 0:
                    sql = "INSERT INTO weibo_info (USER_ID, WEIBO_ID, IS_ORIGINAL, TEXT, PICS, VIDEO_URL, CREATE_TIME, RETWEET_WEIBO_ID) " \
                        "VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')" \
                        % (w['retweet_user_id'], retweet_weibo_id, '1', retweet_text, retweet_pics, retweet_video_url, retweet_created_at, '')
                    print sql
                    cursor.execute(sql)

    def get_pages(self, db_user_info):
        """获取全部微博"""
        self.get_user_info()
        page_count = self.get_page_count()
        wrote_count = 0
        self.print_user_info()
        page1 = 0
        random_pages = random.randint(1, 5)

        page = 1
        since_weibo_id = ""

        latest_weibo_time = db_user_info['latest_weibo_time']
        update_time = db_user_info['update_time']

        start_time = datetime.now()

        error_since_weibo_id = 'null'

        db_error_since_weibo_id = db_user_info['error_since_weibo_id']

        recovery = False

        if db_error_since_weibo_id:
            since_weibo_id = db_error_since_weibo_id
            recovery = True

        with tqdm(total=page_count) as progress_bar:
            while True:
                result = self.get_one_page(since_weibo_id, latest_weibo_time, update_time, recovery)
                result_code = result['code']
                if result_code == 1:
                    next_since_weibo_id = result['data']
                    if (next_since_weibo_id is not None) and (next_since_weibo_id != 0):
                        since_weibo_id = next_since_weibo_id
                    else:
                        break
                elif result_code == 0:
                    break
                elif result_code == 2:
                    error_since_weibo_id = since_weibo_id
                    break

                if page % 20 == 0:  # 每爬20页写入一次文件
                    self.write_data(wrote_count)
                    wrote_count = self.got_count

                # 通过加入随机等待避免被限制。爬虫速度过快容易被系统限制(一段时间后限
                # 制会自动解除)，加入随机等待模拟人的操作，可降低被系统限制的风险。默
                # 认是每爬取1到5页随机等待6到10秒，如果仍然被限，可适当增加sleep时间
                if page - page1 == random_pages and page < page_count:
                    sleep(random.randint(8, 15))
                    page1 = page
                    random_pages = random.randint(1, 5)

                progress_bar.update(page)
                page = page + 1

        if len(self.weibo) > 0:
            # 更新用户信息
            cursor.execute("UPDATE weibo_user_info SET NICK_NAME = '%s', AVATAR_URL = '%s', LATEST_WEIBO_ID = '%s', LATEST_WEIBO_TIME = '%s', ERROR_SINCE_WEIBO_ID = %s, UPDATE_TIME = '%s' WHERE USER_ID = '%s'"
                           % (self.user['screen_name'], self.user['avatar_hd'], self.weibo[0]['id'], self.weibo[0]['created_at'], error_since_weibo_id, start_time, self.user['id']))
        self.write_data(wrote_count)  # 将剩余不足20页的微博写入文件
        print(u'微博爬取完成，共爬取%d条微博' % self.got_count)

    def get_user_list(self, file_name):
        """获取文件中的微博id信息"""
        with open(file_name, 'r') as f:
            user_id_list = f.read().splitlines()
        return user_id_list

    def initialize_info(self, user_id):
        """初始化爬虫信息"""
        self.weibo = []
        self.user = {}
        self.got_count = 0
        self.user_id = user_id

    def is_login(self):
        """获取网页中json数据"""
        url = 'https://m.weibo.cn/api/config'
        r = requests.get(url, cookies=self.cookie)
        json = r.json()
        login = json['data']['login']
        if isinstance(login, bool):
            return login
        else:
            return False

    def start(self, user_id_list):
        """运行爬虫"""
        # try:
        is_login = self.is_login()
        if is_login:
            print(u'已登录')
            self.printHrLine()
        else:
            sys.exit(u'cookie失效！')

        for db_user_info in user_id_list:
            user_id = db_user_info['user_id']
            self.initialize_info(user_id)
            self.get_pages(db_user_info)
            print(u'信息抓取完毕')
            print('*' * 100)
            if self.pic_download == 1:
                self.download_files('img')
            if self.video_download == 1:
                self.download_files('video')
        # except Exception as e:
        #     print('Error: ', e)
        #     traceback.print_exc()

    def printHrLine(self):
        print('----------------------------')


def main():
    try:
        # 以下是程序配置信息，可以根据自己需求修改
        filter = 1  # 值为0表示爬取全部微博（原创微博+转发微博），值为1表示只爬取原创微博
        since_date = '2010-01-01'  # 起始时间，即爬取发布日期从该值到现在的微博，形式为yyyy-mm-dd
        """mongodb_write值为0代表不将结果写入MongoDB数据库,1代表写入；若要写入MongoDB数据库，
        请先安装MongoDB数据库和pymongo，pymongo安装方法为命令行运行:pip install pymongo"""
        mongodb_write = 0
        """mysql_write值为0代表不将结果写入MySQL数据库,1代表写入;若要写入MySQL数据库，
        请先安装MySQL数据库和pymysql，pymysql安装方法为命令行运行:pip install pymysql"""
        mysql_write = 0
        pic_download = 0  # 值为0代表不下载微博原始图片,1代表下载微博原始图片
        video_download = 0  # 值为0代表不下载微博视频,1代表下载微博视频

        wb = Weibo(filter, since_date, mongodb_write, mysql_write,
                   pic_download, video_download)

        # 下面是自定义MySQL数据库连接配置(可选)
        """因为操作MySQL数据库需要用户名、密码等参数，本程序默认为:
        mysql_config = {
            'host': 'localhost',
            'port': 3306,
            'user': 'root',
            'password': '123456',
            'charset': 'utf8mb4'
        }
        大家的参数配置如果和默认值不同，可以将上面的参数值替换成自己的，
        然后添加如下代码，使修改生效，如果你的参数和默认值相同则不需要下面的代码:
        wb.change_mysql_config(mysql_config)"""

        # 下面是配置user_id_list
        """user_id_list包含了要爬的目标微博id，可以是一个，也可以是多个，也可以从文件中读取
        爬单个微博，user_id_list如下所示，可以改成任意合法的用户id
        user_id_list = ['1669879400']
        爬多个微博，user_id_list如下所示，可以改成任意合法的用户id
        user_id_list = ['1669879400', '1729370543']
        也可以在文件中读取user_id_list，文件中可以包含很多user_id，
        每个user_id占一行，文件名任意，类型为txt，位置位于本程序的同目录下，
        比如文件可以叫user_id_list.txt，读取文件中的user_id_list如下所示:
        user_id_list = wb.get_user_list('user_id_list.txt')"""
        user_id_list = []

        n = cursor.execute("SELECT USER_ID, NICK_NAME, LATEST_WEIBO_ID, LATEST_WEIBO_TIME, ERROR_SINCE_WEIBO_ID, UPDATE_TIME from weibo_user_info WHERE FLAG = '1' ")
        print u'共需要爬' + str(n) + u'个微博'
        for row in cursor.fetchall():
            db_user_info = {'user_id': row[0], 'latest_weibo_id': row[2], 'latest_weibo_time': row[3], 'error_since_weibo_id': row[4], 'update_time': row[5]}
            print row
            user_id_list.append(db_user_info)
        wb.printHrLine()

        wb.start(user_id_list)
        conn.commit()
    except Exception as e:
        print('Error: ', e)
        traceback.print_exc()
        conn.rollback()


if __name__ == '__main__':
    main()
