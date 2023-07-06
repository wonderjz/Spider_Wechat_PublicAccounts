# -*- coding: utf-8 -*-
"""
Created on Fri Apr  2 09:47:29 2021

@author: RG23436
"""

import logging
from logging.handlers import TimedRotatingFileHandler
import os
import pymysql as pysql
import traceback
import time
import re
import pandas as pd
import numpy as np
import psycopg2 as pg
import random
import string

from logging import FileHandler
from email import encoders
from email.header import Header
from email.mime.text import MIMEText
from email.utils import formataddr
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from urllib.parse import unquote
from fake_useragent import UserAgent
from selenium.webdriver import Chrome, DesiredCapabilities
from selenium.webdriver.chrome.options import Options
import smtplib
import traceback
import platform


# 在PostgreSQL中可以直接对时间进行加减运算：、
# SELECT now()::timestamp + '1 year';  --当前时间加1年
# SELECT now()::timestamp + '1 month';  --当前时间加一个月
# SELECT now()::timestamp + '1 day';  --当前时间加一天
# SELECT now()::timestamp + '1 hour';  --当前时间加一个小时
# SELECT now()::timestamp + '1 min';  --当前时间加一分钟
# SELECT now()::timestamp + '1 sec';  --加一秒钟
# select now()::timestamp + '1 year 1 month 1 day 1 hour 1 min 1 sec';  --加1年1月1天1时1分1秒
# SELECT now()::timestamp + (col || ' day')::interval FROM table --把col字段转换成天 然后相加

# mysql数据库取数
def selectData(connection, sql):
    cursor = connection.cursor()   # 通过cursor创建游标
    cursor.execute(sql)
    connection.commit()
    result = cursor.fetchall()  # 获取查询结果的全部数据
    connection.close()  # 关闭数据库连接
    if len(result) == 0:
        result = None
    return result


def selectPg(connection, sql, after_close=True):
    cursor = connection.cursor()
    cursor.execute(sql)
    connection.commit()
    columns = cursor.description
    df_res = pd.DataFrame(cursor.fetchall())  # 获取查询结果的全部数据
    if np.size(df_res) == 0:
        return None
    dic = {}
    for i in range(len(columns)):
        dic[columns[i].name] = df_res[i].tolist()
    if after_close:
        connection.close()  # 关闭数据库连接
    return dic


def send_email(from_addr, to_addr, mailhost, password, msg_subject, msg_text,
               cc_addr=None, text_type='plain', file_path=None):
    """
    from_addr: dict, 发件人的邮箱昵称和地址， 例如 {'吴刚':'wugang@example.com'}
    to_addr: dict, 收件人的邮箱昵称和地址， 例如 {'a':'abc@qq.com', 'b':'efg@163.com'}
    mailhost: tuple or list, 邮箱服务器地址和端口, 例如 ('smtp.exmail.qq.com', 465)
    password: str, 密码或授权码
    msg_subject: str, 邮件的标题
    msg_text: str, 邮件的正文
    cc_addr: dict, 抄送人的邮箱昵称和地址, 例如 {'小明': 'xiaoming@example.com'}
    text_type: str, 发送邮件的文本格式， default 'plain' ，普通文本用此即可，发送html表格
                    则使用 'html'
    file_path: str, 附件文件路径
    """
    msg = MIMEMultipart()   # 发送附件需要建立容器 MIMEMultipart,
                            # 如不需要发送附件，可直接建立容器 MIMEText
    msg['From'] = _format_addr(from_addr)   # 发送人
    msg['To'] = _format_addr(to_addr)   # 收件人
    if cc_addr is not None:
        msg['Cc'] = _format_addr(cc_addr)   # 抄送人
    msg['Subject'] = Header(msg_subject, 'utf-8').encode()  # 邮件主题
    msg.attach(MIMEText(msg_text, text_type, 'utf-8'))  # 指定文本格式为简洁，编码 utf-8
    if file_path is not None:
        msg = files(msg, file_path) # 读取附件

    smtp_server, port = mailhost  # 邮箱服务器以及端口号
    # 使用 smtp 协议以及 ssl 加密方式
    server = smtplib.SMTP_SSL(smtp_server, int(port), timeout=10.0)
    server.set_debuglevel(0)  # 设置 debug 级别 1：打印 0: 不打印
    server.connect(smtp_server)  # 连接服务器
    server.login(get_val(from_addr)[0], password)  # 发件人账号登录

    # 发送邮件
    if cc_addr is not None:
        server.sendmail(get_val(from_addr)[0],
                        get_val(to_addr) + get_val(cc_addr),
                        msg.as_string())
    else:
        server.sendmail(get_val(from_addr)[0], get_val(to_addr),
                        msg.as_string())
    server.quit()  # 退出


def get_val(dic):
    """字典取值，返回列表"""
    val = list(dic.values())
    return val


def _format_addr(dic):
    """规范地址格式处理, 支持多个收件人"""
    addr_list = []
    for name, addr in dic.items():
        addr_list.append(formataddr((Header(name, 'utf-8').encode(), addr)))
    return ','.join(addr_list)


def files(msg, file_path):
    """
    msg : object
        邮件主体容器
    file_path : str or list
        附件的文件路径
    """
    if isinstance(file_path, str):
        file_path = [file_path]

    for path in file_path:
        # 添加附件就是加上一个MIMEBase，从本地读取一个文件:
        with open(path, 'rb') as f:
            # 创建收纳附件的容器 mime
            mime = MIMEBase('application', 'octet-stream')
            # 构造附件
            basename = os.path.basename(path) # 注意：此时 basename 为 utf-8 编码

            # 加上必要的头信息
            # basename 转码 gbk， 否则附件带有中文名会有乱码
            mime.add_header('Content-Disposition', 'attachment',
                            filename=('gbk', '', basename))
            mime.add_header('Content-ID', '<0>')
            mime.add_header('X-Attachment-Id', '0')
            mime.set_payload(f.read())  # 把附件的内容读进 mime
            encoders.encode_base64(mime)  # 用Base64编码
            msg.attach(mime)  # 添加到 MIMEMultipart 中
    return msg


def get_exist_id(tid, table, tid_list=None):
    """
    提取pg库表中已存在的唯一性字段。
    当 tid_list 为 None 时，提取表中存在的该唯一性字段的所有数据id
    当 tid_list 不为 None 时，则提取指定数据中心存在的所有数据id

    Args:
        tid (str): 唯一性字段.
        table (str): 表名.
        tid_list (list, optional): 指定数据. Defaults to None.

    Returns:
        result (dict): 返回结果.

    """
    if tid_list == []:
        result = {tid: []}
        return result
    elif tid_list is None:
        sql = '''SELECT DISTINCT {0} FROM {1}'''.format(tid, table)
    else:
        sql = '''
        SELECT DISTINCT {0} FROM {1} WHERE {0} in ({2})
        '''.format(tid, table, str(tid_list).replace('[', '').replace(']', ''))

    connection = pg.connect(database="pg_fk_data",
                            user="postgres", password=,
                            host=, port=)
    result = selectPg(connection, sql)
    if result is None:
        result = {tid: []}
    return result


class SafeFileHandler(FileHandler):
    def __init__(self, filename, mode="a", encoding='utf-8', delay=0, when='H', backupCount=0, end_log='.log'):
        self.when = when.upper()
        self.end_log = end_log
        if self.when == 'S':
            # self.interval = 1 # one second
            self.suffix = "%Y-%m-%d_%H-%M-%S" + self.end_log
            self.extMatch = r"^\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}(\.\w+)?$"
        elif self.when == 'M':
            # self.interval = 60 # one minute
            self.suffix = "%Y-%m-%d_%H-%M" + self.end_log
            self.extMatch = r"^\d{4}-\d{2}-\d{2}_\d{2}-\d{2}(\.\w+)?$"
        elif self.when == 'H':
            # self.interval = 60 * 60 # one hour
            self.suffix = "%Y-%m-%d_%H" + self.end_log
            self.extMatch = r"^\d{4}-\d{2}-\d{2}_\d{2}(\.\w+)?$"
        elif self.when == 'D':
            # self.interval = 60 * 60 * 24 # one day
            self.suffix = "%Y-%m-%d" + self.end_log
            self.extMatch = r"^\d{4}-\d{2}-\d{2}(\.\w+)?$"
        # elif self.when.startswith('W'):
        #     self.interval = 60 * 60 * 24 * 7 # one week
        #     if len(self.when) != 2:
        #         raise ValueError("You must specify a day for weekly rollover from 0 to 6 (0 is Monday): %s" % self.when)
        #     if self.when[1] < '0' or self.when[1] > '6':
        #         raise ValueError("Invalid day specified for weekly rollover: %s" % self.when)
        #     self.dayOfWeek = int(self.when[1])
        #     self.suffix = "%Y-%m-%d" + self.end_log
        #     self.extMatch = r"^\d{4}-\d{2}-\d{2}(\.\w+)?$"
        else:
            raise ValueError(
                "Invalid rollover interval specified: %s" % self.when)
        current_time = time.strftime(self.suffix, time.localtime())

        FileHandler.__init__(self, filename + "." +
                             current_time, mode, encoding, delay)

        self.filename = filename
        self.mode = mode
        self.encoding = encoding
        self.suffix_time = current_time
        self.backupCount = backupCount
        self.extMatch = re.compile(self.extMatch, re.ASCII)

    def emit(self, record):
        try:
            if self.check_base_filename():
                self.build_base_filename()

            logging.FileHandler.emit(self, record)
            if self.backupCount > 0:
                del_result = self.getFilesToDelete()
                if del_result:
                    for res in del_result:
                        os.remove(res)
        except(KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)

    def check_base_filename(self):
        time_tuple = time.localtime()

        if self.suffix_time != time.strftime(self.suffix, time_tuple) or not os.path.exists(
                os.path.abspath(self.filename) + '.' + self.suffix_time):
            return 1
        else:
            return 0

    def getFilesToDelete(self):
        """
        Determine the files to delete when rolling over.

        More specific than the earlier method, which just used glob.glob().
        """
        # extMatch = re.compile(r"^\d{4}-\d{2}-\d{2}_\d{2}-\d{2}.log$", re.ASCII)
        dirName, baseName = os.path.split(self.filename)
        fileNames = os.listdir(dirName)
        result = []
        prefix = baseName + "."
        plen = len(prefix)
        for fileName in fileNames:
            if fileName[:plen] == prefix:
                suffix = fileName[plen:]
                if self.extMatch.match(suffix):
                    result.append(os.path.join(dirName, fileName))
        if len(result) < self.backupCount:
            result = []
        else:
            result.sort()
            result = result[:len(result) - self.backupCount]
        return result

    def build_base_filename(self):
        if self.stream:
            self.stream.close()
            self.stream = None

        # if self.suffix_time != "":
        #     index = self.baseFilename.find("." + self.suffix_time)
        #     if index == -1:
        #         index = self.baseFilename.rfind(".")
        #     self.baseFilename = self.baseFilename[:index]

        current_time_tuple = time.localtime()
        self.suffix_time = time.strftime(self.suffix, current_time_tuple)
        self.baseFilename = os.path.abspath(
            self.filename) + "." + self.suffix_time

        if not self.delay:
            self.stream = open(self.baseFilename, self.mode,
                               encoding=self.encoding)

ser_num = {'ser_num': '流水号: {}'.format("".join(random.sample(string.ascii_letters + string.digits, 18)))}
def create_logger(LOG_PATH, filename='runlogger', level=logging.INFO, ser_num=None):
    """针对create_logger函数在多进程/多线程写入日志中存在的日志错乱的情况，
    新写入SafeFileHandler类，该类继承logging中的FileHandler类
    SafeFileHandler参数:
    filename: 日志文件名
    when 以什么时间间隔分割文件：默认 D
    参数选择：S:以秒分割，
             M:以分钟分割，
             H：以小时分割，
             D：以天数分割
    backupCount: int,保留最新的几个文件，默认None，即不删除
    end_log：str,日志文件名的结尾，例：'.log',默认'.log'
    """
    LOG_PATH = LOG_PATH.strip()
    isExists = os.path.exists(LOG_PATH)
    if not isExists:
        os.makedirs(LOG_PATH)

    record_format = "%(asctime)s\t%(levelname)s\t%(module)s.%(funcName)s\t%(threadName)s\t%(lineno)s\t%(message)s"
    if ser_num:
        record_format = record_format + "\t%(ser_num)s"

    filename_abspath = os.path.join(LOG_PATH, filename)

    logger = logging.getLogger(filename_abspath)
    logger.setLevel(level)
    logger.handlers.clear()
    formatter = logging.Formatter(record_format)
    tfrHandler = SafeFileHandler(filename=filename_abspath,
                                 when='D', backupCount=5, end_log='.log')
    tfrHandler.setFormatter(formatter)
    logger.addHandler(tfrHandler)
    return logger


def open_browser(proxy=None, if_desired_capabilities=False):
    u_system = platform.system()  # 系统平台
    username = os.environ.get("USERNAME")  # 用户名
    if u_system == 'Windows':
        chrome_options = Options()
        # chrome_options.add_argument("--headless")  # 无界面模式
        path_user_data = r"C:/Users/{}/AppData/Local/Google/Chrome/User Data".format(username)
        chrome_options.add_argument("--user-data-dir="+path_user_data)  # 加载用户数据伪装
        chrome_options.add_argument('user-agent={}'.format(UserAgent().chrome))
        prefs = {
            "profile.managed_default_content_settings.images": 2,  # 加载图片,1允许，2禁止
            'permissions.default.stylesheet': 2  # 加载CSS，1允许，2禁止
            }
        chrome_options.add_experimental_option('prefs', prefs)
        chrome_options.add_experimental_option('excludeSwitches', ['enable-automation'])
        chrome_options.add_argument('--ignore-certificate-errors') #
        chrome_options.add_argument('--incognito') #
        chrome_options.add_argument('--headless') #
        chrome_options.add_argument("--no-sandbox") #
        chrome_options.add_argument("--disable-dev-shm-usage") #
        #chrome_options.binary_location = "C:/Program Files/Google/Chrome/Application/chrome.exe" #
        if proxy is not None:
            chrome_options.add_argument('--proxy-server={0}'.format(proxy.proxy))

        if if_desired_capabilities:
        # 实现Network功能
            d = DesiredCapabilities.CHROME
            d['goog:loggingPrefs'] = {'performance': 'ALL'}
        else:
            d = None
        # if username == 'Administriter':
            # path_chromedriver = r"C:/Program Files/Google/Chrome/Application/chromedriver.exe"
        path_chromedriver = "D:/chromedriver_win32/chromedriver.exe"
        # elif username == 'HZED':
            # path_chromedriver = r"C:/Program Files (x86)/Google/Chrome/Application/chromedriver.exe"
            # path_chromedriver = r"D:/chromedriver_win32/chromedriver.exe"
        driver = Chrome(path_chromedriver, options=chrome_options)
        driver.set_page_load_timeout(60)  # timeout限时

        with open('./stealth.min.js-main/stealth.min.js') as f:
            js = f.read()

        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
          "source": js
        })
        # driver.get('https://bot.sannysoft.com/')
        # time.sleep(5)
        # driver.save_screenshot('walkaround.png')

        # # 你可以保存源代码为 html 再双击打开，查看完整结果
        # source = driver.page_source
        # with open('result.html', 'w') as f:
        #     f.write(source)
    elif u_system == 'Linux':
        chrome_options = Options()
        chrome_options.add_argument("start-maximized")
        chrome_options.add_argument("enable-automation")
        chrome_options.add_argument("--disable-infobars")
        chrome_options.add_argument("--disable-browser-side-navigation")
        chrome_options.add_argument("--headless")  # 无界面模式
        chrome_options.add_argument('--ignore-certificate-errors') #
        chrome_options.add_argument('--incognito') #
        # chrome_options.add_argument('--headless') #
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument("binary_location"+r"/usr/bin/google-chrome")

        driver = Chrome(r"/usr/bin/chromedriver", chrome_options=chrome_options)
        driver.set_page_load_timeout(60)
        with open('./stealth.min.js-main/stealth.min.js') as f:
            js = f.read()

        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
          "source": js
        })

    return driver


def close_browser(driver):
    u_system = platform.system()  # 系统平台
    username = os.environ.get("USERNAME")  # 用户名
    if u_system == 'Windows':
        driver.quit()
        chrome_options = Options()
        # chrome_options.add_argument("--headless")  # 无界面模式
        path_user_data = r"C:/Users/{}/AppData/Local/Google/Chrome/User Data".format(username)
        chrome_options.add_argument("--user-data-dir="+path_user_data)  # 加载用户数据伪装

        chrome_options.add_experimental_option('excludeSwitches', ['enable-automation'])
        chrome_options.add_argument('--ignore-certificate-errors') #
        chrome_options.add_argument('--incognito') #
        chrome_options.add_argument('--headless') #
        
        prefs = {
            "profile.managed_default_content_settings.images": 1,  # 恢复加载图片
            'permissions.default.stylesheet': 1  # 恢复加载CSS
            }

        chrome_options.add_experimental_option('prefs', prefs)
        path_chromedriver = "D:/chromedriver_win32/chromedriver.exe"
        # if username == 'RG23436':
        #     path_chromedriver = r"C:/Users/RG23436/AppData/Local/Google/Chrome/chromedriver_win32/chromedriver"
        # elif username == 'HZED':
        #     path_chromedriver = r"C:/Program Files (x86)/Google/Chrome/Application/chromedriver.exe"
        driver = Chrome(path_chromedriver, options=chrome_options)
        time.sleep(3)
        driver.quit()
    elif u_system == 'Linux':
        driver.quit()
    else:
        pass


def insertData(connection, table, data):
    if data is None or data.empty:
        return
    data = data.where(data != '', None)
    split_list = list(range(0, len(data), 1000))  # 以1000行数据量为一次插入
    split_list.append(len(data))
    for i in range(len(split_list)-1):
        if isinstance(data, pd.DataFrame):
            data = data.where(data != '', None)
            split_data = data.iloc[split_list[i]: split_list[i+1]].values.tolist()   # dataframe转list
        elif isinstance(data, list):
            data = [q if q != '' else None for q in data]
            split_data = data[split_list[i]:split_list[i+1]]
        else:
            print('数据类型不符,请使用list或者DataFrame')
            break

        values_list = str(list(data.columns)).replace('[','').replace(']','').replace('\'','')  # 构建sql中的values
        placeholder_list = '%s, ' * len(data.columns)  # 构建sql中的占位符
        placeholder_list = placeholder_list[:len(placeholder_list)-2]  # 去除占位符多于的,和空格

        # connection = pysql.connect(host=, port=3306, user='u_fk_data',
        #                            password='b',db='fk_data',charset='utf8mb4',
        #                            cursorclass = pysql.cursors.DictCursor)
        # connection = pg.connect(database="pg_fk_data",
        #                 user="postgres", password=,
        #                 host="", port="5432")
        cursor = connection.cursor()   # 通过cursor创建游标
        sql = """INSERT INTO {0} ({1}) VALUES({2})""".format(
            table, values_list, placeholder_list)  # 编写插入sql

        try:
            cursor.executemany(sql, split_data)   # 多行execute
            connection.commit()  # 提交
            print('已插入{}行数据'.format(split_list[i+1]))
        except Exception:
            raise Exception('报错：{}'.format(traceback.format_exc()))
            connection.rollback()  # 报错Exception则回滚
            connection.close()
    connection.close()


def article_cleaner(text, regex_ex=None):
    """剔除正文各种无关字符"""
    regex_list = [
        '<.*?>', '<a.*?/a>', '<b.*?/b>', '</?strong.*?>',
        '<!--.*?-->', '&nbsp;', '\u3000'
        ]
    if regex_ex is not None:
        if isinstance(regex_ex, str):
            regex_ex = [regex_ex]
        regex_list.extend(regex_ex)
    # print('regex_list', regex_list)
    for regex in regex_list:
        text = re.sub(regex, '', text)
    text = re.sub('[\t\n\r\f\v]', '', text) # 最后处理
    return text


def updateData(connection, table, data, convert={}, key_id='cms_id'):
    if data is None or data.empty:
        return
    col_dict = {**{col: col for col in data.columns}, **convert}
    if key_id not in col_dict.keys():
        return
    split_list = list(range(0, len(data), 1000))  # 以1000行数据量为一次插入
    split_list.append(len(data))
    for i in range(len(split_list)-1):
        if str(type(data)) == "<class 'pandas.core.frame.DataFrame'>":
            data = data.where(data != '', None)
            split_data = data.iloc[split_list[i]: split_list[i+1]].values.tolist()   # dataframe转list
        elif type(data) == list:
            data = [q if q != '' else None for q in data]
            split_data = data[split_list[i]:split_list[i+1]]
        else:
            print('数据类型不符,请使用list或者DataFrame')
            break
        
        #                            

        update_sql = ", ".join(['{0} = temp_table.temp_{0}'.format(key) for key, val in col_dict.items() if key != key_id])
        temp_table = ", ".join(['unnest(array{0}) as temp_{1}'.format(
            str(data[val].tolist()), key)
            for key, val in col_dict.items()])
        cursor = connection.cursor()   # 通过cursor创建游标
        sql = """
        UPDATE fk_data.{2}
        SET {0}
        FROM (
            SELECT {1}
            ) as temp_table
        where {2}.{3} = temp_table.temp_{3}

        """.format(update_sql, temp_table, table, key_id)  # 编写插入sql
        # sql = """INSERT INTO {0} ({1}) VALUES({2})""".format(
        #     table, values_list, placeholder_list)  # 编写插入sql

        try:
            cursor.execute(sql)  # 多行execute
            connection.commit()  # 提交
            print('已更新{}行数据'.format(split_list[i+1]))
        except Exception:
            print('报错：{}'.format(traceback.format_exc()))
            connection.rollback()  # 报错Exception则回滚
            connection.close()
    connection.close()

def unquote_url(url):
    return unquote(url.replace('&amp;', '&'))
