#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# @Author ：Tingting Yang
# @Time ：2020/9/10 15:12
# @File : main_hk01.py

import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
from selenium import webdriver
import selenium.webdriver.support.ui as ui
import time
import threading
from langconv import *

from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
import logging
import pymysql

driver = webdriver.Chrome()
wait = ui.WebDriverWait(driver, 10)

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='log.txt',
                    filemode='a')

headers = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.132 Safari/537.36"
}

threadLock = threading.Lock()

# 获取爬取页面列表
def get_urllist(url, file):
    href_crawled = []  # 已经爬过的数据
    if os.path.exists(file):
        if os.path.getsize(file):
            crawled_data = pd.read_csv(file, sep='\t', header=0)
            href_crawled = crawled_data['address'].values
    driver.get(url)
    try:
        for i in range(10):
            # x水平，y垂直
            js = 'window.scrollTo(0,%s)' % (i * 2000)  # 下拉加载更多新闻
            driver.execute_script(js)
            time.sleep(2)
            print(f"i:{i}")
    except:
        print("no more news to update!")
    hrefs = driver.find_elements_by_xpath("//div[@class='infinite-scroll-item sc-bdVaJa fpBbNM']/div/span/a")
    hrefs_list = []
    for href in hrefs:
        href = href.get_attribute("href")
        if href not in href_crawled:
            hrefs_list.append(href)

    return hrefs_list


# 将繁体转换成简体
def tradition2simple(line):
     line = Converter('zh-hans').convert(line)
     return line


# 页面信息爬取
def crawl(thread_id, url):
    print('{} - {}'.format(thread_id, url))
    data_html = requests.get(url, headers=headers).text
    data_soup = BeautifulSoup(data_html, 'lxml')
    try:
        title = tradition2simple(data_soup.find('h1', {'class': "s1jdnux-3 bLkyjc sc-gqjmRU iEiEQ"}).text)
        date = data_soup.find('div', {'class': "pubdate sc-bwzfXH loXcNn sc-bdVaJa jJqEVj"}).text
        content = data_soup.find('article', {'class': "sc-bwzfXH liBCIH sc-bdVaJa iMCZeY"})
        content = content.select('p')
        s_content = ""
        for c in content:
            s_content = s_content + c.text.strip()
        for ch in ['\t', '\n']:
            title = title.replace(ch, '')
            s_content = s_content.replace(ch, '')
        s_content = tradition2simple(s_content)
        threadLock.acquire()
        if title != "" and s_content != "":
            d = (title, s_content, date, url)
            data.append(d)
        threadLock.release()
    except AttributeError:
        print("Attribute Error.")


# 多线程爬虫
class Thread_crawler(threading.Thread):
    def __init__(self, thread_id, urls):
        threading.Thread.__init__(self)
        self.thread_id = thread_id
        self.urls = urls

    def run(self):
        for url in self.urls:
            crawl(self.thread_id, url)


def main():
    baseurl = 'https://www.hk01.com'  # 香港01日报
    output_file = "hk01.csv"
    url = baseurl + "/channel/310/%E6%94%BF%E6%83%85"   # 政治板块
    global data
    data = []
    num_thread = 4    # 线程数
    threads = []
    url_list = get_urllist(url, output_file)
    print("num_url - {}".format(len(url_list)))
    block = len(url_list) // num_thread
    for i in range(num_thread):
        if i < num_thread - 1:
            urls = url_list[i*block: (i+1)*block]
        else:
            urls = url_list[i*block:]
        threads.append(Thread_crawler(i, urls))
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    # 写入csv文件
    Title, Content, Date, Link = zip(*data)
    print(Title)
    print(len(Title))
    result = pd.DataFrame({"title": Title, "content": Content, "date": Date,
                           "address": Link, "location": ["HK"] * len(Link)})
    if os.path.exists(output_file):
        result.to_csv(output_file, sep='\t', header=False, index=False, mode='a')
    else:
        result.to_csv(output_file, sep='\t', index=False)


if __name__ == '__main__':
    main()
