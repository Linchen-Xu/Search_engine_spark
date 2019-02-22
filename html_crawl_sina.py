
# coding: utf-8

# In[1]:


import sys
import jieba
from os import path
from bs4 import BeautifulSoup
import os
import urllib.request
import urllib.parse
import re
import urllib.request, urllib.parse, http.cookiejar
import codecs
from kafka import KafkaProducer
from datetime import datetime
import requests
import time


# In[2]:


url = 'https://feed.mix.sina.com.cn/api/roll/get?pageid=153&lid=2509&k=&num=50&page=1'

headers = {
    'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2272.101 Safari/537.36'
}


# In[3]:


def getData():
    r = requests.get(url,headers=headers)
    a = r.json()
    return a['result']['data']


# In[4]:


# from kafka import KafkaProducer

# producer = KafkaProducer(bootstrap_servers=['10.255.64.92:9092'])
# for i in range(10000):
#     msg = "msg%d" % i
#     producer.send('test', msg)
# producer.close()


# In[5]:


#获取网页内容

#读取网页
def getHtml(url):
    cj = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
    opener.addheaders = [('User-Agent',
    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2272.101 Safari/537.36'),('Cookie', '4564564564564564565646540')]

    urllib.request.install_opener(opener)
    try:
        html_bytes = urllib.request.urlopen(url).read()
    except:
        return '',False
    
    html_string = ''
    try:
        html_string = html_bytes.decode('utf-8')
    except:
        try:
            html_string = html_bytes.decode('gbk')
        except:
            try:
                html_string = html_bytes.decode('gb2312')
            except:
                return '',False
    return html_string,True

#分词

re_cdata=re.compile('//<!\[CDATA\[[^>]*//\]\]>',re.I)#处理CDATA
re_doctype=re.compile('<!DOCTYPE HTML PUBLIC[^>]*>',re.I)  
re_doctype2=re.compile('<!DOCTYPE HTML[^>]*>',re.I) 
re_script=re.compile(r'<script[\s\S]+?/script>',re.I)#处理script
re_style=re.compile(r'<style[\s\S]+?/style>',re.I)#处理style
re_div=re.compile('<\s*div[^>]*>[^<]*<\s*/\s*div\s*>',re.I)#处理div
re_br = re.compile('<br\s*?/?>',re.I)  
re_h=re.compile('</?\w+[^>]*>',re.I)#HTML标签
re_comment = re.compile('<!--[\s\S]-->',re.I)  

def processHtml(file_content):
    file_content = re_cdata.sub('',file_content)
#     print('1 content:',file_content)
    file_content = re_doctype.sub('',file_content)
#     print('2 content:',file_content)
    file_content = re_doctype2.sub('',file_content)
#     print('3 content:',file_content)
    file_content = re_script.sub('',file_content)
#     print('4 content:',file_content)
    file_content = re_style.sub('',file_content)
#     print('5 content:',file_content)
    file_content = re_div.sub('',file_content)
#     print('6 content:',file_content)
    file_content = re_h.sub('',file_content)
#     print('7 content:',file_content)
    file_content = re_comment.sub('',file_content)
#     print('8 content:',file_content)
    file_content = re.sub('[\r\n\t  !©<>“”»《》!:,?。.：？，！、　•"_◆×|()-【】]）（；%&👉📅', '', file_content)
#     print('9 content:',file_content)
    file_content = re.sub('DOCTYPEdoctypehtml', '', file_content)
#     print('10 content:',file_content)
    seg_list = jieba.cut_for_search(file_content)
    return '/'.join(seg_list)


# In[6]:


# while True:
#     print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
#     a = getData()
#     for i in a:
#         print(i['title'],'\n',i['url'])
#         soup = BeautifulSoup(html_doc, 'html.parser')
#         pass
#     time.sleep(10)


# In[17]:


# reload(sys)
# sys.setdefaultencoding('utf-8')
producer = KafkaProducer(bootstrap_servers=['172.17.11.54:9092','172.17.11.55:9092','172.17.11.56:9092'],api_version = (0,10,1))


# In[18]:


arr = []
while True:
    print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    a = getData()
    for i in a:
        key = i['url']
        if key in arr:
            continue
        html_doc,success_crwal = getHtml(key)
        if success_crwal:
            arr.append(key)
            soup = BeautifulSoup(html_doc, 'html.parser')
            tmp = soup.find('div',attrs={'class':'article-content-left'})
#             print(tmp)
            value = processHtml(str(tmp))
            print('key:',key)
#             print('value:',value)
            future = producer.send('test', value=bytes(str(key).encode('utf8')),key=bytes(str(value).encode('utf8')))
            result = future.get(timeout=10)
        pass
#     time.sleep(10)


producer.close()
print('done')


# In[ ]:


# import sys
# reload(sys)
# sys.setdefaultencoding('utf-8')

# from kafka import KafkaProducer

# producer = KafkaProducer(bootstrap_servers=['172.17.11.54:9092','172.17.11.55:9092','172.17.11.56:9092'])
# msg = ''
# for i in range(10):
#     #msg = 'msg' + str(i)
#     msg = str(i)
#     value = '哈哈哈'
#     producer.send('test', value=bytes(value.encode('utf8')),key=bytes(msg.encode('utf8')))
#     pass

# producer.close()
# print('done')

