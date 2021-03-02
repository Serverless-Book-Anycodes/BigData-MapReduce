# -*- coding: utf8 -*-
import oss2
from operator import itemgetter
import os
import sys
import datetime
import logging

logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger()
logger.setLevel(level=logging.INFO)
auth = oss2.Auth('<AccessKeyID>', '<AccessKeySecret>' )
middle_bucket = oss2.Bucket(auth, 'http://oss-cn-hangzhou.aliyuncs.com', 'serverless-book-mr-middle')
target_bucket = oss2.Bucket(auth, 'http://oss-cn-hangzhou.aliyuncs.com', 'serverless-book-mr-target')


def delete_file_folder(src):
    if os.path.isfile(src):
        try:
            os.remove(src)
        except:
            pass
    elif os.path.isdir(src):
        for item in os.listdir(src):
            itemsrc = os.path.join(src, item)
            delete_file_folder(itemsrc)
        try:
            os.rmdir(src)
        except:
            pass


def download_file(key, download_path):
    logger.info("Download file [%s]" % (key))
    try:
        middle_bucket.get_object_to_file(key, download_path)
    except Exception as e:
        print(e)
        return -1
    return 0


def upload_file(key, local_file_path):
    logger.info("Start to upload file to oss")
    try:
        target_bucket.put_object_from_file(key, local_file_path)
    except Exception as e:
        print(e)
        return -1
    logger.info("Upload data map file [%s] Success" % key)
    return 0


def alifc_reducer(key, result_key):
    word2count = {}
    src_file_path = u'/tmp/' + key.split('/')[-1]
    result_file_path = u'/tmp/' + u'result_' + key.split('/')[-1]
    download_ret = download_file(key, src_file_path)
    if download_ret == 0:
        map_file = open(src_file_path, 'r')
        result_file = open(result_file_path, 'w')
        for line in map_file:
            line = line.strip()
            word, count = line.split('\t', 1)
            try:
                count = int(count)
                word2count[word] = word2count.get(word, 0) + count
            except ValueError:
                logger.error("error value: %s, current line: %s" % (ValueError, line))
                continue
        map_file.close()
        delete_file_folder(src_file_path)
        sorted_word2count = sorted(word2count.items(), key=itemgetter(1))[::-1]
        for wordcount in sorted_word2count:
            res = '%s\t%s' % (wordcount[0], wordcount[1])
            result_file.write(res)
            result_file.write('\n')
        result_file.close()
        upload_ret = upload_file(result_key, result_file_path)
        delete_file_folder(result_file_path)
        return upload_ret
    else:
        return -1


def reduce_caller(event):
    key = event["events"][0]["oss"]["object"]["key"]
    logger.info("Key is " + key)
    result_key = '/' + 'result_' + key.split('/')[-1]
    return alifc_reducer(key, result_key)


def main_handler(event, context):
    logger.info("start main handler")
    if "Records" not in event.keys():
        return {"errorMsg": "event is not come from cos"}
    start_time = datetime.datetime.now()
    res = reduce_caller(event)
    end_time = datetime.datetime.now()
    print("data reducing duration: " + str((end_time - start_time).microseconds / 1000) + "ms")
    if res == 0:
        return "Data reducing SUCCESS"
    else:
        return "Data reducing FAILED"
