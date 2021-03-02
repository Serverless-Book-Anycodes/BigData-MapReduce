# -*- coding: utf8 -*-
import datetime
import oss2
import re
import os
import sys
import logging

logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger()
logger.setLevel(level=logging.INFO)
auth = oss2.Auth('<AccessKeyID>', '<AccessKeySecret>')
source_bucket = oss2.Bucket(auth, 'http://oss-cn-hangzhou.aliyuncs.com', 'serverless-book-mr-origin')
middle_bucket = oss2.Bucket(auth, 'http://oss-cn-hangzhou.aliyuncs.com', 'serverless-book-mr-middle')


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
        source_bucket.get_object_to_file(key, download_path)
    except Exception as e:
        print(e)
        return -1
    return 0


def upload_file(key, local_file_path):
    logger.info("Start to upload file to oss")
    try:
        middle_bucket.put_object_from_file(key, local_file_path)
    except Exception as e:
        print(e)
        return -1
    logger.info("Upload data map file [%s] Success" % key)
    return 0


def do_mapping(key, middle_file_key):
    src_file_path = u'/tmp/' + key.split('/')[-1]
    middle_file_path = u'/tmp/' + u'mapped_' + key.split('/')[-1]
    download_ret = download_file(key, src_file_path)  # download src file
    if download_ret == 0:
        inputfile = open(src_file_path, 'r')  # open local /tmp file
        mapfile = open(middle_file_path, 'w')  # open a new file write stream
        for line in inputfile:
            line = re.sub('[^a-zA-Z0-9]', ' ', line)  # replace non-alphabetic/number characters
            words = line.split()
            for word in words:
                mapfile.write('%s\t%s' % (word, 1))  # count for 1
                mapfile.write('\n')
        inputfile.close()
        mapfile.close()
        upload_ret = upload_file(middle_file_key, middle_file_path)  # upload the file's each word
        delete_file_folder(src_file_path)
        delete_file_folder(middle_file_path)
        return upload_ret
    else:
        return -1


def map_caller(event):
    key = event["events"][0]["oss"]["object"]["key"]
    logger.info("Key is " + key)
    middle_file_key = 'middle_' + key.split('/')[-1]
    return do_mapping(key, middle_file_key)


def handler(event, context):
    logger.info("start main handler")
    start_time = datetime.datetime.now()
    res = map_caller(event)
    end_time = datetime.datetime.now()
    print("data mapping duration: " + str((end_time - start_time).microseconds / 1000) + "ms")
    if res == 0:
        return "Data mapping SUCCESS"
    else:
        return "Data mapping FAILED"
