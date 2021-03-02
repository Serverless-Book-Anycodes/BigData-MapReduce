# 帮助文档

- 配套章节：函数计算与对象存储实现WordCount
- 函数计算使用方法：需要自行配置对象存储Bucket和用户密钥信息，例如:
```python
auth = oss2.Auth('<AccessKeyID>', '<AccessKeySecret>')
source_bucket = oss2.Bucket(auth, 'http://oss-cn-hangzhou.aliyuncs.com', 'serverless-book-mr-origin')
middle_bucket = oss2.Bucket(auth, 'http://oss-cn-hangzhou.aliyuncs.com', 'serverless-book-mr-middle')
```
