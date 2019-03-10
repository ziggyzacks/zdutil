"""Utility classes for things like database connections, configurations, & S3"""

import gzip
import io
import os
import time

import boto3
import pandas as pd
import s3fs

from zdutil.log import LogMixin


class Util:
    @property
    def env(self):
        return os.environ.get('ENV', 'DEV').lower()

    @property
    def is_prod(self):
        """is production environment?"""
        return self.env.lower() == 'prod'

    @property
    def is_dev(self):
        """is dev environment?"""
        return self.env.lower() == 'dev'


class FileSystem(Util, LogMixin):
    """base class for s3 and gcs"""

    def _fp(self, key, no_bucket=False):
        if no_bucket:
            return os.path.join(self.env, key)
        else:
            return os.path.join(self.bucket, self.env, key)


class S3Resource(FileSystem, LogMixin):
    """ helper class for dealing with interactions with S3 """
    BUCKET = 'zdutil-data'

    def __init__(self, bucket=BUCKET, verbose=False):
        self.s3 = boto3.resource('s3')
        self.client = boto3.client('s3')
        self.bucket = bucket
        self.time = time
        self.fs = s3fs.S3FileSystem()
        self.verbose = verbose

    def get_key(self, path, bucket=None):
        """
        fetches contents of key in S3

        :param str path: path to file
        :param str bucket: bucket
        :return: string of contents in path
        :rtype: str
        """
        if bucket is None:
            bucket = self.bucket
        self.logger.info(f's3://{bucket}/{path}')
        content_object = self.s3.Object(bucket, path)
        return content_object.get()['Body'].read().decode('utf-8')

    def _get_paths(self, prefix, just_key=False):
        """
        fetches all S3 paths with given prefix

        :param str prefix: prefix to filter by (e.g. stripe, ios/2018, etc)
        :param bool just_key: do you want just the key? (note key is full path minus "s3://" and the bucket
        :return: generator of paths that match prefix
        """
        for obj in self.s3.Bucket(self.bucket).objects.filter(Prefix=prefix):
            path = obj.key if just_key else f's3://{obj.bucket_name}/{obj.key}'
            yield path

    def upload(self, key):
        """
        upload a local file to S3

        :param str key: path to save to
        :return:
        """
        fp = self._fp(key, no_bucket=True)
        self.logger.info(f'Saving to: {fp}')
        return self.s3.Bucket(self.bucket).upload_file(Key=fp, Filename=key)

    def write(self, path, content):
        """
        put content to key

        :param str key: path to save to
        :param content: content to put
        :return: full path written to
        """
        s3path = os.path.join('s3://', self._fp(path))
        with self.fs.open(s3path, 'wb') as f:
            f.write(content)
        return s3path

    def download(self, key):
        """
        downloads file from S3 to local filesystem

        :param str key: path to file in S3
        :return:
        """
        fp = self._fp(key, no_bucket=True)
        self.logger.info(f'Downloading from: {fp}')
        return self.s3.Bucket(self.bucket).download_file(Key=fp, Filename=key)

    def write_df(self, path, df, header=False, index=False, gzipped=False):
        """
        writes dataframe to S3

        :param str path: path to save to
        :param pd.DataFrame df: actual dataframe to save
        :param bool header: w/header or nah?
        :return: full S3 path saved to
        :rtype: str
        """
        s3path = os.path.join('s3://', self._fp(path))
        if self.verbose:
            self.logger.debug(f'Saving dataframe with shape {df.shape} to {s3path}')
        with self.fs.open(s3path, 'wb') as f:
            body = df[sorted(df.columns)].to_csv(index=index, header=header).encode()
            if gzipped:
                body = gzip.compress(body)
            f.write(body)
        return s3path

    def read_df(self, path, columns=None, gzipped=False, header=None):
        """
        reads dataframe from s3

        :param str path: path to read from
        :return: dataframe from string in path
        """
        if 's3' not in path:
            path = os.path.join('s3://', self._fp(path))

        with self.fs.open(path, 'rb') as f:
            if gzipped:
                csv = gzip.GzipFile(fileobj=f)
            else:
                data_str = f.read().decode()
                csv = io.StringIO(data_str)

            df = pd.read_csv(csv, header=header)
            if columns is not None:
                df.columns = columns
        return df

    def presigned(self, path, ttl=3600):
        """
        generate presigned URL to download S3 files

        :param str path: path to file (no bucket)
        :param int ttl: time to live
        :return: presigned S3 URL
        :rtype: str
        """
        params = {
            'Bucket': self.bucket,
            'Key': path
        }
        return self.client.generate_presigned_url('get_object', Params=params, ExpiresIn=ttl)
