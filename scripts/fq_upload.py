#!/bin/env python
#
# @note Copyright (C) 2019, Atgenomix Incorporated. All Rights Reserved.
#       This program is an unpublished copyrighted work which is proprietary to
#       Atgenomix Incorporated and contains confidential information that is not to
#       be reproduced or disclosed to any other person or entity without prior
#       written consent from Atgenomix, Inc. in each and every instance.
#
# @warning Unauthorized reproduction of this program as well as unauthorized
#          preparation of derivative works based upon the program or distribution of
#          copies by sale, rental, lease or lending are violations of federal copyright
#          laws and state trade secret laws, punishable by civil and criminal penalties.
#
# @file    fq_upload.py
#
# @brief   Chunk pair-end FASTQ files into small blocks and upload them to HDFS
#
# @author  Chung-Tsai Su(chungtsai.su@atgenomix.com)
#
# @date    2019/09/18
#
# @version 1.0
#
# @remark
#
import sys
import getopt
import zlib
import subprocess
import os
import uuid
import shutil
import io
import logging
import traceback
import re
import tempfile
from logging.handlers import RotatingFileHandler
from hdfs import Config

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

FILE_UPLOAD_TEMP_DIR = '/seqslab/tmp'
VARTOTAL_STORAGE_CHOICES = (
    (0, 'Local disk'),
    (1, 'HDFS'),
    (2, 'S3'),
    (3, 'GCS'),
    (4, 'ADLS'),
)

VARTOTAL_STORAGE = VARTOTAL_STORAGE_CHOICES[1][0]
VARTOTAL_STORAGE_ROOT = '/seqslab'
VARTOTAL_STORAGE_USR_FOLDER = os.path.join(VARTOTAL_STORAGE_ROOT, 'usr')
VARTOTAL_STORAGE_SYS_FOLDER = os.path.join(VARTOTAL_STORAGE_ROOT, 'system')
VARTOTAL_STORAGE_BED_FOLDER = os.path.join(VARTOTAL_STORAGE_SYS_FOLDER, 'bed')
VARTOTAL_STORAGE_LOCI_FOLDER = os.path.join(VARTOTAL_STORAGE_SYS_FOLDER, 'loci')
VARTOTAL_STORAGE_CACHE_FOLDER = os.path.join(VARTOTAL_STORAGE_SYS_FOLDER, 'cache')
VARTOTAL_STORAGE_INTERVAL_FOLDER = os.path.join(VARTOTAL_STORAGE_SYS_FOLDER, 'target_interval')

SEQSLAB_LOCAL_HOME = '/usr/local/seqslab'
SEQSLAB_LOCAL_TEMP = '/seqslab/tmp'
SEQSLAB_LOCAL_ARTEMIS_FOLDER = os.path.join(SEQSLAB_LOCAL_HOME, 'artemis')
SEQSLAB_LOCAL_ETC_FOLDER = os.path.join(SEQSLAB_LOCAL_ARTEMIS_FOLDER, 'etc')
SEQSLAB_LOCAL_LOG_FOLDER = os.path.join(SEQSLAB_LOCAL_ARTEMIS_FOLDER, 'logs')
SEQSLAB_LOCAL_CORE_FOLDER = os.path.join(SEQSLAB_LOCAL_ARTEMIS_FOLDER, 'core')

SEQSLAB_USER = 'root' # 'artemis'
SEQSLAB_GROUP = 'root' # 'artemis'

logger = logging.getLogger('chunkwise_logger')
sys.path.append('/usr/local/seqslab/artemis')

LOG_LEVEL = logging.WARNING    ##DEBUG, INFO, WARNING, ERROR, CRITICAL
LOG_FILENAME = os.path.join("/tmp", 'chunkwise-uploader.log')
LOG_MAXBYTES = 10*1024*1024 #10MB
LOG_BACKUPCOUNT = 5
LOG_FORMAT = '%(asctime)s [%(levelname)s] <%(process)s:%(thread)s> %(pathname)s:%(lineno)d\t%(funcName)s\t%(message)s'

logger.setLevel(LOG_LEVEL)

ch = logging.StreamHandler()
ch.setLevel(LOG_LEVEL)

rh = RotatingFileHandler(LOG_FILENAME, maxBytes=LOG_MAXBYTES, backupCount=LOG_BACKUPCOUNT)
rh.setLevel(LOG_LEVEL)

formatter = logging.Formatter(LOG_FORMAT)
ch.setFormatter(formatter)
rh.setFormatter(formatter)

logger.addHandler(ch)
logger.addHandler(rh)

# constant
##############################################

SNZIP = '/usr/local/bin/snzip'
HADOOP = '/usr/local/hadoop/bin/hadoop'
TMP_DIR = FILE_UPLOAD_TEMP_DIR
BUFFER_READ_SIZE = 16384
GZ_SPLIT_CHUNK_SIZE = 256 * 1024 * 1024
SUBPROCESS_NUM = 3
PLAIN_TEXT_LENGTH_LIMIT = 600 * 1024 * 1024
HDFS_NAME_TEMPLATE = '{}/part-{}.fastq.snappy'
FS_NAME_TEMPLATE = '{}/part-{}.fq'
FS_NAME_PAIRED_TEMPLATE = '{}/part-{}-{}.fq'
CONTENT_BUFFER_SIZE = 10240
SNZIP_N_UPLOAD_RETRY = 20
PAIRED_TWO_SEARCH_SCOPE = 1 * 1024 * 1024
PAIR_TWO_SEARCH_BUF_SIZE = 10 * 1024

# class
##############################################

class BaseDatasetsStorage:
    def __init__(self):
        self._logger = logging.getLogger()

    def resolve(self, path):
        raise NotImplemented

    def makedirs(self, path, permission=None):
        raise NotImplemented

    def delete(self, path):
        raise NotImplemented

    def rmdir(self, path):
        raise NotImplemented

    def content(self, path):
        """
        ContentSummary :
        {
            "directoryCount":
            {
                "description": "The number of directories.",
                "type": "integer",
                "required": true
            },
            "fileCount":
            {
                "description": "The number of files.",
                "type": "integer",
                "required": true
            },
            "length":
            {
                "description": "The number of bytes used by the content.",
                "type": "integer",
                "required": true
            },
            "quota":
            {
                "description": "The namespace quota of this directory.",
                "type": "integer",
                "required": true
            },
            "spaceConsumed":
            {
                "description": "The disk space consumed by the content.",
                "type": "integer",
                "required": true
            },
            "spaceQuota":
            {
                "description": "The disk space quota.",
                "type": "integer",
                "required": true
            }
        }
        """
        raise NotImplemented

    def chown(self, path, user, group):
        raise NotImplemented

    def list(self, path, status=False):
        raise NotImplemented

    def upload(self, src_path, dst_path):
        raise NotImplemented

    def download(self, storage_path, local_path, overwrite=False):
        raise NotImplemented

    def status(self, path):
        raise NotImplemented

    # This method must be called using a with block
    def read(self, path, offset=0, length=None, encoding=None, delimiter=None):
        raise NotImplemented

    # This method must be called using a with block
    def write(self, path, overwrite=False, append=False):
        raise NotImplemented

    def set_replication(self, path, replication):
        raise NotImplemented

    def rename(self, src_path, dst_path):
        raise NotImplemented

    def create_user_folders(self, username):
        raise NotImplemented

    def get_usr_path(self, user_name):
        return os.path.join(VARTOTAL_STORAGE_USR_FOLDER, user_name)

    def get_usr_fastq_path(self, user_name):
        return os.path.join(VARTOTAL_STORAGE_USR_FOLDER, user_name, 'reads')

    def get_usr_bam_path(self, user_name):
        return os.path.join(VARTOTAL_STORAGE_USR_FOLDER, user_name, 'alignment')

    def get_usr_vcf_path(self, user_name):
        return os.path.join(VARTOTAL_STORAGE_USR_FOLDER, user_name, 'variant')

    def get_usr_ped_path(self, user_name):
        return os.path.join(VARTOTAL_STORAGE_USR_FOLDER, user_name, 'pedigree')

    def get_usr_jobs_path(self, user_name):
        return os.path.join(VARTOTAL_STORAGE_USR_FOLDER, user_name, 'jobs')

    def create_new_vcf_storage_path(self, user_name):
        return os.path.join(self.get_usr_vcf_path(user_name), str(uuid.uuid4()) + '/')

    def create_new_bam_storage_path(self, user_name):
        return os.path.join(self.get_usr_bam_path(user_name), str(uuid.uuid4()) + '/')

    def create_new_fastq_storage_path(self, user_name):
        return os.path.join(self.get_usr_fastq_path(user_name), str(uuid.uuid4()) + '/')

    def create_new_ped_storage_path(self, user_name):
        return os.path.join(self.get_usr_ped_path(user_name), str(uuid.uuid4()) + '.ped')


class HdfsDatasetsStorage(BaseDatasetsStorage):

    def __init__(self):
        super(BaseDatasetsStorage, self).__init__()
        self._dfs = Config().get_client('prod')

    def resolve(self, path):
        return self._dfs.resolve(path)

    def makedirs(self, path, permission=None):
        self._dfs.makedirs(path, permission)

    def delete(self, path):
        self._dfs.delete(path, recursive=False)

    def rmdir(self, path):
        self._dfs.delete(path, recursive=True)

    def content(self, path):
        return self._dfs.content(path, strict=False)

    def list(self, path, status=False):
        return self._dfs.list(path, status)

    def chown(self, path, user, group):
        self._dfs.set_owner(path, owner=user, group=group)

    def upload(self, src_path, dst_path):
        self._dfs.upload(dst_path, src_path, n_threads=5, chunk_size=10*1024*1024)

    def download(self, storage_path, local_path, overwrite=False):
        return self._dfs.download(storage_path, local_path, overwrite=overwrite, n_threads=3, chunk_size=100*1024*1024)

    def status(self, path):
        return self._dfs.status(path, False)

    def read(self, path, offset=0, length=None, encoding=None, delimiter=None):
        return self._dfs.read(path, offset=offset, length=length, encoding=encoding, delimiter=delimiter)

    def write(self, path, overwrite=False, append=False):
        return self._dfs.write(path, overwrite=overwrite, append=append, blocksize=128*1024*1024, buffersize=10*1024*1024)

    def set_replication(self, path, replication):
        self._dfs.set_replication(path, replication)

    def rename(self, src_path, dst_path):
        self._dfs.rename(src_path, dst_path)

    def create_user_folders(self, username):
        self.makedirs(self.get_usr_path(username))
        self.makedirs(self.get_usr_fastq_path(username))
        self.makedirs(self.get_usr_bam_path(username))
        self.makedirs(self.get_usr_vcf_path(username))
        self.makedirs(self.get_usr_ped_path(username))
        self.makedirs(self.get_usr_jobs_path(username))
        self.chown(self.get_usr_path(username), SEQSLAB_USER, SEQSLAB_GROUP)
        self.chown(self.get_usr_fastq_path(username), SEQSLAB_USER, SEQSLAB_GROUP)
        self.chown(self.get_usr_bam_path(username), SEQSLAB_USER, SEQSLAB_GROUP)
        self.chown(self.get_usr_vcf_path(username), SEQSLAB_USER, SEQSLAB_GROUP)
        self.chown(self.get_usr_ped_path(username), SEQSLAB_USER, SEQSLAB_GROUP)
        self.chown(self.get_usr_jobs_path(username), SEQSLAB_USER, SEQSLAB_GROUP)


class DatasetStorageClient(object):
    instance = None

    def __new__(cls):
        if not DatasetStorageClient.instance:
            DatasetStorageClient.instance = HdfsDatasetsStorage()
        return DatasetStorageClient.instance

    def __getattr__(self, name):
        return getattr(self.instance, name)

    def __setattr__(self, name):
        return setattr(self.instance, name)


os.environ['HDFSCLI_CONFIG'] = os.path.join(SEQSLAB_LOCAL_ETC_FOLDER, 'hdfscli.cfg')
dfs_client = DatasetStorageClient()


class DatasetsStorageError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return self.value


class GzChunkFileWrapper:
    def __init__(self, source, is_dir=True, input_chunk_size=GZ_SPLIT_CHUNK_SIZE, handle_buff_size=1024 * 1024):
        self.decompressor = zlib.decompressobj(zlib.MAX_WBITS | 16)
        self.input_chunk_size = input_chunk_size
        self.index = 0
        self.source = source
        self.is_dir = is_dir

        if not os.path.isdir(self.source) and is_dir:
            logger.error("provided source not a directory {} when is_dir==True".format(self.source))
            raise Exception("provided source not a directory {} when is_dir==True".format(self.source))
        if os.path.isdir(self.source) and not is_dir:
            logger.error("provided source not a file {} when is_dir!=True".format(self.source))
            raise Exception("provided source not a file {} when is_dir!=True".format(self.source))

        self.path_list, self.path_list_size, self.list_last_index = self.init_input_file_list()
        self.source_handle, self.buff, self.handle = self.init_file_handle(self.input_chunk_size, handle_buff_size)
        logger.info('path_list {}; path_list_length {}'.format(self.path_list, self.path_list_size))

    def __del__(self):
        self.buff.close()
        self.handle.close()

    def init_input_file_list(self):
        if self.is_dir:
            tmp = [f for f in os.listdir(self.source)]
            logger.debug('input chunk list -- {}'.format(tmp))
            tmp.sort(key=int)
            chunk_list = ["{}/{}".format(self.source, f) for f in tmp]
        else:
            chunk_list = [self.source]
        return chunk_list, len(chunk_list), len(chunk_list) - 1

    def init_file_handle(self, chunk_size, handle_buff_size):
        f = open(self.path_list[self.index], "rb")
        buff = io.BytesIO(f.read(chunk_size))
        return f, buff, io.BufferedReader(buff, handle_buff_size)

    def read(self, length=BUFFER_READ_SIZE):
        try:
            total = 0
            buffer = self.handle.read(length)
            # return length < read length => self.buff is consumed, and need to load next chunk to self.buff
            if len(buffer) < length:
                logger.debug('buffer.len -- {}'.format(len(buffer)))
                self.append_next_chunk()
                buffer += self.handle.read(length)

            # decompress buffer (gz.compressed) to outstr (binary string)
            outstr = self.decompressor.decompress(buffer)
            total += len(outstr)

            # loop through all the unused_data, and append decompressed portion into outstr
            while self.decompressor.unused_data != b'':
                unused_data = self.decompressor.unused_data
                self.decompressor = zlib.decompressobj(zlib.MAX_WBITS | 16)
                tmp = self.decompressor.decompress(unused_data)
                total += len(tmp)
                outstr += tmp
        except:
            logger.error('input decompress error -- {}'.format(traceback.format_exc()))
            raise DatasetsStorageError('input decompress error -- {}'.format(traceback.format_exc()))

        return len(outstr) == 0, outstr

    def append_next_chunk(self):
        # check whether source_handle has been consumed
        if self.index > self.list_last_index:
            return

        content = self.source_handle.read(self.input_chunk_size)

        if not content:
            # remove consumed chunk file
            self.source_handle.close()
            if self.is_dir:
                os.unlink(self.path_list[self.index])

            # about to append next chunk
            self.index += 1

            if self.index > self.list_last_index:
                return

            self.source_handle = open(self.path_list[self.index], 'rb')

            # append chunk routine
            remain_buf = self.buff.read(sys.getsizeof(self.buff) - self.buff.tell())
            self.buff.truncate(0)
            self.buff.seek(0)
            self.buff.write(remain_buf)
            self.buff.write(self.source_handle.read(self.input_chunk_size))
            logger.debug("__appended chunk No. {}, with buf length -- {}".format(self.index, sys.getsizeof(self.buff)))
            self.buff.seek(0)
        else:
            remain_buf = self.buff.read(sys.getsizeof(self.buff) - self.buff.tell())
            self.buff.truncate(0)
            self.buff.seek(0)
            self.buff.write(remain_buf)
            self.buff.write(content)
            logger.debug("appending next source file content, with buf length -- {}".format(sys.getsizeof(self.buff)))
            self.buff.seek(0)


class FastqChunkwiseChopNUploader:
    def __init__(self, file_dir_list, dest_path,
                 fastq_txt_chunk_size=PLAIN_TEXT_LENGTH_LIMIT,
                 input_chunk_size=GZ_SPLIT_CHUNK_SIZE,
                 write_header=True):
        self.write_header = write_header
        self.upload_retried_count = 0
        self.source_handle = []
        logger.info(file_dir_list)
        logger.info(dest_path)
        for item in file_dir_list:
            if os.path.isdir(item):
                self.source_handle.append(GzChunkFileWrapper(item, True, input_chunk_size))
            elif os.path.isfile(item):
                self.source_handle.append(GzChunkFileWrapper(item, False, input_chunk_size))
            else:
                logger.error('input source {} is not a directory nor a file'.format(item))
                raise DatasetsStorageError('input source {} is not a directory nor a file'.format(item))

        self.result_dest = dest_path
        self.tmp_dir = '{}/{}'.format(TMP_DIR, uuid.uuid4())
        try:
            os.mkdir(self.tmp_dir)
        except FileExistsError:
            logger.error("{} -- already exist".format(self.tmp_dir))
        self.init_dest_dir()
        # for single-end file, self.fastq_txt_chunk_size = fastq_txt_chunk_size
        # for paired-end file, self.fastq_txt_chunk_size = 1/2 * fastq_txt_chunk_size
        self.fastq_txt_chunk_size = int(fastq_txt_chunk_size / len(self.source_handle))
        logger.debug('source dir: {}, dest dir: {}, chunk fastq plain text size {}'.format(
            file_dir_list, format(dest_path), self.fastq_txt_chunk_size))

    def init_dest_dir(self):
        dfs_client.makedirs(self.result_dest)
        logger.info('result dest dir -- {}'.format(self.result_dest))

    @staticmethod
    def find_split_point(shortfall, content):
        content_size = len(content)
        idx1 = shortfall
        while idx1 < content_size:
            if content[idx1] == ord('\n') and content[idx1 + 1] == ord('@'):
                idx2 = idx1 + 1
                while idx2 < content_size:
                    if content[idx2] == ord('\n'):
                        if content[idx2 + 1] == ord('@'):
                            return idx2 + 1
                        else:
                            return idx1 + 1
                    idx2 += 1
            idx1 += 1
        logger.error("cannot find proper position for partitioning fastq files")
        return -1

    def process_file(self, chunk_file_path, chunk_index, pid):
        # compress and upload file
        cmd = '{0} -c -t hadoop-snappy {1} | {2} fs -put - {3}'.format(SNZIP,
                                                                       chunk_file_path,
                                                                       HADOOP,
                                                                       HDFS_NAME_TEMPLATE.format(self.result_dest,
                                                                                                 str(chunk_index).zfill(5)))
        logger.debug(cmd)
        pid.append((subprocess.Popen(cmd, shell=True), cmd, [chunk_file_path]))
        if len(pid) > SUBPROCESS_NUM:
            return self.join_subprocess(pid)
        return pid

    def join_subprocess(self, pid):
        retry_pid = []
        for item in pid:
            return_code = item[0].wait()
            if return_code != 0:
                if self.upload_retried_count < SNZIP_N_UPLOAD_RETRY:
                    self.upload_retried_count += 1
                    logger.error('retry {} with command {}'.format(self.upload_retried_count, item[1]))
                    retry_pid.append((subprocess.Popen(item[1], shell=True), item[1], item[2]))
                else:
                    logger.error('compress process error failed {} time'.format(SNZIP_N_UPLOAD_RETRY))
                    raise DatasetsStorageError('compress process error failed {} time'.format(SNZIP_N_UPLOAD_RETRY))
            else:
                for f in item[2]:
                    try:
                        os.remove(f)
                    except:
                        logger.error('failed to remove {}'.format(f))
        pid = retry_pid
        return pid

    def run(self):
        if len(self.source_handle) == 1:
            logger.info('run fastq upload -- single mode')
            self.run_single()
        else:
            logger.info('run fastq upload -- paired mode')
            self.run_paired()

    def run_single(self):
        try:
            chunk_index = 0
            written_length = 0
            chunk_file_path = FS_NAME_TEMPLATE.format(self.tmp_dir, chunk_index)
            chunk_f = open(chunk_file_path, 'wb')
            pid = []
            while True:
                eof, content = self.source_handle[-1].read()
                shortfall_length = self.fastq_txt_chunk_size - written_length

                if len(content) > shortfall_length + CONTENT_BUFFER_SIZE:
                    if shortfall_length < 0:
                        shortfall_length = 0
                    split_point = self.find_split_point(shortfall_length, content)
                    written_length += split_point
                    chunk_f.write(content[:split_point])
                    logger.debug('done {} writing, file length {}'.format(chunk_file_path, written_length))
                    chunk_f.close()
                    pid = self.process_file(chunk_file_path, chunk_index, pid)

                    chunk_index += 1
                    chunk_file_path = FS_NAME_TEMPLATE.format(self.tmp_dir, chunk_index)
                    chunk_f = open(chunk_file_path, 'wb')
                    chunk_f.write(content[split_point:])
                    written_length = len(content) - split_point

                else:
                    written_length += len(content)
                    chunk_f.write(content)

                if eof:
                    # eof chunk writing
                    written_length += len(content)
                    chunk_f.write(content)
                    logger.debug('done eof chunk - {} writing, file length {}'.format(chunk_file_path, written_length))
                    chunk_f.close()
                    self.process_file(chunk_file_path, chunk_index, pid)
                    self.join_subprocess(pid)
                    break

        finally:
            shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def run_paired(self):
        try:
            chunk_index = [0, 0]
            written_length = [0, 0]
            chunk_file_path = [FS_NAME_PAIRED_TEMPLATE.format(self.tmp_dir, chunk_index[0], 'r1'),
                               FS_NAME_PAIRED_TEMPLATE.format(self.tmp_dir, chunk_index[1], 'r2')]
            out_handle = [open(chunk_file_path[0], 'wb'), open(chunk_file_path[1], 'wb')]
            pid = []
            processed_index = 0
            eof_flag = [False, False]

            while True:
                eof_flag[0], file_length, last_read_name = self.read_impl(self.source_handle,
                                                                          out_handle,
                                                                          chunk_file_path,
                                                                          written_length,
                                                                          chunk_index,
                                                                          0,
                                                                          self.fastq_txt_chunk_size,
                                                                          None)
                logger.debug('read file #1, eof {}, length {}'.format(eof_flag[0], file_length))

                eof_flag[1], _, _ = self.read_impl(self.source_handle,
                                                   out_handle,
                                                   chunk_file_path,
                                                   written_length,
                                                   chunk_index,
                                                   1,
                                                   file_length,
                                                   last_read_name)
                logger.debug('read file #2, eof {}'.format(eof_flag[1]))

                pid, processed_index = self.process_paired_file(processed_index, chunk_index, pid)
                if eof_flag[0] and eof_flag[1] and processed_index == chunk_index[0]:
                    pid.append(self.process_paired_file_impl(chunk_index[0]))
                    self.join_subprocess(pid)
                    ## TODO: skipping last two files merging
                    # self.merge_last_paired_file(chunk_index[0])
                    break
        finally:
            shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def read_impl(self, input_handle, out_f, chunk_file_path, written_length, chunk_index, pair_idx, file_length,
                  pair1_last_read_name):
        if pair_idx == 0:
            read_str = 'r1'
        else:
            read_str = 'r2'
        while True:
            eof, content = input_handle[pair_idx].read()
            shortfall_length = file_length - written_length[pair_idx]

            if pair_idx == 0 and len(content) > shortfall_length + CONTENT_BUFFER_SIZE:
                logger.debug('split pos identification for handle #{}, len(content) {} shortfall_length {}'.format(
                    pair_idx, len(content), shortfall_length))
                if shortfall_length < 0:
                    shortfall_length = 0
                split_point, last_read_name = self.find_next_read(content, shortfall_length, is_forward=True)
                logger.debug('write to pair_idx == 0, with split_point {}'.format(split_point))
                file_length = self.split_file_handling(content,
                                                       out_f,
                                                       chunk_file_path,
                                                       written_length,
                                                       chunk_index,
                                                       pair_idx,
                                                       read_str,
                                                       split_point)
                logger.debug('written file length {} last read name {}'.format(file_length, last_read_name))
                return False, file_length, last_read_name

            elif pair_idx == 1 and shortfall_length < PAIRED_TWO_SEARCH_SCOPE:
                eof2, content2 = input_handle[pair_idx].read(2 * PAIRED_TWO_SEARCH_SCOPE)

                content += content2
                logger.debug('eof --{}  content length --{} short_fall length {}'.format(eof2, len(content),
                                                                                         shortfall_length))

                # eof handling
                if len(content) <= shortfall_length or pair1_last_read_name is None:
                    written_length[pair_idx] += len(content)
                    out_f[pair_idx].write(content)
                    return True, written_length[pair_idx], None

                split_point = self.find_pair2_split_pos(content, shortfall_length, pair1_last_read_name)

                if split_point == -1:
                    raise DatasetsStorageError('read1 and read2 are not properly paired')

                logger.debug('write to pair_idx == 1, with split_point {}'.format(split_point))
                file_length = self.split_file_handling(content,
                                                       out_f,
                                                       chunk_file_path,
                                                       written_length,
                                                       chunk_index,
                                                       pair_idx,
                                                       read_str,
                                                       split_point)
                logger.debug('written file length {}'.format(file_length))

                # /n check;
                return False, file_length, None

            # general handling
            else:
                written_length[pair_idx] += len(content)
                out_f[pair_idx].write(content)

            if eof:
                logger.debug(
                    'done eof chunk - {} writing, file length {}'.format(chunk_file_path, written_length[pair_idx]))
                out_f[pair_idx].close()
                return True, written_length[pair_idx], None

    def find_next_read(self, content, split_position, is_forward=True):
        read_name = None
        head_start = None

        if is_forward:
            head_start = self.find_split_point(split_position, content)
            idx = head_start
            while idx < len(content):
                if content[idx] == ord('\n'):
                    read_name = str(content[head_start:idx], 'utf-8')
                    break
                idx += 1

        else:
            idx = split_position
            found_line_3 = False
            head_end = None
            while idx > 0:
                if content[idx] == ord('+') and content[idx - 1] == ord('\n'):
                    found_line_3 = True
                if found_line_3 and content[idx - 1] == ord('\n'):
                    if content[idx] == ord('@'):
                        head_start = idx
                        read_name = str(content[head_start:head_end], 'utf-8')
                        break
                    head_end = idx - 1
                idx -= 1

        if read_name is None:
            return -1, None
        return head_start, read_name

    def find_pair2_split_pos(self, content, pos, pair1_last_read_name):
        logger.debug('pair1 coordinate str -- {}'.format(pair1_last_read_name))
        pair1_read_coord_str = re.split('[/ ]', pair1_last_read_name)[0]
        ## read1 : @SRR7782669.1.1 1 length=151
        ## read2 : @SRR7782669.1.2 1 length=151
        if pair1_read_coord_str[-2:-1] == ".":
            pair1_read_coord_str = pair1_read_coord_str[:-1]
        logger.debug('pair1 coordinate str -- {}'.format(pair1_read_coord_str))
        needle = bytearray(pair1_read_coord_str, 'utf-8')

        # pos corresponding to @, start searching from \n
        find_str_pos = pos - 1
        anchor_pos, read_name = self.find_next_read(content, find_str_pos, is_forward=True)
        logger.debug('anchor pos -- {}, read name {}'.format(anchor_pos, read_name))
        if anchor_pos == -1:
            return -1

        br = [anchor_pos - PAIR_TWO_SEARCH_BUF_SIZE - len(pair1_last_read_name), anchor_pos + len(pair1_last_read_name)]
        fr = [anchor_pos - len(pair1_last_read_name), anchor_pos + PAIR_TWO_SEARCH_BUF_SIZE + len(pair1_last_read_name)]

        logger.debug('total search space {} - {} - {}'.format(0, anchor_pos, len(content)))
        while br[0] > 0 or fr[1] < len(content):

            # forward search
            if fr[1] < len(content):
                logger.debug('forward search from {} - {}'.format(fr[0], fr[1]))
                pos = content.find(needle, fr[0], fr[1])
                if pos == -1:
                    fr[0] += PAIR_TWO_SEARCH_BUF_SIZE
                    fr[1] += PAIR_TWO_SEARCH_BUF_SIZE
                else:
                    logger.debug('split pos forward found -- {}'.format(pos))
                    return pos

            # backward search
            if br[0] > 0:
                logger.debug('backward search from {} - {}'.format(br[0], br[1]))
                pos = content.find(needle, br[0], br[1])
                if pos == -1:
                    br[0] -= PAIR_TWO_SEARCH_BUF_SIZE
                    br[1] -= PAIR_TWO_SEARCH_BUF_SIZE
                else:
                    logger.debug('split pos backward found -- {}'.format(pos))
                    return pos
        return -1

    def split_file_handling(self, content, out_f, chunk_file_path, written_length, chunk_index, pair_idx,
                            read_str, split_point):
        file_length = written_length[pair_idx] + split_point
        out_f[pair_idx].write(content[:split_point])
        logger.debug('done {} writing, file length {}'.format
                     (chunk_file_path[pair_idx], file_length))
        out_f[pair_idx].close()

        chunk_index[pair_idx] += 1
        chunk_file_path[pair_idx] = FS_NAME_PAIRED_TEMPLATE.format(self.tmp_dir, chunk_index[pair_idx], read_str)
        out_f[pair_idx] = open(chunk_file_path[pair_idx], 'wb')
        out_f[pair_idx].write(content[split_point:])
        written_length[pair_idx] = len(content) - split_point
        logger.debug('add new file {}, file length {}'.format
                     (chunk_file_path[pair_idx], written_length[pair_idx]))
        return file_length

    def process_paired_file(self, processed_index, chunk_index, pid):
        cur_written_index = min(chunk_index)
        for index in range(processed_index, cur_written_index):
            pid.append(self.process_paired_file_impl(index))
        if len(pid) > SUBPROCESS_NUM:
            return self.join_subprocess(pid), cur_written_index
        return pid, cur_written_index

    def process_paired_file_impl(self, index):
        file_r1 = FS_NAME_PAIRED_TEMPLATE.format(self.tmp_dir, index, 'r1')
        file_r2 = FS_NAME_PAIRED_TEMPLATE.format(self.tmp_dir, index, 'r2')
        file_cat = FS_NAME_TEMPLATE.format(self.tmp_dir, index)
        file_hdfs = HDFS_NAME_TEMPLATE.format(self.result_dest, str(index).zfill(5))
        cmd = ''
        if self.write_header:
            hdr_parts = '##parts=<ID={}>'.format(str(index).zfill(5))
            hdr_read1 = '##reads=<ID=R1, LN=0, SZ={}>'.format(os.stat(file_r1).st_size)
            hdr_read2 = '##reads=<ID=R2, LN=0, SZ={}>'.format(os.stat(file_r2).st_size)
            cmd += 'bash -c "echo -e \'{0}\n{1}\n{2}\'" > {3} && '.format(hdr_parts, hdr_read1, hdr_read2, file_cat)
        cmd += 'cat {0} {1} >> {2} && {3} -c -t hadoop-snappy {2} | {4} fs -put - {5}'.format(
            file_r1, file_r2, file_cat, SNZIP, HADOOP, file_hdfs)
        return subprocess.Popen(cmd, shell=True), cmd, [file_r1, file_r2, file_cat]

    def merge_last_paired_file(self, index):
        pfn = HDFS_NAME_TEMPLATE.format(self.result_dest, str(index - 1).zfill(5))
        lfn = HDFS_NAME_TEMPLATE.format(self.result_dest, str(index).zfill(5))
        if dfs_client.status(lfn)['length'] >= dfs_client.status(pfn)['length'] / 2:
            return

        def _split(src, prefix, wd):
            r1sz = 0
            r2sz = 0
            with tempfile.NamedTemporaryFile(mode='w', dir=wd) as fout:
                with open(src, 'r') as fin:
                    for line in fin:
                        m = re.match(r'^##parts=', line)
                        if m is not None:
                            pass
                        else:
                            m = re.match(r'^##reads=<ID=R1, LN=0, SZ=([0-9]+)>\n', line)
                            if m is not None:
                                r1sz = int(m.group(1))
                            else:
                                m = re.match(r'^##reads=<ID=R2, LN=0, SZ=([0-9]+)>\n', line)
                                if m is not None:
                                    r2sz = int(m.group(1))
                                else:
                                    fout.write(line)

                fout.flush()
                if r1sz != 0:
                    with open(os.path.join(wd, prefix + '00'), 'wb') as r1:
                        subprocess.call(['head', '-c', str(r1sz), fout.name], stdout=r1, stderr=subprocess.PIPE)
                    with open(os.path.join(wd, prefix + '01'), 'wb') as r2:
                        subprocess.call(['tail', '-c', str(r2sz), fout.name], stdout=r2, stderr=subprocess.PIPE)
                else:
                    subprocess.run(['split', '-d', '-n', '2', fout.name, prefix], cwd=wd)

            os.unlink(src)
            return os.path.join(wd, prefix + '00'), os.path.join(wd, prefix + '01'), r1sz, r2sz

        pfp = dfs_client.download(pfn, self.tmp_dir)
        subprocess.call([SNZIP, '-d', pfp])
        pfp_r1, pfp_r2, pfp_s1, pfp_s2 = _split(os.path.splitext(pfp)[0], str(index - 1), self.tmp_dir)

        lfp = dfs_client.download(lfn, self.tmp_dir)
        subprocess.call([SNZIP, '-d', lfp])
        lfp_r1, lfp_r2, lfp_s1, lfp_s2 = _split(os.path.splitext(lfp)[0], str(index), self.tmp_dir)

        subprocess.call('cat {} >> {}'.format(lfp_r1, pfp_r1), shell=True)
        subprocess.call('cat {} >> {}'.format(lfp_r2, pfp_r2), shell=True)
        os.unlink(lfp_r1)
        os.unlink(lfp_r2)

        pfcat = FS_NAME_TEMPLATE.format(self.tmp_dir, index - 1)
        cmd = ''
        if self.write_header:
            hdr_parts = '##parts=<ID={}>'.format(str(index - 1).zfill(5))
            hdr_read1 = '##reads=<ID=R1, LN=0, SZ={}>'.format(pfp_s1 + lfp_s1)
            hdr_read2 = '##reads=<ID=R2, LN=0, SZ={}>'.format(pfp_s2 + lfp_s2)
            cmd += 'bash -c "echo -e \'{0}\n{1}\n{2}\'" > {3} && '.format(hdr_parts, hdr_read1, hdr_read2, pfcat)

        cmd += 'cat {0} {1} >> {2} && {3} -c -t hadoop-snappy {2} | {4} fs -put -f - {5}'.format(pfp_r1,
                                                                                                 pfp_r2,
                                                                                                 pfcat,
                                                                                                 SNZIP,
                                                                                                 HADOOP,
                                                                                                 pfn)
        dfs_client.delete(pfn)
        dfs_client.delete(lfn)
        subprocess.run(cmd, shell=True)
        os.unlink(pfcat)
        os.unlink(pfp_r1)
        os.unlink(pfp_r2)


def usage():
    print(
        "fq_upload.py -1 <Read 1 FASTQ file> -2 <Read 2 FASTQ file> -o <Output HDFS Folder>")
    print("Argument:")
    print("\t-h: Usage")
    print("\t-1: Input Read 1 FASTQ file")
    print("\t-2: Input Read 2 FASTQ file")
    print("\t-o: Output tsv list")
    print("Usage:")
    print("\t./fq_upload.py -1 ~/NA19240/SRR7782669_same_r1.fq.gz -2 ~/NA19240/SRR7782669_same_r2.fq.gz "
          "-o /NA12878/chunk.fq")

    return


def fq_upload(i1file, i2file, ofolder):
    uploader = FastqChunkwiseChopNUploader([i1file, i2file], ofolder)
    uploader.run()
    return


def main(argv):
    i1file = ""
    i2file = ""
    ofolder = ""

    try:
        opts, args = getopt.getopt(argv, "h1:2:o:")
    except getopt.GetoptError:
        usage()
        sys.exit(1)

    for opt, arg in opts:
        if opt == '-h':
            usage()
            sys.exit()
        elif opt == "-1":
            i1file = arg
        elif opt == "-2":
            i2file = arg
        elif opt == "-o":
            ofolder = arg

    # error handling for input parameters
    if i1file == "":
        print("Error: '-1' is required")
        usage()
        sys.exit(2)
    elif not os.path.isfile(i1file):
        print("Error: input file(%s) is not existed" % i1file)
        usage()
        sys.exit(3)
    elif i2file == "":
        print("Error: '-2' is required")
        usage()
        sys.exit(4)
    elif not os.path.isfile(i2file):
        print("Error: input file(%s) is not existed" % i2file)
        usage()
        sys.exit(5)
    elif ofolder == "":
        print("Error: '-o' is required")
        usage()
        sys.exit(4)

    # Main Function
    fq_upload(i1file, i2file, ofolder)

    return


if __name__ == '__main__':
    main(sys.argv[1:])
