"""
S3 Storage object.
"""
import os
import tempfile
from cStringIO import StringIO

import boto
from boto.s3.key import Key
from django.conf import settings

from .base import BaseStorage, StorageError


################################
#  S3 Storage Object
################################

class Storage(BaseStorage):
    """ S3 API Storage. """
    S3_BUCKET = getattr(settings, 'DBBACKUP_S3_BUCKET', None)
    S3_ACCESS_KEY = getattr(settings, 'DBBACKUP_S3_ACCESS_KEY', None)
    S3_SECRET_KEY = getattr(settings, 'DBBACKUP_S3_SECRET_KEY', None)
    S3_DOMAIN = getattr(settings, 'DBBACKUP_S3_DOMAIN', 'https://s3.amazonaws.com/')
    S3_DIRECTORY = getattr(settings, 'DBBACKUP_S3_DIRECTORY', "django-dbbackups/")
    S3_DIRECTORY = '%s/' % S3_DIRECTORY.strip('/')

    def __init__(self, server_name=None):
        self._check_filesystem_errors()
        self.name = 'AmazonS3'
        self.conn = boto.connect_s3(self.S3_ACCESS_KEY, self.S3_SECRET_KEY)
        self.bucket = self.conn.get_bucket(self.S3_BUCKET)
        BaseStorage.__init__(self)

    def _check_filesystem_errors(self):
        """ Check we have all the required settings defined. """
        if not self.S3_BUCKET:
            raise StorageError('Filesystem storage requires DBBACKUP_S3_BUCKET to be defined in settings.')
        if not self.S3_ACCESS_KEY:
            raise StorageError('Filesystem storage requires DBBACKUP_S3_ACCESS_KEY to be defined in settings.')
        if not self.S3_SECRET_KEY:
            raise StorageError('Filesystem storage requires DBBACKUP_S3_SECRET_KEY to be defined in settings.')

    ###################################
    #  DBBackup Storage Methods
    ###################################

    @property
    def bucket(self):
        return self.bucket

    def backup_dir(self):
        return self.S3_DIRECTORY

    def delete_file(self, filepath):
        """ Delete the specified filepath. """
        self.bucket.delete_key(filepath)

    def list_directory(self):
        """ List all stored backups for the specified. """
        return [k.name for k in
                self.bucket.get_all_keys(prefix=self.S3_DIRECTORY)]

    def write_file(self, filehandle):
        """ Write the specified file.
            Use multipart upload because normal upload maximum is 5 GB.
        """
        filepath = os.path.join(self.S3_DIRECTORY, filehandle.name)

        filehandle.seek(0)

        mp = self.bucket.initiate_multipart_upload(filepath)

        try:
            part_index = 1
            while True:
                buffer = filehandle.read(5 * 1024 * 1024)

                if not buffer:
                    break
                else:
                    string_file = StringIO(buffer)
                    try:
                        string_file.seek(0)
                        mp.upload_part_from_file(string_file, part_index)
                    finally:
                        string_file.close()

                    part_index += 1

            mp.complete_upload()
        except:
            mp.cancel_upload()
            raise

    def read_file(self, filepath):
        """ Read the specified file and return it's handle. """
        key = Key(self.bucket)
        key.key = filepath
        filehandle = tempfile.SpooledTemporaryFile(max_size=10 * 1024 * 1024)
        key.get_contents_to_file(filehandle)
        return filehandle
