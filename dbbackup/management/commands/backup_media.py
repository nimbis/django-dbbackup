import os
from datetime import datetime
import tarfile
import tempfile
from optparse import make_option
import re

from django.conf import settings
from django.core.management.base import BaseCommand
from django.core.management.base import CommandError

from ... import utils
from ...storage.base import BaseStorage
from ...storage.base import StorageError


DATE_FORMAT = getattr(settings, 'DBBACKUP_DATE_FORMAT', '%Y-%m-%d-%H%M%S')
CLEANUP_KEEP = getattr(settings, 'DBBACKUP_CLEANUP_KEEP', 10)


class Command(BaseCommand):
    help = "backup_media [--encrypt]"
    option_list = BaseCommand.option_list + (
        make_option("-c", "--clean", help="Clean up old backup files", action="store_true", default=False),
        make_option("-s", "--servername", help="Specify server name to include in backup filename"),
        make_option("-e", "--encrypt", help="Encrypt the backup files", action="store_true", default=False),
    )

    @utils.email_uncaught_exception
    def handle(self, *args, **options):
        try:
            self.servername = options.get('servername')
            self.storage = BaseStorage.storage_factory()

            self.backup_mediafiles(options.get('encrypt'))

            if options.get('clean'):
                self.cleanup_old_backups()

        except StorageError, err:
            raise CommandError(err)

    def backup_mediafiles(self, encrypt):
        print "Backing up media files"
        output_file = self.create_backup_file(self.get_source_dir(), self.get_backup_basename())

        if encrypt:
            encrypted_file = utils.encrypt_file(output_file)
            output_file = encrypted_file

        print "  Backup tempfile created: %s (%s)" % (output_file.name, utils.handle_size(output_file))
        print "  Writing file to %s: %s" % (self.storage.name, self.storage.backup_dir())
        self.storage.write_file(output_file)

    def get_backup_basename(self):
        # todo: use DBBACKUP_FILENAME_TEMPLATE
        server_name = self.get_servername()
        if server_name:
            server_name = '-%s' % server_name

        return '%s%s-%s.media.tar.gz' % (
            self.get_databasename(),
            server_name,
            datetime.now().strftime(DATE_FORMAT)
        )

    def get_databasename(self):
        return settings.DATABASES['default']['NAME']

    def create_backup_file(self, source_dir, backup_basename):
        temp_dir = tempfile.mkdtemp()
        try:
            backup_filename = os.path.join(temp_dir, backup_basename)
            try:
                tar_file = tarfile.open(backup_filename, 'w|gz')
                try:
                    tar_file.add(source_dir)
                finally:
                    tar_file.close()

                return utils.create_spooled_temporary_file(backup_filename, backup_basename)
            finally:
                if os.path.exists(backup_filename):
                    os.remove(backup_filename)
        finally:
            os.rmdir(temp_dir)

    def get_source_dir(self):
        return getattr(settings, 'DBBACKUP_MEDIA_PATH') or settings.MEDIA_ROOT

    def cleanup_old_backups(self):
        """ Cleanup old backups, keeping the number of backups specified by
        DBBACKUP_CLEANUP_KEEP and any backups that occur on first of the month.
        """
        print "Cleaning Old Backups for media files"

        file_list = self.get_backup_file_list()

        for backup_date, filename in file_list[0:-CLEANUP_KEEP]:
            if int(backup_date.strftime("%d")) != 1:
                print "  Deleting: %s" % filename
                self.storage.delete_file(filename)

    def get_backup_file_list(self):
        """ Return a list of backup files including the backup date. The result is a list of tuples (datetime, filename).
            The list is sorted by date.
        """
        server_name = self.get_servername()
        if server_name:
            server_name = '-%s' % server_name

        media_re = re.compile(r'%s%s-(.*)\.media\.tar\.gz' % (self.get_databasename(), server_name))

        def is_media_backup(filename):
            return media_re.search(filename)

        def get_datetime_from_filename(filename):
            datestr = re.findall(media_re, filename)[0]
            return datetime.strptime(datestr, DATE_FORMAT)

        file_list = [
            (get_datetime_from_filename(f), f)
            for f in self.storage.list_directory()
            if is_media_backup(f)
        ]
        return sorted(file_list, key=lambda v: v[0])

    def get_servername(self):
        return self.servername or getattr(settings, 'DBBACKUP_SERVER_NAME', '')