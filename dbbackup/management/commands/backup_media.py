import os
from datetime import datetime
import tarfile
import tempfile
from optparse import make_option

from django.conf import settings
from django.core.management.base import BaseCommand
from django.core.management.base import CommandError

from ... import utils
from ...storage.base import BaseStorage
from ...storage.base import StorageError


DATE_FORMAT = getattr(settings, 'DBBACKUP_DATE_FORMAT', '%Y-%m-%d-%H%M%S')


class Command(BaseCommand):
    help = "backup_media [--encrypt]"
    option_list = BaseCommand.option_list + (
        make_option("-e", "--encrypt", help="Encrypt the backup files", action="store_true", default=False),
    )

    @utils.email_uncaught_exception
    def handle(self, *args, **options):
        try:
            self.encrypt = options.get('encrypt')
            self.storage = BaseStorage.storage_factory()

            self.backup_mediafiles()
        except StorageError, err:
            raise CommandError(err)

    def backup_mediafiles(self):
        print "Backing up media files"
        output_file = self.create_backup_file(self.get_source_dir(), self.get_backup_basename())

        if self.encrypt:
            encrypted_file = utils.encrypt_file(output_file)
            output_file = encrypted_file

        print "  Backup tempfile created: %s (%s)" % (output_file.name, utils.handle_size(output_file))
        print "  Writing file to %s: %s" % (self.storage.name, self.storage.backup_dir())
        self.storage.write_file(output_file)

    def get_backup_basename(self):
        database_name = settings.DATABASES['default']['NAME']
        timestamp = datetime.now().strftime(DATE_FORMAT)

        return '%s-%s.media.tar.gz' % (database_name, timestamp)

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

                    outputfile = tempfile.SpooledTemporaryFile(max_size=10 * 1024 * 1024)
                    outputfile.name = backup_basename

                    f = open(backup_filename)
                    try:
                        outputfile.write(f.read())
                    finally:
                        f.close()
            finally:
                if os.path.exists(backup_filename):
                    os.remove(backup_filename)
        finally:
            os.rmdir(temp_dir)

        return outputfile

    def get_source_dir(self):
        return getattr(settings, 'DBBACKUP_MEDIA_PATH') or settings.MEDIA_ROOT