"""
Save backup files to Dropbox.
"""
import re
import datetime
import tempfile
from optparse import make_option
import gzip

from django.conf import settings
from django.core.management.base import BaseCommand
from django.core.management.base import CommandError
from django.core.management.base import LabelCommand

from ... import utils
from ...dbcommands import DBCommands
from ...dbcommands import DATE_FORMAT
from ...storage.base import BaseStorage
from ...storage.base import StorageError


DATABASE_KEYS = getattr(settings, 'DBBACKUP_DATABASES', settings.DATABASES.keys())
CLEANUP_KEEP = getattr(settings, 'DBBACKUP_CLEANUP_KEEP', 10)


class Command(LabelCommand):
    help = "dbbackup [-c] [-d <dbname>] [-s <servername>] [--compress] [--encrypt]"
    option_list = BaseCommand.option_list + (
        make_option("-c", "--clean", help="Clean up old backup files", action="store_true", default=False),
        make_option("-d", "--database", help="Database to backup (default: everything)"),
        make_option("-s", "--servername", help="Specifiy server name to include in backup filename"),
        make_option("-z", "--compress", help="Compress the backup files", action="store_true", default=False),
        make_option("-e", "--encrypt", help="Encrypt the backup files", action="store_true", default=False),
    )

    @utils.email_uncaught_exception
    def handle(self, **options):
        """ Django command handler. """
        try:
            self.clean = options.get('clean')
            self.database = options.get('database')
            self.servername = options.get('servername')
            self.compress = options.get('compress')
            self.encrypt = options.get('encrypt')
            self.storage = BaseStorage.storage_factory()
            database_keys = (self.database,) if self.database else DATABASE_KEYS
            for database_key in database_keys:
                database = settings.DATABASES[database_key]
                self.dbcommands = DBCommands(database)
                self.save_new_backup(database)
                self.cleanup_old_backups(database)
        except StorageError, err:
            raise CommandError(err)

    def save_new_backup(self, database):
        """ Save a new backup file. """
        print "Backing Up Database: %s" % database['NAME']
        output_file = tempfile.SpooledTemporaryFile(max_size=10 * 1024 * 1024)
        output_file.name = self.dbcommands.filename(self.servername)
        self.dbcommands.run_backup_commands(output_file)

        if self.compress:
            compressed_file = self.compress_file(output_file)
            output_file.close()
            output_file = compressed_file

        if self.encrypt:
            encrypted_file = utils.encrypt_file(output_file)
            output_file = encrypted_file

        print "  Backup tempfile created: %s (%s)" % (output_file.name, utils.handle_size(output_file))
        print "  Writing file to %s: %s" % (self.storage.name, self.storage.backup_dir())
        self.storage.write_file(output_file)

    def cleanup_old_backups(self, database):
        """ Cleanup old backups, keeping the number of backups specified by
        DBBACKUP_CLEANUP_KEEP and any backups that occur on first of the month.
        """
        if self.clean:
            print "Cleaning Old Backups for: %s" % database['NAME']
            filepaths = self.storage.list_directory()
            filepaths = self.dbcommands.filter_filepaths(filepaths)
            for filepath in sorted(filepaths[0:-CLEANUP_KEEP]):
                regex = self.dbcommands.filename_match(self.servername, '(.*?)')
                datestr = re.findall(regex, filepath)[0]
                dateTime = datetime.datetime.strptime(datestr, DATE_FORMAT)
                if int(dateTime.strftime("%d")) != 1:
                    print "  Deleting: %s" % filepath
                    self.storage.delete_file(filepath)

    def compress_file(self, input_file):
        """ Compress this file using gzip.
        The input and the output are filelike objects.
        """
        outputfile = tempfile.SpooledTemporaryFile(max_size=10 * 1024 * 1024)
        outputfile.name = input_file.name + '.gz'

        zipfile = gzip.GzipFile(fileobj=outputfile, mode="wb")
        try:
            input_file.seek(0)
            zipfile.write(input_file.read())
        finally:
            zipfile.close()

        return outputfile