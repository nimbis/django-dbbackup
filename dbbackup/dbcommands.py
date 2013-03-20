"""
Process the Backup or Restore commands.
"""
import copy
import os
import re
import shlex
from datetime import datetime
from shutil import copyfileobj
from subprocess import Popen

from django.conf import settings
from django.core.management.base import CommandError


READ_FILE = '<READ_FILE>'
WRITE_FILE = '<WRITE_FILE>'
DATE_FORMAT = getattr(settings, 'DBBACKUP_DATE_FORMAT', '%Y-%m-%d-%H%M%S')
SERVER_NAME = getattr(settings, 'DBBACKUP_SERVER_NAME', '')
FILENAME_TEMPLATE = getattr(settings, 'DBBACKUP_FILENAME_TEMPLATE', '{databasename}-{servername}-{datetime}.{extension}')


##################################
#  Base Engine Settings
##################################

class BaseEngineSettings:
    """Base settings for a database engine"""

    def __init__(self, database):
        self.database = database
        self.database_adminuser = self.database.get('ADMINUSER', self.database['USER'])
        self.database_user = self.database['USER']
        self.database_password = self.database['PASSWORD']
        self.database_name = self.database['NAME']
        self.database_host = self.database.get('HOST', '')
        self.database_port = str(self.database.get('PORT', ''))
        self.EXTENSION = self.get_extension()
        self.BACKUP_COMMANDS = self.get_backup_commands()
        self.RESTORE_COMMANDS = self.get_restore_commands()

    def get_extension(self):
        raise NotImplementedError("Subclasses must implement get_extensions")

    def get_backup_commands(self):
        raise NotImplementedError("Subclasses must implement get_backup_commands")

    def get_restore_commands(self):
        raise NotImplementedError("Subclasses must implement get_restore_commands")


##################################
#  MySQL Settings
##################################

class MySQLSettings(BaseEngineSettings):
    """Settings for the MySQL database engine"""

    def get_extension(self):
        return getattr(settings, 'DBBACKUP_MYSQL_EXTENSION', 'mysql')

    def get_backup_commands(self):
        backup_commands = getattr(settings, 'DBBACKUP_MYSQL_BACKUP_COMMANDS', None)
        if not backup_commands:
            command = 'mysqldump --user={adminuser} --password={password}'
            if self.database_host:
                command = '%s --host={host}' % command
            if self.database_port:
                command = '%s --port={port}' % command
            command = '%s {databasename} >' % command
            backup_commands = [shlex.split(command)]
        return backup_commands

    def get_restore_commands(self):
        restore_commands = getattr(settings, 'DBBACKUP_MYSQL_RESTORE_COMMANDS', None)
        if not restore_commands:
            command = 'mysql --user={adminuser} --password={password}'
            if self.database_host:
                command = '%s --host={host}' % command
            if self.database_port:
                command = '%s --port={port}' % command
            command = '%s {databasename} <' % command
            restore_commands = [shlex.split(command)]
        return restore_commands


##################################
#  PostgreSQL Settings
##################################

class PostgreSQLSettings(BaseEngineSettings):
    """Settings for the PostgreSQL database engine"""

    def get_extension(self):
        return getattr(settings, 'DBBACKUP_POSTGRESQL_EXTENSION', 'psql')

    def get_backup_commands(self):
        backup_commands = getattr(settings, 'DBBACKUP_POSTGRESQL_BACKUP_COMMANDS', None)
        if not backup_commands:
            command = 'pg_dump --username={adminuser}'
            if self.database_host:
                command = '%s --host={host}' % command
            if self.database_port:
                command = '%s --port={port}' % command
            command = '%s {databasename} >' % command
            backup_commands = [shlex.split(command)]
        return backup_commands

    def get_restore_commands(self):
        restore_commands = getattr(settings, 'DBBACKUP_POSTGRESQL_RESTORE_COMMANDS', None)
        if not restore_commands:
            restore_commands = [
                shlex.split(self.dropdb_command()),
                shlex.split(self.createdb_command()),
                shlex.split(self.import_command())
            ]
        return restore_commands

    def dropdb_command(self):
        """Constructs the PostgreSQL dropdb command"""
        command = 'dropdb --username={adminuser}'
        if self.database_host:
            command = '%s --host={host}' % command
        if self.database_port:
            command = '%s --port={port}' % command
        return '%s {databasename}' % command

    def createdb_command(self):
        """Constructs the PostgreSQL createdb command"""
        command = 'createdb --username={adminuser} --owner={username}'
        if self.database_host:
            command = '%s --host={host}' % command
        if self.database_port:
            command = '%s --port={port}' % command
        return '%s {databasename}' % command

    def import_command(self):
        """Constructs the PostgreSQL db import command"""
        command = 'psql --username={adminuser}'
        if self.database_host:
            command = '%s --host={host}' % command
        if self.database_port:
            command = '%s --port={port}' % command
        return '%s --single-transaction {databasename} <' % command


##################################
#  Sqlite Settings
##################################

class SQLiteSettings(BaseEngineSettings):
    """Settings for the SQLite database engine"""

    def get_extension(self):
        return getattr(settings, 'DBBACKUP_SQLITE_EXTENSION', 'sqlite')

    def get_backup_commands(self):
        return getattr(settings, 'DBBACKUP_SQLITE_BACKUP_COMMANDS', [
            [READ_FILE, '{databasename}'],
        ])

    def get_restore_commands(self):
        return getattr(settings, 'DBBACKUP_SQLITE_RESTORE_COMMANDS', [
            [WRITE_FILE, '{databasename}'],
        ])


##################################
#  DBCommands Class
##################################

class DBCommands:
    """ Process the Backup or Restore commands. """

    def __init__(self, database):
        self.database = database
        self.engine = self.database['ENGINE'].split('.')[-1]
        self.settings = self._get_settings()

    def _get_settings(self):
        """ Returns the proper settings dictionary. """
        if self.engine == 'mysql':
            return MySQLSettings(self.database)
        elif self.engine in ('postgresql_psycopg2', 'postgis'):
            return PostgreSQLSettings(self.database)
        elif self.engine == 'sqlite3':
            return SQLiteSettings(self.database)

    def _clean_passwd(self, instr):
        return instr.replace(self.database['PASSWORD'], '******')

    def filename(self, servername=None, wildcard=None):
        """ Create a new backup filename. """
        params = {
            'databasename': self.database['NAME'].replace("/", "_"),
            'servername': servername or SERVER_NAME,
            'timestamp': datetime.now(),
            'extension': self.settings.EXTENSION,
            'wildcard': wildcard,
        }
        if callable(FILENAME_TEMPLATE):
            filename = FILENAME_TEMPLATE(**params)
        else:
            params['datetime'] = wildcard or params['timestamp'].strftime(DATE_FORMAT)
            # if Python 2.6 is okay, this line can replace the 4 below it:
            # filename = FILENAME_TEMPLATE.format(**params)
            filename = FILENAME_TEMPLATE
            for key, value in params.iteritems():
                filename = filename.replace('{%s}' % key, unicode(value))
            filename = filename.replace('--', '-')
        return filename

    def filename_match(self, servername=None, wildcard='*'):
        """ Return the prefix for backup filenames. """
        return self.filename(servername, wildcard)

    def filter_filepaths(self, filepaths, servername=None):
        """ Returns a list of backups file paths from the dropbox entries. """
        regex = self.filename_match(servername, '.*?')
        return filter(lambda path: re.search(regex, path), filepaths)

    def translate_command(self, command):
        """ Translate the specified command. """
        command = copy.copy(command)
        for i in range(len(command)):
            command[i] = command[i].replace('{adminuser}', self.database.get('ADMINUSER', self.database['USER']))
            command[i] = command[i].replace('{username}', self.database['USER'])
            command[i] = command[i].replace('{password}', self.database['PASSWORD'])
            command[i] = command[i].replace('{databasename}', self.database['NAME'])
            command[i] = command[i].replace('{host}', self.database['HOST'])
            command[i] = command[i].replace('{port}', str(self.database['PORT']))
        return command

    def run_backup_commands(self, stdout):
        """ Translate and run the backup commands. """
        return self.run_commands(self.settings.BACKUP_COMMANDS, stdout=stdout)

    def run_restore_commands(self, stdin):
        """ Translate and run the backup commands. """
        stdin.seek(0)
        return self.run_commands(self.settings.RESTORE_COMMANDS, stdin=stdin)

    def run_commands(self, commands, stdin=None, stdout=None):
        """ Translate and run the specified commands. """
        for command in commands:
            command = self.translate_command(command)
            if (command[0] == READ_FILE):
                self.read_file(command[1], stdout)
            elif (command[0] == WRITE_FILE):
                self.write_file(command[1], stdin)
            else:
                self.run_command(command, stdin, stdout)

    def run_command(self, command, stdin=None, stdout=None):
        """ Run the specified command. """
        devnull = open(os.devnull, 'w')
        pstdin = stdin if command[-1] == '<' else None
        pstdout = stdout if command[-1] == '>' else devnull
        command = filter(lambda arg: arg not in ['<', '>'], command)
        print self._clean_passwd("  Running: %s" % ' '.join(command))
        process = Popen(command, stdin=pstdin, stdout=pstdout)
        process.wait()
        devnull.close()
        if process.poll():
            raise CommandError("Error running: %s" % command)

    def read_file(self, filepath, stdout):
        """ Read the specified file to stdout. """
        print "  Reading: %s" % filepath
        with open(filepath, "rb") as f:
            copyfileobj(f, stdout)

    def write_file(self, filepath, stdin):
        """ Write the specified file from stdin. """
        print "  Writing: %s" % filepath
        with open(filepath, 'wb') as f:
            copyfileobj(stdin, f)
