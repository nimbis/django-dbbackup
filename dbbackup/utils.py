"""
Util functions for dropbox application.
"""
import sys
import os
import tempfile
from django.conf import settings
from django.core.mail import EmailMessage
from django.db import connection
from django.http import HttpRequest
from django.views.debug import ExceptionReporter
from functools import wraps

FAKE_HTTP_REQUEST = HttpRequest()
FAKE_HTTP_REQUEST.META['SERVER_NAME'] = ''
FAKE_HTTP_REQUEST.META['SERVER_PORT'] = ''
FAKE_HTTP_REQUEST.META['HTTP_HOST'] = 'django-dbbackup'

BYTES = (
    ('PB', 1125899906842624.0),
    ('TB', 1099511627776.0),
    ('GB', 1073741824.0),
    ('MB', 1048576.0),
    ('KB', 1024.0),
    ('B', 1.0)
)


###################################
#  Display Filesizes
###################################

def bytes_to_str(byteVal, decimals=1):
    """ Convert bytes to a human readable string. """
    for unit, byte in BYTES:
        if (byteVal >= byte):
            if (decimals == 0):
                return "%s %s" % (int(round(byteVal / byte, 0)), unit)
            else:
                return "%s %s" % (round(byteVal / byte, decimals), unit)
    return "%s B" % byteVal


def handle_size(filehandle):
    """ Given a filehandle return the filesize. """
    filehandle.seek(0, 2)
    return bytes_to_str(filehandle.tell())


###################################
#  Email Exception Decorator
###################################

def email_uncaught_exception(func):
    """ Email uncaught exceptions to the SERVER_EMAIL. """
    module = func.__module__

    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            func(*args, **kwargs)
        except:
            if getattr(settings, 'DBBACKUP_SEND_EMAIL', True):
                excType, excValue, traceback = sys.exc_info()
                reporter = ExceptionReporter(FAKE_HTTP_REQUEST, excType,
                                             excValue, traceback.tb_next)
                subject = "Cron: Uncaught exception running %s" % module
                body = reporter.get_traceback_html()
                msgFrom = settings.SERVER_EMAIL
                msgTo = [admin[1] for admin in settings.ADMINS]
                message = EmailMessage(subject, body, msgFrom, msgTo)
                message.content_subtype = 'html'
                message.send(fail_silently=True)
            raise
        finally:
            connection.close()
    return wrapper


def encrypt_file(input_file):
    """ Encrypt the file using gpg.
    The input and the output are filelike objects. Closes the input file.
    """
    import gnupg

    temp_dir = tempfile.mkdtemp()
    try:
        temp_filename = os.path.join(temp_dir, input_file.name + '.gpg')
        try:
            input_file.seek(0)

            g = gnupg.GPG()
            result = g.encrypt_file(input_file, output=temp_filename, recipients=settings.DBBACKUP_GPG_RECIPIENT)
            input_file.close()

            if not result:
                raise Exception('Encryption failed; status: %s' % result.status)

            outputfile = tempfile.SpooledTemporaryFile(max_size=10 * 1024 * 1024)
            outputfile.name = input_file.name + '.gpg'

            f = open(temp_filename)
            try:
                outputfile.write(f.read())
            finally:
                f.close()
        finally:
            if os.path.exists(temp_filename):
                os.remove(temp_filename)
    finally:
        os.rmdir(temp_dir)

    return outputfile