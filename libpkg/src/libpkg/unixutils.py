import os
import shutil
import smtplib
import tempfile

from email.message import EmailMessage


def make_tempdir(root_dir):
    try:
        tdir_name = tempfile.mkdtemp(dir=root_dir)
        os.chmod(tdir_name, 0o777)
        return tdir_name
    except Exception:
        return ""


def remove_tempdir(tdir_name):
    try:
        shutil.rmtree(tdir_name)
    except Exception:
        pass


def sendmail(to_list, from_addr, subject, body, **kwargs):
    if 'port' in kwargs:
        port = kwargs['port']
    else:
        port = 1025 if 'devel' in kwargs and kwargs['devel'] else 465

    host = kwargs['host'] if 'host' in kwargs else "localhost"
    s = smtplib.SMTP(host, port)
    msg = EmailMessage()
    msg['To'] = ", ".join(to_list)
    msg['From'] = from_addr
    msg['Subject'] = subject
    if 'bcc_list' in kwargs:
        msg['Bcc'] = ", ".join(kwargs['bcc_list'])

    msg.set_content(body)
    s.send_message(msg)
    s.quit()
