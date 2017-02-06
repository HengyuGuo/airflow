#!/usr/bin/python2.7
import uuid
import sys
from airflow import models, settings
from airflow.contrib.auth.backends.password_auth import PasswordUser

if len(sys.argv) < 3:
    print 'usage: add_user.py unixname email'
    exit(-1)

user = PasswordUser(models.User())
username = sys.argv[1]
user.username = username
user.email = sys.argv[2]
password = uuid.uuid4().bytes.encode("base64")[0:12]
user.password = password
session = settings.Session()
session.add(user)
session.commit()
session.close()

print 'user {user} has password {pw}'.format(user=username, pw=password)
