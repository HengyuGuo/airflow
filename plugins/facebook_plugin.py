import boto.sns
import imp
import sys

from airflow.plugins_manager import AirflowPlugin

from hooks.cached_db_api_hook import FBCachedDbApiHook

from operators.redshift_operator import FBRedshiftOperator
from operators.redshift_to_s3_operator import FBRedshiftToS3Transfer
from operators.write_signal_operator import FBWriteSignalOperator
from operators.signal_sensor import FBSignalSensor
from operators.postgres_to_s3_operator import FBPostgresToS3Operator
from operators.s3_to_redshift_operator import FBS3ToRedshiftOperator

class FacebookPlugin(AirflowPlugin):
    name = "facebook_plugin"
    operators = [
        FBRedshiftOperator,
        FBRedshiftToS3Transfer,
        FBWriteSignalOperator,
        FBSignalSensor,
        FBPostgresToS3Operator,
        FBS3ToRedshiftOperator,
    ]
    flask_blueprints = []
    hooks = [
        FBCachedDbApiHook,
    ]
    executors = []
    admin_views = []
    menu_links = []

# SNS handling code.
SNS_TOPIC = 'arn:aws:sns:us-east-1:950587841421:airflow-failures'
def send_sns(to, subject, html_content, files=None, dryrun=False, cc=None, bcc=None, mime_subtype='mixed'):
    conn = boto.sns.connect_to_region('us-east-1')
    # On linux boto, subject argument doesn't work, so let's just concatenate it.
    message = subject + "\n\n" + html_content
    conn.publish(topic=SNS_TOPIC, message=message)

# Hacks to make the SNS package available.
sns_module = imp.new_module('airflowsnshack')
sns_module.send_sns = send_sns
sys.modules['airflowsnshack'] = sns_module
