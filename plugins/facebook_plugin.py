import boto.sns
import imp
import sys

from airflow.plugins_manager import AirflowPlugin

from hooks.cached_db_api_hook import FBCachedDbApiHook

from operators.constants import FAILURE_SNS_TOPIC
from operators.redshift_operator import FBRedshiftOperator
from operators.redshift_to_s3_operator import FBRedshiftToS3Transfer
from operators.write_signal_operator import FBWriteSignalOperator
from operators.signal_sensor import FBSignalSensor
from operators.postgres_to_s3_operator import FBPostgresToS3JSONOperator, FBPostgresToS3CSVOperator
from operators.s3_to_redshift_operator import FBS3ToRedshiftOperator
from operators.redshift_query_killer_operator import FBRedshiftQueryKillerOperator

class FacebookPlugin(AirflowPlugin):
    name = "facebook_plugin"
    operators = [
        FBRedshiftOperator,
        FBRedshiftToS3Transfer,
        FBWriteSignalOperator,
        FBSignalSensor,
        FBPostgresToS3JSONOperator,
        FBPostgresToS3CSVOperator,
        FBS3ToRedshiftOperator,
        FBRedshiftQueryKillerOperator,
    ]
    flask_blueprints = []
    hooks = [
        FBCachedDbApiHook,
    ]
    executors = []
    admin_views = []
    menu_links = []

# SNS handling code.
def send_sns(to, subject, html_content, files=None, dryrun=False, cc=None, bcc=None, mime_subtype='mixed'):
    conn = boto.sns.connect_to_region('us-east-1')
    # On linux boto, subject argument doesn't work, so let's just concatenate it.
    message = subject + "\n\n" + html_content
    conn.publish(topic=FAILURE_SNS_TOPIC, message=message)

# Hacks to make the SNS package available.
sns_module = imp.new_module('airflowsnshack')
sns_module.send_sns = send_sns
sys.modules['airflowsnshack'] = sns_module
