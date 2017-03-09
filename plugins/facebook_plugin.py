import boto.sns
import imp
import sys

from airflow.plugins_manager import AirflowPlugin

from hooks.cached_db_api_hook import FBCachedDbApiHook

from operators.constants import FAILURE_SNS_TOPIC, SNS_CONNECTION_REGION
from operators.csv_to_json_operator import FBCSVToJSONOperator
from operators.external_dagrun_sensor import FBExternalDagRunSensor
from operators.fb_historical_check_operator import FBHistoricalCheckOperator
from operators.postgres_to_s3_operator import FBPostgresToS3JSONOperator, FBPostgresToS3CSVOperator
from operators.redshift_enum_udf_operator import FBRedshiftEnumUDFOperator
from operators.redshift_operator import FBHistoricalOperator, FBRedshiftOperator
from operators.redshift_query_killer_operator import FBRedshiftQueryKillerOperator
from operators.redshift_to_s3_operator import FBRedshiftToS3Transfer
from operators.s3_key_sensor import FBS3KeySensor
from operators.s3_to_redshift_operator import FBS3ToRedshiftOperator
from operators.signal_sensor import FBSignalSensor
from operators.sns_operator import FBSNSOperator
from operators.write_signal_operator import FBWriteSignalOperator
from operators.external_dagrun_sensor import FBExternalDagRunSensor
from operators.snowflake_operator import FBSnowflakeOperator, FBSnowflakeCreateStageOperator
from operators.s3_to_snowflake_operator import FBS3ToSnowflakeOperator

class FacebookPlugin(AirflowPlugin):
    name = "facebook_plugin"
    operators = [
        FBCSVToJSONOperator,
        FBExternalDagRunSensor,
        FBHistoricalCheckOperator,
        FBHistoricalOperator,
        FBPostgresToS3CSVOperator,
        FBPostgresToS3JSONOperator,
        FBRedshiftEnumUDFOperator,
        FBRedshiftOperator,
        FBRedshiftQueryKillerOperator,
        FBRedshiftToS3Transfer,
        FBS3KeySensor,
        FBS3ToRedshiftOperator,
        FBS3ToSnowflakeOperator,
        FBSignalSensor,
        FBSnowflakeCreateStageOperator,
        FBSnowflakeOperator,
        FBSNSOperator,
        FBWriteSignalOperator,
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
    conn = boto.sns.connect_to_region(SNS_CONNECTION_REGION)
    # On linux boto, subject argument doesn't work, so let's just concatenate it.
    message = subject + "\n\n" + html_content
    conn.publish(topic=FAILURE_SNS_TOPIC, message=message)

# Hacks to make the SNS package available.
sns_module = imp.new_module('airflowsnshack')
sns_module.send_sns = send_sns
sys.modules['airflowsnshack'] = sns_module
