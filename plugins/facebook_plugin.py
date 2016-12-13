from airflow.plugins_manager import AirflowPlugin

from operators.redshift_operator import FBRedshiftOperator
from operators.redshift_to_s3_operator import FBRedshiftToS3Transfer
from operators.write_signal_operator import FBWriteSignalOperator
from operators.signal_sensor import FBSignalSensor

class FacebookPlugin(AirflowPlugin):
    name = "facebook_plugin"
    operators = [
        FBRedshiftOperator,
        FBRedshiftToS3Transfer,
        FBWriteSignalOperator,
        FBSignalSensor,
    ]
    flask_blueprints = []
    hooks = []
    executors = []
    admin_views = []
    menu_links = []
