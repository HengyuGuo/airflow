import boto
import logging

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from operators.constants import SNS_CONNECTION_REGION, MESSAGE_SNS_TOPIC

class FBSNSOperator(BaseOperator):
    template_fields = (
        'message',
    )

    @apply_defaults
    def __init__(self, message, *args, **kwargs):
        super(FBSNSOperator, self).__init__(*args, **kwargs)
        self.message = message

    def execute(self, context):
        topic = MESSAGE_SNS_TOPIC
        logging.info('Writing message to \'{0}\': "{1}"'.format(topic, self.message))
        sns = boto.sns.connect_to_region(SNS_CONNECTION_REGION)
        sns.publish(topic=topic, message=self.message)
