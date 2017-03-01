from datetime import timedelta
from airflow import settings
from airflow.operators.sensors import BaseSensorOperator
from airflow.exceptions import AirflowException
from airflow.models import DagRun
from airflow.utils.state import State
from fb_required_args import require_keyword_args

class FBExternalDagRunSensor(BaseSensorOperator):
    template_fields = (
        'wait_for_dag_id',
        'execution_date',
    )
 
    @require_keyword_args(['task_id', 'wait_for_dag_id', 'dag'])
    def __init__(
        self,
        wait_for_dag_id=None,
        execution_date='{{ ts }}',
        retry_delay=timedelta(seconds=600),
        retries=144,  # 600 seconds * 144 = 1 day
        *args, **kwargs):
        super(FBExternalDagRunSensor, self).__init__(*args, **kwargs)
        self.wait_for_dag_id = wait_for_dag_id
        self.execution_date = execution_date
        self.retry_delay = retry_delay
        self.retries = retries

    def poke(self, context):
        session = settings.Session()
        successes = session.query(DagRun).filter(
            DagRun.dag_id == self.wait_for_dag_id,
            DagRun.execution_date == self.execution_date,
            DagRun.state == State.SUCCESS,
        ).count()
        if successes > 0:
            return True
        raise AirflowException('DAG {dag}, execution_date={date} did not complete yet.'.format(
            dag=self.wait_for_dag_id,
            date=self.execution_date,
        ))
