import logging
from airflow.operators import IntervalCheckOperator
from airflow.utils.decorators import apply_defaults
from fb_required_args import require_keyword_args

class FBHistoricalCheckOperator(IntervalCheckOperator):

    @apply_defaults
    @require_keyword_args(['task_id', 'table', 'conn_id', 'dag'])
    def __init__(
        self,
        days_back=-7,
        metrics_thresholds={'COUNT(1)': '0.25'},
        date_filter_column='as_of',
        *args,
        **kwargs
    ):
        super(FBHistoricalCheckOperator, self).__init__(
            date_filter_column=date_filter_column,
            days_back=days_back,
            metrics_thresholds=metrics_thresholds,
            *args, 
            **kwargs
        )

