from datetime import datetime
import logging

from airflow.models import BaseOperator, DagRun
from airflow.utils.decorators import apply_defaults
from airflow import settings


class DagRunOrder(object):
    def __init__(self, dag_id, execution_date, payload=None):
        self.dag_id = dag_id
        self.execution_date = execution_date
        self.payload = payload


class TriggerDagRunOperator(BaseOperator):
    """
    Triggers a DAG run for a specified ``dag_id`` if a criteria is met

    :param trigger_dag_id: the dag_id to trigger
    :type trigger_dag_id: str
    :param python_callable: a reference to a python function that will be
        called while passing it the ``context`` object and a placeholder
        object ``obj`` for your callable to fill and return if you want
        a DagRun created. This ``obj`` object contains a ``dag_id``,
        ``execution_date``, and
        ``payload`` attribute that you can modify in your function.
        The payload has to be a picklable object that will be made available
        to your tasks while executing that DAG run. Your function header
        should look like ``def foo(context, dag_run_obj):``
    :type python_callable: python callable
    """
    template_fields = tuple()
    template_ext = tuple()
    ui_color = '#ffefeb'
    @apply_defaults
    def __init__(
            self,
            trigger_dag_id,
            python_callable,
            *args, **kwargs):
        super(TriggerDagRunOperator, self).__init__(*args, **kwargs)
        self.python_callable = python_callable
        self.trigger_dag_id = trigger_dag_id

    def execute(self, context):
        execution_date = datetime.now()
        dro = DagRunOrder(
            dag_id=self.trigger_dag_id,
            execution_date=execution_date)
        dro = self.python_callable(context, dro)
        if dro:
            session = settings.Session()
            dr = DagRun(
                dag_id=self.trigger_dag_id,
                execution_date=execution_date)
            dr.set_conf(dro.payload)
            logging.info("Creating DagRun {}".format(dr))
            dr.refresh_from_db()
            session.merge(dr)
            session.commit()
            session.close()
        else:
            logging.info("Criteria not met, moving on")
