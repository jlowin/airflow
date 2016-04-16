# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import datetime
import logging
import unittest

import airflow
from airflow import AirflowException, settings
from airflow.bin import cli
from airflow.jobs import BaseJob, DagRunJob, BackfillJob, SchedulerJob
from airflow.models import DagBag, DagRun, Pool, TaskInstance as TI
from airflow.utils.state import State
from airflow.utils.timeout import timeout
from airflow.utils.db import provide_session

from sqlalchemy.orm.session import make_transient

from airflow import configuration
configuration.test_mode()

DEV_NULL = '/dev/null'
DEFAULT_DATE = datetime.datetime(2016, 1, 1)

class DagRunJobTest(unittest.TestCase):

    def setUp(self):
        self.job = DagRunJob()
        self.job.run()
        self.job.dagbag = DagBag(include_examples=True)
        self.job.executor = self.job.dagbag.executor
        self.dag = self.job.dagbag.dags['example_bash_operator']

    def test_kill_zombie_dagruns(self):
        dr = DagRun(self.dag.dag_id, DEFAULT_DATE)

        session = settings.Session()
        dr.lock(lock_id=session.query(BaseJob.id).first()[0])
        self.assertTrue(dr.locked)

        # kill the job
        try:
            self.job.kill()
        except AirflowException:
            pass
        self.job.refresh_dags(full_refresh=True)
        dr.refresh_from_db()
        self.assertFalse(dr.locked)


class BackfillJobTest(unittest.TestCase):

    def setUp(self):
        self.parser = cli.CLIFactory.get_parser()
        self.dagbag = DagBag(include_examples=True)

    def test_backfill_examples(self):
        """
        Test backfilling example dags
        """
        1/0
        # some DAGs really are just examples... but try to make them work!
        skip_dags = [
            'example_http_operator',
            'example_twitter_dag',
        ]

        logger = logging.getLogger('BackfillJobTest.test_backfill_examples')
        dags = [
            dag for dag in DagBag().dags.values()
            if 'example_dags' in dag.full_filepath
            and dag.dag_id not in skip_dags
            ]

        for dag in dags:
            dag.clear(
                start_date=DEFAULT_DATE,
                end_date=DEFAULT_DATE)

        for i, dag in enumerate(sorted(dags, key=lambda d: d.dag_id)):
            logger.info('*** Running example DAG #{}: {}'.format(i, dag.dag_id))
            job = BackfillJob(
                dag=dag,
                start_date=DEFAULT_DATE,
                end_date=DEFAULT_DATE,
                ignore_first_depends_on_past=True)
            job.run()

    def test_backfill_pooled_tasks(self):
        """
        Test that queued tasks are executed by BackfillJob

        Test for https://github.com/airbnb/airflow/pull/1225
        """
        session = settings.Session()
        pool = Pool(pool='test_backfill_pooled_task_pool', slots=1)
        session.add(pool)
        session.commit()

        dag = DagBag().get_dag('test_backfill_pooled_task_dag')
        dag.clear()

        job = BackfillJob(
            dag=dag,
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE)

        # run with timeout because this creates an infinite loop if not
        # caught
        with timeout(seconds=30):
            job.run()

        ti = TI(
            task=dag.get_task('test_backfill_pooled_task'),
            execution_date=DEFAULT_DATE)
        ti.refresh_from_db()
        self.assertEqual(ti.state, State.SUCCESS)

    def test_backfill_depends_on_past(self):
        """
        Test that backfill respects ignore_depends_on_past
        """
        dag = DagBag().get_dag('test_depends_on_past')
        dag.clear()
        run_date = DEFAULT_DATE + datetime.timedelta(days=5)

        # backfill should deadlock
        self.assertRaisesRegexp(
            AirflowException,
            'BackfillJob is deadlocked',
            BackfillJob(dag=dag, start_date=run_date, end_date=run_date).run)

        BackfillJob(
            dag=dag,
            start_date=run_date,
            end_date=run_date,
            ignore_first_depends_on_past=True).run()

        # ti should have succeeded
        ti = TI(dag.tasks[0], run_date)
        ti.refresh_from_db()
        self.assertEquals(ti.state, State.SUCCESS)

    def test_cli_backfill_depends_on_past(self):
        """
        Test that CLI respects -I argument
        """
        dag_id = 'test_dagrun_states_deadlock'
        run_date = DEFAULT_DATE + datetime.timedelta(days=1)
        args = [
            'backfill',
            dag_id,
            '-l',
            '-s',
            run_date.isoformat(),
        ]
        dag = DagBag().get_dag(dag_id)
        dag.clear()

        # backfill shouldn't run because of depends_on_past
        cli.backfill(self.parser.parse_args(args))
        ti = TI(dag.get_task('test_depends_on_past'), run_date)
        ti.refresh_from_db()
        self.assertEqual(ti.state, State.NONE)

        # backfill now ignores depends_on_past
        dag.clear()
        cli.backfill(self.parser.parse_args(args + ['-I']))
        ti = TI(dag.get_task('test_depends_on_past'), run_date)
        ti.refresh_from_db()
        # task ran
        self.assertEqual(ti.state, State.SUCCESS)


class SchedulerJobTest(unittest.TestCase):

    def test_scheduler_pooled_tasks(self):
        """
        Test that the scheduler handles queued tasks correctly
        See issue #1299
        """
        session = settings.Session()
        if not (
                session.query(Pool)
                .filter(Pool.pool == 'test_queued_pool')
                .first()):
            pool = Pool(pool='test_queued_pool', slots=5)
            session.merge(pool)
            session.commit()
        session.close()

        dag_id = 'test_scheduled_queued_tasks'
        dag = DagBag().get_dag(dag_id)
        dag.clear()

        scheduler = SchedulerJob(dag_ids=[dag_id], num_runs=10)
        scheduler.run()

        task_1 = dag.tasks[0]
        logging.info("Trying to find task {}".format(task_1))
        ti = TI(task_1, dag.start_date)
        ti.refresh_from_db()
        self.assertEqual(ti.state, State.FAILED)

        dag.clear()

    def test_scheduler_start_date(self):
        """
        Test that the scheduler respects start_dates, even when DAGS have run
        """

        dag_id = 'test_start_date_scheduling'
        dag = DagBag().get_dag(dag_id)
        dag.clear()
        self.assertTrue(dag.start_date > DEFAULT_DATE)

        scheduler = SchedulerJob(dag_ids=[dag_id], num_runs=2)
        scheduler.run()

        # zero tasks ran
        session = settings.Session()
        self.assertEqual(
            len(session.query(TI).filter(TI.dag_id == dag_id).all()), 0)

        # previously, running this backfill would kick off the Scheduler
        # because it would take the most recent run and start from there
        # That behavior still exists, but now it will only do so if after the
        # start date
        backfill = BackfillJob(
            dag=dag,
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE)
        backfill.run()

        # one task ran
        session = settings.Session()
        self.assertEqual(
            len(session.query(TI).filter(TI.dag_id == dag_id).all()), 1)

        scheduler = SchedulerJob(dag_ids=[dag_id], num_runs=2)
        scheduler.run()

        # still one task
        session = settings.Session()
        self.assertEqual(
            len(session.query(TI).filter(TI.dag_id == dag_id).all()), 1)

    def test_scheduler_multiprocessing(self):
        """
        Test that the scheduler can successfully queue multiple dags in parallel
        """
        dag_ids = ['test_start_date_scheduling', 'test_dagrun_states_success']
        for dag_id in dag_ids:
            dag = DagBag().get_dag(dag_id)
            dag.clear()

        scheduler = SchedulerJob(dag_ids=dag_ids, num_runs=2)
        scheduler.run()

        # zero tasks ran
        dag_id = 'test_start_date_scheduling'
        session = settings.Session()
        self.assertEqual(
            len(session.query(TI).filter(TI.dag_id == dag_id).all()), 0)
