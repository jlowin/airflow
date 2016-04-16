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
import os
import unittest
import time

from airflow import models, AirflowException
from airflow.exceptions import AirflowSkipException
from airflow.models import DAG, DagBag, DagRun, DagModel, TaskInstance as TI
from airflow.models import State as ST
from airflow.operators import DummyOperator, BashOperator, PythonOperator
from airflow.utils.state import State
from airflow.utils.db import provide_session
from mock import patch
from nose_parameterized import parameterized

DEFAULT_DATE = datetime.datetime(2016, 1, 1)
TEST_DAGS_FOLDER = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), 'dags')


class DagTest(unittest.TestCase):

    def test_parms_not_passed_is_empty_dict(self):
        """
        Test that when 'params' is _not_ passed to a new Dag, that the params
        attribute is set to an empty dictionary.
        """
        dag = models.DAG('test-dag')

        assert type(dag.params) == dict
        assert len(dag.params) == 0

    def test_params_passed_and_params_in_default_args_no_override(self):
        """
        Test that when 'params' exists as a key passed to the default_args dict
        in addition to params being passed explicitly as an argument to the
        dag, that the 'params' key of the default_args dict is merged with the
        dict of the params argument.
        """
        params1 = {'parameter1': 1}
        params2 = {'parameter2': 2}

        dag = models.DAG('test-dag',
                         default_args={'params': params1},
                         params=params2)

        params_combined = params1.copy()
        params_combined.update(params2)
        assert dag.params == params_combined

    def test_dag_as_context_manager(self):
        """
        Test DAG as a context manager.

        When used as a context manager, Operators are automatically added to
        the DAG (unless they specifiy a different DAG)
        """
        dag = DAG(
            'dag',
            start_date=DEFAULT_DATE,
            default_args={'owner': 'owner1'})
        dag2 = DAG(
            'dag2',
            start_date=DEFAULT_DATE,
            default_args={'owner': 'owner2'})

        with dag:
            op1 = DummyOperator(task_id='op1')
            op2 = DummyOperator(task_id='op2', dag=dag2)

        self.assertIs(op1.dag, dag)
        self.assertEqual(op1.owner, 'owner1')
        self.assertIs(op2.dag, dag2)
        self.assertEqual(op2.owner, 'owner2')

        with dag2:
            op3 = DummyOperator(task_id='op3')

        self.assertIs(op3.dag, dag2)
        self.assertEqual(op3.owner, 'owner2')

        with dag:
            with dag2:
                op4 = DummyOperator(task_id='op4')
            op5 = DummyOperator(task_id='op5')

        self.assertIs(op4.dag, dag2)
        self.assertIs(op5.dag, dag)
        self.assertEqual(op4.owner, 'owner2')
        self.assertEqual(op5.owner, 'owner1')

        with DAG('creating_dag_in_cm', start_date=DEFAULT_DATE) as dag:
            DummyOperator(task_id='op6')

        self.assertEqual(dag.dag_id, 'creating_dag_in_cm')
        self.assertEqual(dag.tasks[0].task_id, 'op6')

class DagRunTest(unittest.TestCase):

    def test_comparison(self):
        """
        Test DagRun ==, >, and hashing ops
        """
        d1 = DagRun('dag', DEFAULT_DATE)
        d2 = DagRun('dag', DEFAULT_DATE)
        d3 = DagRun('dag2', DEFAULT_DATE)
        d4 = DagRun('dag2', DEFAULT_DATE + datetime.timedelta(days=1))

        self.assertEqual(d1, d2)
        self.assertGreater(d3, d2)
        self.assertGreater(d4, d3)

        s = set([d1, d2, d3, d4])
        # d1 and d2 are the same and should not be double counted in the set
        self.assertEqual(len(s), 3)
        # however all DRs should report that they are "in" the set
        for d in [d1, d2, d3, d4]:
            self.assertIn(d, s)

    def test_set_lock(self):
        """
        Test locking DagRuns and syncing state to other (identical) DagRuns
        """
        d1 = DagRun('dag', DEFAULT_DATE)
        d2 = DagRun('dag', DEFAULT_DATE)

        # locked starts out as False
        self.assertEqual((d1.locked, d2.locked), (False, False))
        d1.lock()
        d2.refresh_from_db()
        self.assertEqual((d1.locked, d2.locked), (True, True))
        # unlock d1 and sync
        d1.unlock()
        d2.refresh_from_db()
        self.assertEqual((d1.locked, d2.locked), (False, False))

    def test_cant_modify_locked(self):
        """
        Test that locked DagRuns can't be modified
        """
        d1 = DagRun('dag', DEFAULT_DATE)
        d2 = DagRun('dag', DEFAULT_DATE)
        d1.lock(lock_id=25)

        # process 25 can re-lock d1
        d1.lock(lock_id=25)

        # but process 26 can't lock d1 because process 25 already did
        self.assertRaises(AirflowException, d1.lock, lock_id=26)

    def test_set_conf(self):
        """
        Test setting DagRun conf and syncing to other (identical) DagRuns
        """
        d1 = DagRun('dag', DEFAULT_DATE)
        d2 = DagRun('dag', DEFAULT_DATE)

        # conf is None
        self.assertEqual((d1.conf, d2.conf), (None, None))
        # set d1 conf
        d1.set_conf(1)
        d2.refresh_from_db()
        self.assertEqual((d1.conf, d2.conf), (1, 1))

    def test_initial_state(self):
        """
        Test that DagRuns start in PENDING state
        """
        d1 = DagRun('dag', DEFAULT_DATE)
        # db object gets default state: PENDING
        self.assertEqual(d1.state, ST.PENDING)
        d1.refresh_from_db()
        self.assertEqual(d1.state, ST.PENDING)

    def test_update_state(self):
        """
        Test state modification
        """
        d1 = DagRun('dag', DEFAULT_DATE)
        self.assertEqual(d1.state, ST.PENDING)
        self.assertIs(d1.start_date, None)
        self.assertIs(d1.end_date, None)

        # changing to RUNNING should change start_date
        d1.set_state(ST.RUNNING)
        self.assertEqual(d1.state, ST.RUNNING)
        self.assertIsNot(d1.start_date, None)
        sd = d1.start_date

        # changing to SUCCESS should set end_date
        d1.set_state(ST.SUCCESS)
        self.assertEqual(d1.state, ST.SUCCESS)
        self.assertEqual(d1.start_date, sd)
        self.assertIsNot(d1.end_date, None)

        # back to PENDING, should reset
        d1.set_state(ST.PENDING)
        self.assertEqual(d1.state, ST.PENDING)
        self.assertIs(d1.start_date, None)
        self.assertIs(d1.end_date, None)

        # and failed, should set end date
        d1.set_state(ST.FAILED)
        self.assertEqual(d1.state, ST.FAILED)
        self.assertIs(d1.start_date, None)
        self.assertIsNot(d1.end_date, None)

        # invalid state
        self.assertRaises(AirflowException, d1.set_state, 'fake state')

    @provide_session
    def evaluate_dagrun(
            self,
            dag_id,
            expected_task_states,  # dict of task_id: state
            dagrun_state,
            run_kwargs=None,
            advance_execution_date=False,
            session=None):
        """
        Helper for testing DagRun states with simple two-task DAGS
        """
        if run_kwargs is None:
            run_kwargs = {}

        dag = DagBag().get_dag(dag_id)
        dag.clear()
        if advance_execution_date:
            # run a second time to schedule a dagrun after the start_date
            dr = dag.schedule_dag(
                last_scheduled=DEFAULT_DATE + datetime.timedelta(days=5))
        else:
            dr = dag.schedule_dag()
        ex_date = dr.execution_date

        try:
            dag.run(start_date=ex_date, end_date=ex_date, **run_kwargs)
        except AirflowException:
            pass

        # test tasks
        for task_id, expected_state in expected_task_states.items():
            task = dag.get_task(task_id)
            ti = TI(task, ex_date)
            ti.refresh_from_db()
            self.assertEqual(ti.state, expected_state)

        dr.refresh_from_db()
        self.assertEqual(dr.state, dagrun_state)

    def test_dagrun_fail(self):
        """
        DagRuns with one failed and one incomplete root task -> FAILED
        """
        self.evaluate_dagrun(
            dag_id='test_dagrun_states_fail',
            expected_task_states={
                'test_dagrun_fail': State.FAILED,
                'test_dagrun_succeed': State.UPSTREAM_FAILED,
            },
            dagrun_state=State.FAILED)

    def test_dagrun_success(self):
        """
        DagRuns with one failed and one successful root task -> SUCCESS
        """
        self.evaluate_dagrun(
            dag_id='test_dagrun_states_success',
            expected_task_states={
                'test_dagrun_fail': State.FAILED,
                'test_dagrun_succeed': State.SUCCESS,
            },
            dagrun_state=State.SUCCESS)

    def test_dagrun_root_fail(self):
        """
        DagRuns with one successful and one failed root task -> FAILED
        """
        self.evaluate_dagrun(
            dag_id='test_dagrun_states_root_fail',
            expected_task_states={
                'test_dagrun_succeed': State.SUCCESS,
                'test_dagrun_fail': State.FAILED,
            },
            dagrun_state=State.FAILED)

    def test_dagrun_deadlock(self):
        """
        Deadlocked DagRun is marked a failure

        Test that a deadlocked dagrun is marked as a failure by having
        depends_on_past and an execution_date after the start_date
        """
        self.evaluate_dagrun(
            dag_id='test_dagrun_states_deadlock',
            expected_task_states={
                'test_depends_on_past': None,
                'test_depends_on_past_2': None,
            },
            dagrun_state=State.FAILED,
            advance_execution_date=True)

    def test_dagrun_deadlock_ignore_depends_on_past_advance_ex_date(self):
        """
        DagRun is marked a success if ignore_first_depends_on_past=True

        Test that an otherwise-deadlocked dagrun is marked as a success
        if ignore_first_depends_on_past=True and the dagrun execution_date
        is after the start_date.
        """
        self.evaluate_dagrun(
            dag_id='test_dagrun_states_deadlock',
            expected_task_states={
                'test_depends_on_past': State.SUCCESS,
                'test_depends_on_past_2': State.SUCCESS,
            },
            dagrun_state=State.SUCCESS,
            advance_execution_date=True,
            run_kwargs=dict(ignore_first_depends_on_past=True))

    def test_dagrun_deadlock_ignore_depends_on_past(self):
        """
        Test that ignore_first_depends_on_past doesn't affect results
        (this is the same test as
        test_dagrun_deadlock_ignore_depends_on_past_advance_ex_date except
        that start_date == execution_date so depends_on_past is irrelevant).
        """
        self.evaluate_dagrun(
            dag_id='test_dagrun_states_deadlock',
            expected_task_states={
                'test_depends_on_past': State.SUCCESS,
                'test_depends_on_past_2': State.SUCCESS,
            },
            dagrun_state=State.SUCCESS,
            run_kwargs=dict(ignore_first_depends_on_past=True))


class DagBagTest(unittest.TestCase):

    def test_get_existing_dag(self):
        """
        test that were're able to parse some example DAGs and retrieve them
        """
        dagbag = models.DagBag(include_examples=True)

        some_expected_dag_ids = ["example_bash_operator",
                                 "example_branch_operator"]

        for dag_id in some_expected_dag_ids:
            dag = dagbag.get_dag(dag_id)

            assert dag is not None
            assert dag.dag_id == dag_id

        assert dagbag.size() >= 7

    def test_get_non_existing_dag(self):
        """
        test that retrieving a non existing dag id returns None without crashing
        """
        dagbag = models.DagBag(include_examples=True)

        non_existing_dag_id = "non_existing_dag_id"
        assert dagbag.get_dag(non_existing_dag_id) is None

    def test_process_file_that_contains_multi_bytes_char(self):
        """
        test that we're able to parse file that contains multi-byte char
        """
        from tempfile import NamedTemporaryFile
        f = NamedTemporaryFile()
        f.write('\u3042'.encode('utf8'))  # write multi-byte char (hiragana)
        f.flush()

        dagbag = models.DagBag(include_examples=True)
        assert dagbag.process_file(f.name) == []

    def test_zip(self):
        """
        test the loading of a DAG within a zip file that includes dependencies
        """
        dagbag = models.DagBag()
        dagbag.process_file(os.path.join(TEST_DAGS_FOLDER, "test_zip.zip"))
        assert dagbag.get_dag("test_zip_dag")

    @patch.object(DagModel,'get_current')
    def test_get_dag_without_refresh(self, mock_dagmodel):
        """
        Test that, once a DAG is loaded, it doesn't get refreshed again if it
        hasn't been expired.
        """
        dag_id = 'example_bash_operator'

        mock_dagmodel.return_value = DagModel()
        mock_dagmodel.return_value.last_expired = None
        mock_dagmodel.return_value.fileloc = 'foo'

        class TestDagBag(models.DagBag):
            process_file_calls = 0
            def process_file(self, filepath, only_if_updated=True, safe_mode=True):
                if 'example_bash_operator.py' in filepath:
                    TestDagBag.process_file_calls += 1
                super(TestDagBag, self).process_file(filepath, only_if_updated, safe_mode)

        dagbag = TestDagBag(include_examples=True)
        processed_files = dagbag.process_file_calls

        # Should not call process_file agani, since it's already loaded during init.
        assert dagbag.process_file_calls == 1
        assert dagbag.get_dag(dag_id) != None
        assert dagbag.process_file_calls == 1


class TaskInstanceTest(unittest.TestCase):

    def test_set_dag(self):
        """
        Test assigning Operators to Dags, including deferred assignment
        """
        dag = DAG('dag', start_date=DEFAULT_DATE)
        dag2 = DAG('dag2', start_date=DEFAULT_DATE)
        op = DummyOperator(task_id='op_1', owner='test')

        # no dag assigned
        self.assertFalse(op.has_dag())
        self.assertRaises(AirflowException, getattr, op, 'dag')

        # no improper assignment
        with self.assertRaises(TypeError):
            op.dag = 1

        op.dag = dag

        # no reassignment
        with self.assertRaises(AirflowException):
            op.dag = dag2

        # but assigning the same dag is ok
        op.dag = dag

        self.assertIs(op.dag, dag)
        self.assertIn(op, dag.tasks)

    def test_infer_dag(self):
        dag = DAG('dag', start_date=DEFAULT_DATE)
        dag2 = DAG('dag2', start_date=DEFAULT_DATE)

        op1 = DummyOperator(task_id='test_op_1', owner='test')
        op2 = DummyOperator(task_id='test_op_2', owner='test')
        op3 = DummyOperator(task_id='test_op_3', owner='test', dag=dag)
        op4 = DummyOperator(task_id='test_op_4', owner='test', dag=dag2)

        # double check dags
        self.assertEqual(
            [i.has_dag() for i in [op1, op2, op3, op4]],
            [False, False, True, True])

        # can't combine operators with no dags
        self.assertRaises(AirflowException, op1.set_downstream, op2)

        # op2 should infer dag from op1
        op1.dag = dag
        op1.set_downstream(op2)
        self.assertIs(op2.dag, dag)

        # can't assign across multiple DAGs
        self.assertRaises(AirflowException, op1.set_downstream, op4)
        self.assertRaises(AirflowException, op1.set_downstream, [op3, op4])

    def test_bitshift_compose_operators(self):
        dag = DAG('dag', start_date=DEFAULT_DATE)
        op1 = DummyOperator(task_id='test_op_1', owner='test')
        op2 = DummyOperator(task_id='test_op_2', owner='test')
        op3 = DummyOperator(task_id='test_op_3', owner='test')
        op4 = DummyOperator(task_id='test_op_4', owner='test')
        op5 = DummyOperator(task_id='test_op_5', owner='test')

        # can't compose operators without dags
        with self.assertRaises(AirflowException):
            op1 >> op2

        dag >> op1 >> op2 << op3

        # make sure dag assignment carries through
        # using __rrshift__
        self.assertIs(op1.dag, dag)
        self.assertIs(op2.dag, dag)
        self.assertIs(op3.dag, dag)

        # op2 should be downstream of both
        self.assertIn(op2, op1.downstream_list)
        self.assertIn(op2, op3.downstream_list)

        # test dag assignment with __rlshift__
        dag << op4
        self.assertIs(op4.dag, dag)

        # dag assignment with __rrshift__
        dag >> op5
        self.assertIs(op5.dag, dag)

    def test_run_pooling_task(self):
        """
        test that running task with mark_success param update task state as
        SUCCESS without running task.
        """
        dag = models.DAG(dag_id='test_run_pooling_task')
        task = DummyOperator(task_id='test_run_pooling_task_op', dag=dag,
                             pool='test_run_pooling_task_pool', owner='airflow',
                             start_date=datetime.datetime(2016, 2, 1, 0, 0, 0))
        ti = TI(
            task=task, execution_date=datetime.datetime.now())
        ti.run()
        self.assertEqual(ti.state, models.State.QUEUED)

    def test_run_pooling_task_with_mark_success(self):
        """
        test that running task with mark_success param update task state as SUCCESS
        without running task.
        """
        dag = models.DAG(dag_id='test_run_pooling_task_with_mark_success')
        task = DummyOperator(
            task_id='test_run_pooling_task_with_mark_success_op',
            dag=dag,
            pool='test_run_pooling_task_with_mark_success_pool',
            owner='airflow',
            start_date=datetime.datetime(2016, 2, 1, 0, 0, 0))
        ti = TI(
            task=task, execution_date=datetime.datetime.now())
        ti.run(mark_success=True)
        self.assertEqual(ti.state, models.State.SUCCESS)

    def test_run_pooling_task_with_skip(self):
        """
        test that running task which returns AirflowSkipOperator will end
        up in a SKIPPED state.
        """

        def raise_skip_exception():
            raise AirflowSkipException

        dag = models.DAG(dag_id='test_run_pooling_task_with_skip')
        task = PythonOperator(
            task_id='test_run_pooling_task_with_skip',
            dag=dag,
            python_callable=raise_skip_exception,
            owner='airflow',
            start_date=datetime.datetime(2016, 2, 1, 0, 0, 0))
        ti = TI(
            task=task, execution_date=datetime.datetime.now())
        ti.run()
        self.assertTrue(ti.state == models.State.SKIPPED)


    def test_retry_delay(self):
        """
        Test that retry delays are respected
        """
        dag = models.DAG(dag_id='test_retry_handling')
        task = BashOperator(
            task_id='test_retry_handling_op',
            bash_command='exit 1',
            retries=1,
            retry_delay=datetime.timedelta(seconds=3),
            dag=dag,
            owner='airflow',
            start_date=datetime.datetime(2016, 2, 1, 0, 0, 0))

        def run_with_error(ti):
            try:
                ti.run()
            except AirflowException:
                pass

        ti = TI(
            task=task, execution_date=datetime.datetime.now())

        # first run -- up for retry
        run_with_error(ti)
        self.assertEqual(ti.state, State.UP_FOR_RETRY)
        self.assertEqual(ti.try_number, 1)

        # second run -- still up for retry because retry_delay hasn't expired
        run_with_error(ti)
        self.assertEqual(ti.state, State.UP_FOR_RETRY)

        # third run -- failed
        time.sleep(3)
        run_with_error(ti)
        self.assertEqual(ti.state, State.FAILED)

    def test_retry_handling(self):
        """
        Test that task retries are handled properly
        """
        dag = models.DAG(dag_id='test_retry_handling')
        task = BashOperator(
            task_id='test_retry_handling_op',
            bash_command='exit 1',
            retries=1,
            retry_delay=datetime.timedelta(seconds=0),
            dag=dag,
            owner='airflow',
            start_date=datetime.datetime(2016, 2, 1, 0, 0, 0))

        def run_with_error(ti):
            try:
                ti.run()
            except AirflowException:
                pass

        ti = TI(
            task=task, execution_date=datetime.datetime.now())

        # first run -- up for retry
        run_with_error(ti)
        self.assertEqual(ti.state, State.UP_FOR_RETRY)
        self.assertEqual(ti.try_number, 1)

        # second run -- fail
        run_with_error(ti)
        self.assertEqual(ti.state, State.FAILED)
        self.assertEqual(ti.try_number, 2)

        # third run -- up for retry
        run_with_error(ti)
        self.assertEqual(ti.state, State.UP_FOR_RETRY)
        self.assertEqual(ti.try_number, 3)

        # fourth run -- fail
        run_with_error(ti)
        self.assertEqual(ti.state, State.FAILED)
        self.assertEqual(ti.try_number, 4)

    def test_depends_on_past(self):
        dagbag = models.DagBag()
        dag = dagbag.get_dag('test_depends_on_past')
        dag.clear()
        task = dag.tasks[0]
        run_date = task.start_date + datetime.timedelta(days=5)
        ti = TI(task, run_date)

        # depends_on_past prevents the run
        task.run(start_date=run_date, end_date=run_date)
        ti.refresh_from_db()
        self.assertIs(ti.state, None)

        # ignore first depends_on_past to allow the run
        task.run(
            start_date=run_date,
            end_date=run_date,
            ignore_first_depends_on_past=True)
        ti.refresh_from_db()
        self.assertEqual(ti.state, State.SUCCESS)

    # Parameterized tests to check for the correct firing
    # of the trigger_rule under various circumstances
    # Numeric fields are in order:
    #   successes, skipped, failed, upstream_failed, done
    @parameterized.expand([

        #
        # Tests for all_success
        #
        ['all_success', 5, 0, 0, 0, 0, True, None, True],
        ['all_success', 2, 0, 0, 0, 0, True, None, False],
        ['all_success', 2, 0, 1, 0, 0, True, ST.UPSTREAM_FAILED, False],
        ['all_success', 2, 1, 0, 0, 0, True, ST.SKIPPED, False],
        #
        # Tests for one_success
        #
        ['one_success', 5, 0, 0, 0, 5, True, None, True],
        ['one_success', 2, 0, 0, 0, 2, True, None, True],
        ['one_success', 2, 0, 1, 0, 3, True, None, True],
        ['one_success', 2, 1, 0, 0, 3, True, None, True],
        #
        # Tests for all_failed
        #
        ['all_failed', 5, 0, 0, 0, 5, True, ST.SKIPPED, False],
        ['all_failed', 0, 0, 5, 0, 5, True, None, True],
        ['all_failed', 2, 0, 0, 0, 2, True, ST.SKIPPED, False],
        ['all_failed', 2, 0, 1, 0, 3, True, ST.SKIPPED, False],
        ['all_failed', 2, 1, 0, 0, 3, True, ST.SKIPPED, False],
        #
        # Tests for one_failed
        #
        ['one_failed', 5, 0, 0, 0, 0, True, None, False],
        ['one_failed', 2, 0, 0, 0, 0, True, None, False],
        ['one_failed', 2, 0, 1, 0, 0, True, None, True],
        ['one_failed', 2, 1, 0, 0, 3, True, None, False],
        ['one_failed', 2, 3, 0, 0, 5, True, ST.SKIPPED, False],
        #
        # Tests for done
        #
        ['all_done', 5, 0, 0, 0, 5, True, None, True],
        ['all_done', 2, 0, 0, 0, 2, True, None, False],
        ['all_done', 2, 0, 1, 0, 3, True, None, False],
        ['all_done', 2, 1, 0, 0, 3, True, None, False]
    ])
    def test_check_task_dependencies(self, trigger_rule, successes, skipped,
                                     failed, upstream_failed, done,
                                     flag_upstream_failed,
                                     expect_state, expect_completed):
        start_date = datetime.datetime(2016, 2, 1, 0, 0, 0)
        dag = models.DAG('test-dag', start_date=start_date)
        downstream = DummyOperator(task_id='downstream',
                                   dag=dag, owner='airflow',
                                   trigger_rule=trigger_rule)
        for i in range(5):
            task = DummyOperator(task_id='runme_{}'.format(i),
                                 dag=dag, owner='airflow')
            task.set_downstream(downstream)
        run_date = task.start_date + datetime.timedelta(days=5)

        ti = TI(downstream, run_date)
        completed = ti.evaluate_trigger_rule(
            successes=successes, skipped=skipped, failed=failed,
            upstream_failed=upstream_failed, done=done,
            flag_upstream_failed=flag_upstream_failed)

        self.assertEqual(completed, expect_completed)
        self.assertEqual(ti.state, expect_state)
