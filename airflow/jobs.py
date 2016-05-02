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
#
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from past.builtins import basestring
from collections import defaultdict, Counter
from datetime import datetime
from itertools import product
import getpass
import logging
import six
import socket
import subprocess
from time import sleep
import warnings

from sqlalchemy import Column, Integer, String, DateTime, func, Index, or_
from sqlalchemy.orm.session import make_transient

from airflow import executors, models, settings
from airflow.models import DagBag, DagModel, DagRun, TaskInstance as TI
from airflow import configuration as conf
from airflow.exceptions import AirflowException
from airflow.utils.state import State
from airflow.utils.db import provide_session, pessimistic_connection_handling
from airflow.utils.email import send_email
from airflow.utils.logging import LoggingMixin
from airflow.utils import asciiart

Base = models.Base
ID_LEN = models.ID_LEN
Stats = settings.Stats


class BaseJob(Base, LoggingMixin):
    """
    Abstract class to be derived for jobs. Jobs are processing items with state
    and duration that aren't task instances. For instance a BackfillJob is
    a collection of task instance runs, but should have it's own state, start
    and end time.
    """

    __tablename__ = "job"

    id = Column(Integer, primary_key=True)
    dag_id = Column(String(ID_LEN),)
    state = Column(String(20))
    job_type = Column(String(30))
    start_date = Column(DateTime())
    end_date = Column(DateTime())
    latest_heartbeat = Column(DateTime())
    executor_class = Column(String(500))
    hostname = Column(String(500))
    unixname = Column(String(1000))

    __mapper_args__ = {
        'polymorphic_on': job_type,
        'polymorphic_identity': 'BaseJob'
    }

    __table_args__ = (
        Index('job_type_heart', job_type, latest_heartbeat),
    )

    def __init__(
            self,
            executor=None,
            heartrate=None,
            *args, **kwargs):
        if executor is None:
            executor = executors.DEFAULT_EXECUTOR
        if heartrate is None:
            heartrate = conf.getfloat('scheduler', 'JOB_HEARTBEAT_SEC')
        self.hostname = socket.gethostname()
        self.executor = executor
        self.executor_class = executor.__class__.__name__
        self.start_date = datetime.now()
        self.latest_heartbeat = datetime.now()
        self.heartrate = heartrate
        self.unixname = getpass.getuser()
        super(BaseJob, self).__init__(*args, **kwargs)


    def is_alive(self):
        return (
            (datetime.now() - self.latest_heartbeat).seconds <
            (conf.getint('scheduler', 'JOB_HEARTBEAT_SEC') * 2.1)
        )

    def kill(self):
        session = settings.Session()
        job = session.query(BaseJob).filter(BaseJob.id == self.id).first()
        job.end_date = datetime.now()
        try:
            self.on_kill()
        except:
            self.logger.error('on_kill() method failed')
        session.merge(job)
        session.commit()
        session.close()
        raise AirflowException("Job shut down externally.")

    def on_kill(self):
        '''
        Will be called when an external kill command is received
        '''
        pass

    def heartbeat_callback(self):
        pass

    def heartbeat(self):
        '''
        Heartbeats update the job's entry in the database with a timestamp
        for the latest_heartbeat and allows for the job to be killed
        externally. This allows at the system level to monitor what is
        actually active.

        For instance, an old heartbeat for SchedulerJob would mean something
        is wrong.

        This also allows for any job to be killed externally, regardless
        of who is running it or on which machine it is running.

        Note that if your heartbeat is set to 60 seconds and you call this
        method after 10 seconds of processing since the last heartbeat, it
        will sleep 50 seconds to complete the 60 seconds and keep a steady
        heart rate. If you go over 60 seconds before calling it, it won't
        sleep at all.
        '''
        session = settings.Session()
        job = session.query(BaseJob).filter(BaseJob.id == self.id).first()

        if job.state == State.SHUTDOWN:
            self.kill()

        if job.latest_heartbeat:
            sleep_for = self.heartrate - (
                datetime.now() - job.latest_heartbeat).total_seconds()
            if sleep_for > 0:
                sleep(sleep_for)

        job.latest_heartbeat = datetime.now()

        session.merge(job)
        session.commit()
        session.close()

        self.heartbeat_callback()
        self.logger.debug('[heart] Boom.')

    def run(self):
        Stats.incr(self.__class__.__name__.lower()+'_start', 1, 1)
        # Adding an entry in the DB
        session = settings.Session()
        self.state = State.RUNNING
        session.add(self)
        session.commit()
        id_ = self.id
        make_transient(self)
        self.id = id_

        # Run
        self._execute()

        # Marking the success in the DB
        self.end_date = datetime.now()
        self.state = State.SUCCESS
        session.merge(self)
        session.commit()
        session.close()

        Stats.incr(self.__class__.__name__.lower()+'_end', 1, 1)

    def _execute(self):
        raise NotImplementedError("This method needs to be overridden")


class LocalTaskJob(BaseJob):

    __mapper_args__ = {
        'polymorphic_identity': 'LocalTaskJob'
    }

    def __init__(
            self,
            task_instance,
            ignore_dependencies=False,
            ignore_depends_on_past=False,
            force=False,
            mark_success=False,
            pickle_id=None,
            pool=None,
            *args, **kwargs):
        self.task_instance = task_instance
        self.ignore_dependencies = ignore_dependencies
        self.ignore_depends_on_past = ignore_depends_on_past
        self.force = force
        self.pool = pool
        self.pickle_id = pickle_id
        self.mark_success = mark_success
        super(LocalTaskJob, self).__init__(*args, **kwargs)

    def _execute(self):
        command = self.task_instance.command(
            raw=True,
            ignore_dependencies=self.ignore_dependencies,
            ignore_depends_on_past=self.ignore_depends_on_past,
            force=self.force,
            pickle_id=self.pickle_id,
            mark_success=self.mark_success,
            job_id=self.id,
            pool=self.pool,
        )
        self.process = subprocess.Popen(['bash', '-c', command])
        return_code = None
        while return_code is None:
            self.heartbeat()
            return_code = self.process.poll()

    def on_kill(self):
        self.process.terminate()

    """
    def heartbeat_callback(self):
        if datetime.now() - self.start_date < timedelta(seconds=300):
            return
        # Suicide pill
        TI = models.TaskInstance
        ti = self.task_instance
        session = settings.Session()
        state = session.query(TI.state).filter(
            TI.dag_id==ti.dag_id, TI.task_id==ti.task_id,
            TI.execution_date==ti.execution_date).scalar()
        session.commit()
        session.close()
        if state != State.RUNNING:
            logging.warning(
                "State of this instance has been externally set to "
                "{self.task_instance.state}. "
                "Taking the poison pill. So long.".format(**locals()))
            self.process.terminate()
    """

class DagRunJob(BaseJob):
    """
    The DagRunJob is used to create, schedule, and execute DagRuns.

    SchedulerJob and BacktestJob are derived from DagRunJob.
    """

    __mapper_args__ = {
        'polymorphic_identity': 'DagRunJob'
    }

    def __init__(
            self,
            executor=None,
            heartrate=None,
            dag_ids=None,
            subdir=None,
            do_pickle=False,
            refresh_dags_every=10,
            mark_success=False,
            ignore_dependencies=False,
            include_adhoc=False,
            pool=None):
        if isinstance(dag_ids, six.string_types):
            dag_ids = [dag_ids]
        self.dag_ids = set(dag_ids) if dag_ids else None
        self.dagbag = models.DagBag(dag_folder=subdir, sync_to_db=True)
        self.dags = {}
        self.dagruns = set()
        self.refresh_dags_every = refresh_dags_every

        self.do_pickle = do_pickle
        self.mark_success = mark_success
        self.ignore_dependencies = ignore_dependencies
        self.first_run_dates = {}
        self.pool = pool
        self.include_adhoc=False

        super(DagRunJob, self).__init__(
            executor=executor or self.dagbag.executor,
            heartrate=heartrate,
        )

    def _execute(self):

        self.executor.start()

        i = 0
        while self.dagruns:
            self.refresh_dags(full_refresh=(i % self.refresh_dags_every == 0))
            self.collect_dagruns()
            self.process_dagruns()
            self.executor.heartbeat()
            self.heartbeat()
            i += 1

        self.executor.end()

    @provide_session
    def submit_dagruns(self, dagruns, session=None):
        try:
            dagruns = list(dagruns)
        except TypeError:
            dagruns = [dagruns]
        # remove None
        dagruns = [d for d in dagruns if d]

        for dr in dagruns:
            dr.refresh_from_db()
            session.merge(dr)
        session.commit()

        self.dagruns.update(dagruns)

    def refresh_dags(self, full_refresh=False):
        self.dagbag.collect_dags(only_if_updated=not full_refresh)
        self.dagbag.deactivate_inactive_dags()
        if full_refresh:
            self.dagbag.kill_zombies()

    @provide_session
    def collect_dagruns(self, session=None):
        """
        DagRunJob runs any jobs in self.dagruns. In addition, it collects
        any DagRuns that have been assigned to it (via lock_id).
        """
        runs = (
            session.query(DagRun)
                .filter(
                    or_(
                        DagRun.state == State.RUNNING,
                        DagRun.state == State.NONE),
                    DagRun.lock_id == self.id)
                .all())
        self.submit_dagruns(runs)
        return runs

    @provide_session
    def process_dagruns(self, ignore_depends_on_past_dates=None, session=None):
        """
        This method:
            1. loops over each RUNNING dagrun in self.dagruns
                1a. if the dagrun is locked by a different job or not RUNNING,
                    skips it
                1b. takes a lock on the dagrun
                1c. calls dagrun.run()
            2. calls self.prioritize_queued()
            3. calls self.update_dagrun_states()
            4. unlocks the dagruns
        """
        progress = dict()
        skipped_runs = set()
        session.expunge_all()
        if ignore_depends_on_past_dates is None:
            ignore_depends_on_past_dates = {}

        # TODO parallelize with multiprocessing.Pool
        try:
            # take a lock on the dagruns
            for run in list(self.dagruns):
                run.refresh_from_db(session=session)
                # remove finished runs
                if run.state in (State.SUCCESS, State.FAILED):
                    self.dagruns.remove(run)
                # skip over pending [NONE] runs
                elif run.state == State.NONE:
                    skipped_runs.add(run)
                # skip over locked runs
                elif run.lock_id and run.lock_id != self.id:
                    skipped_runs.add(run)
                # lock the run
                else:
                    run.lock(lock_id=self.id, session=session)

            # send each dagrun's tasks to the executor
            for dagrun in sorted(self.dagruns.difference(skipped_runs)):
                try:
                    ignore_depends_on_past = (
                        dagrun.execution_date ==
                            ignore_depends_on_past_dates.get(dagrun.dag_id))

                    progress[dagrun] = dagrun.run(
                        dagbag=self.dagbag,
                        executor=self.executor,
                        run_queued_tasks=False,
                        do_pickle=self.do_pickle,
                        include_adhoc=self.include_adhoc,
                        ignore_dependencies=self.ignore_dependencies,
                        mark_success=self.mark_success,
                        pool=self.pool,
                        ignore_depends_on_past=ignore_depends_on_past,
                        session=session)
                except AirflowException as e:
                    self.logger.exception(e)

            # run queued tasks
            self.prioritize_queued(session=session)

            # update dagrun states
            for dag in self.dagbag.dags.values():
                dag.update_dagrun_states(
                    ignore_depends_on_past_dates=ignore_depends_on_past_dates,
                    session=session)

        except Exception as e:
            self.logger.exception(e)

        finally:
            # unlock the dagruns
            for run in self.dagruns.difference(skipped_runs):
                run.unlock(session=session)

        return progress

    def get_ti_from_key(self, key):
        """
        Given a TaskInstance key, returns the corresponding
        TaskInstance. If the DAG or task can not be found in this
        Job's DagBag, returns None.

        The returned TI is NOT refreshed from the DB. Users should call ti.refresh_from_db() if current information is important.
        """
        dag_id, task_id, execution_date = key
        dag = self.dagbag.dags.get(dag_id, None)
        if not dag:
            self.logger.error('DAG not found in DagBag: {}'.format(dag_id))
            return
        task = dag.task_dict.get(task_id, None)
        if not task:
            self.logger.error('Task not found in DAG: {}'.format(task_id))
            return
        return TI(task, execution_date)

    def get_dag_from_dagrun(self, dagrun):
        dag = self.dagbag.dags.get(dagrun.dag_id, None)
        if not dag:
            self.logger.error(
                'DAG not found in DagBag: {}'.format(dagrun.dag_id))
        else:
            return dag

    @provide_session
    def prioritize_queued(self, session=None):
        """
        Given a set of queued task keys, tries to process
        the queued tasks in the order determined by their
        relative priorities
        """

        if not self.dagruns:
            return

        pools = {p.pool: p for p in session.query(models.Pool).all()}

        session.expunge_all()
        d = defaultdict(list)

        queued = (
            session.query(TI)
                .filter(
                    TI.state == State.QUEUED,
                    TI.dag_id.in_([r.dag_id for r in self.dagruns]),
                    TI.execution_date.in_(
                        [r.execution_date for r in self.dagruns]))
                .all())

        if not queued:
            return

        self.logger.info(
            "Prioritizing {} queued jobs".format(len(queued)))

        for ti in queued:
            d[ti.pool].append(ti)

        for pool, tis in list(d.items()):
            if not pool:
                # Arbitrary:
                # If queued outside of a pool, trigger no more than
                # non_pooled_task_slot_count per run
                open_slots = conf.getint('core', 'non_pooled_task_slot_count')
            else:
                open_slots = pools[pool].open_slots(session=session)

            queue_size = len(tis)
            self.logger.debug("Pool {pool} has {open_slots} slots, {queue_size} "
                             "task instances in queue".format(**locals()))
            if open_slots <= 0:
                continue
            tis = sorted(tis, key=lambda ti: (
                -ti.priority_weight, ti.start_date, ti.execution_date))
            for ti in tis:
                if open_slots <= 0:
                    continue
                task = None
                try:
                    task = self.dagbag.dags[ti.dag_id].get_task(ti.task_id)
                except:
                    self.logger.error(
                        "Queued task {} seems to be gone".format(ti))
                    session.delete(ti)
                    session.commit()
                    continue

                if not task:
                    continue

                ti.task = task

                # picklin'
                dag = self.dagbag.dags[ti.dag_id]
                pickle_id = None
                if self.do_pickle and self.executor.__class__ not in (
                        executors.LocalExecutor,
                        executors.SequentialExecutor):
                    self.logger.info("Pickling DAG {}".format(dag))
                    pickle_id = dag.pickle(session).id

                if dag.concurrency_reached:
                    continue
                if ti.are_dependencies_met():
                    self.executor.queue_task_instance(ti, pickle_id=pickle_id)
                    open_slots -= 1
                else:
                    session.delete(ti)
                    continue
                ti.task = task

                session.commit()

    @provide_session
    def manage_slas(self, session=None):
        for run in self.dagruns:
            dag = self.get_dag_from_dagrun(run)
            if dag:
                self._manage_slas(dag)

    @provide_session
    def _manage_slas(self, dag, session=None):
        """
        Finding all tasks that have SLAs defined, and sending alert emails
        where needed. New SLA misses are also recorded in the database.

        Where assuming that the scheduler runs often, so we only check for
        tasks that should have succeeded in the past hour.
        """
        TI = models.TaskInstance
        sq = (
            session
            .query(
                TI.task_id,
                func.max(TI.execution_date).label('max_ti'))
            .filter(TI.dag_id == dag.dag_id)
            .filter(TI.state == State.SUCCESS)
            .filter(TI.task_id.in_(dag.task_ids))
            .group_by(TI.task_id).subquery('sq')
        )

        max_tis = session.query(TI).filter(
            TI.dag_id == dag.dag_id,
            TI.task_id == sq.c.task_id,
            TI.execution_date == sq.c.max_ti,
        ).all()

        ts = datetime.now()
        SlaMiss = models.SlaMiss
        for ti in max_tis:
            task = dag.get_task(ti.task_id)
            dttm = ti.execution_date
            if task.sla:
                dttm = dag.following_schedule(dttm)
                while dttm < datetime.now():
                    following_schedule = dag.following_schedule(dttm)
                    if following_schedule + task.sla < datetime.now():
                        session.merge(models.SlaMiss(
                            task_id=ti.task_id,
                            dag_id=ti.dag_id,
                            execution_date=dttm,
                            timestamp=ts))
                    dttm = dag.following_schedule(dttm)
        session.commit()

        slas = (
            session
            .query(SlaMiss)
            .filter(SlaMiss.email_sent == False or SlaMiss.notification_sent == False)
            .filter(SlaMiss.dag_id == dag.dag_id)
            .all()
        )

        if slas:
            sla_dates = [sla.execution_date for sla in slas]
            qry = (
                session
                .query(TI)
                .filter(TI.state != State.SUCCESS)
                .filter(TI.execution_date.in_(sla_dates))
                .filter(TI.dag_id == dag.dag_id)
                .all()
            )
            blocking_tis = []
            for ti in qry:
                if ti.task_id in dag.task_ids:
                    ti.task = dag.get_task(ti.task_id)
                    blocking_tis.append(ti)
                else:
                    session.delete(ti)
                    session.commit()

            blocking_tis = ([ti for ti in blocking_tis
                            if ti.are_dependencies_met(session=session)])
            task_list = "\n".join([
                sla.task_id + ' on ' + sla.execution_date.isoformat()
                for sla in slas])
            blocking_task_list = "\n".join([
                ti.task_id + ' on ' + ti.execution_date.isoformat()
                for ti in blocking_tis])
            # Track whether email or any alert notification sent
            # We consider email or the alert callback as notifications
            email_sent = False
            notification_sent = False
            if dag.sla_miss_callback:
                # Execute the alert callback
                self.logger.info(' --------------> ABOUT TO CALL SLA MISS CALL BACK ')
                dag.sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis)
                notification_sent = True
            email_content = """\
            Here's a list of tasks thas missed their SLAs:
            <pre><code>{task_list}\n<code></pre>
            Blocking tasks:
            <pre><code>{blocking_task_list}\n{bug}<code></pre>
            """.format(bug=asciiart.bug, **locals())
            emails = []
            for t in dag.tasks:
                if t.email:
                    if isinstance(t.email, basestring):
                        l = [t.email]
                    elif isinstance(t.email, (list, tuple)):
                        l = t.email
                    for email in l:
                        if email not in emails:
                            emails.append(email)
            if emails and len(slas):
                send_email(
                    emails,
                    "[airflow] SLA miss on DAG=" + dag.dag_id,
                    email_content)
                email_sent = True
                notification_sent = True
            # If we sent any notification, update the sla_miss table
            if notification_sent:
                for sla in slas:
                    if email_sent:
                        sla.email_sent = True
                    sla.notification_sent = True
                    session.merge(sla)
            session.commit()
            session.close()

class SchedulerJob(DagRunJob):

    __mapper_args__ = {
        'polymorphic_identity': 'SchedulerJob'
    }

    def __init__(
            self,
            dag_id=None,
            dag_ids=None,
            executor=None,
            heartrate=None,
            subdir=None,
            num_runs=None,
            test_mode=False,
            refresh_dags_every=10,
            do_pickle=False):

        if dag_id:
            # TODO left dag_id for compatibility, remove in Airflow 2.0
            warnings.warn(
                'Passing dag_id to Scheduler has been deprecated; Please pass '
                'dag_ids instead (accepts either a list or a string). Support '
                'for the dag_id argument will be dropped in Airflow 2.0. ',
                category=PendingDeprecationWarning)
            if dag_ids is None:
                dag_ids = []
            elif isinstance(dag_ids, six.string_types):
                dag_ids = [dag_ids]
            else:
                dag_ids = list(dag_ids)
            dag_ids.append(dag_id)

        if heartrate is None:
            heartrate = conf.getfloat('scheduler', 'JOB_HEARTBEAT_SEC')

        super(SchedulerJob, self).__init__(
            executor=executor,
            heartrate=heartrate,
            dag_ids=dag_ids,
            refresh_dags_every=refresh_dags_every,
            subdir=subdir,
            do_pickle=do_pickle)

        self.test_mode = test_mode

        if test_mode:
            self.num_runs = 1
        else:
            self.num_runs = num_runs

    def schedule_dagruns(self):
        runs = []
        paused_dag_ids = self.dagbag.paused_dags()
        for dag_id, dag in self.dagbag.dags.items():
            # don't schedule subdags or dags not specificly requested or paused dags
            if (
                    # dag is paused
                    dag_id in paused_dag_ids or
                    # dag is a subdag
                    dag.is_subdag or
                    # dag wasn't requested
                    (self.dag_ids and dag_id not in self.dag_ids)):
                continue

            try:
                runs.append(dag.schedule_dag())
            except Exception as e:
                self.logger.exception(e)
        self.submit_dagruns(runs)
        return runs

    @provide_session
    def collect_dagruns(self, session=None):
        """
        The Scheduler collects ALL DagRuns that aren't assigned to other jobs
        and haven't finished.
        """
        runs = (session.query(DagRun)
            .filter(
                or_(
                    DagRun.state == State.RUNNING,
                    DagRun.state == State.NONE),
                or_(
                    DagRun.lock_id == self.id,
                    DagRun.lock_id == None))
            .all())

        self.submit_dagruns(runs)
        return runs

    def _execute(self):

        pessimistic_connection_handling()

        self.logger.info("Starting the scheduler")
        self.heartbeat()

        self.executor.start()

        i = 0
        while not self.num_runs or self.num_runs > i:
            try:
                loop_start_dttm = datetime.now()
                self.logger.info('Starting scheduler loop...')
                try:
                    self.refresh_dags(
                        full_refresh=(i % self.refresh_dags_every == 0))
                    self.schedule_dagruns()
                    self.collect_dagruns()
                    self.process_dagruns()
                    self.manage_slas()

                except Exception as e:
                    self.logger.exception(e)

                self.logger.info('Done scheduling, calling heartbeat.')

                self.executor.heartbeat()
                self.heartbeat()
            except Exception as e:
                self.logger.exception(e)

            i += 1

        self.executor.end()


class BackfillJob(DagRunJob):

    __mapper_args__ = {
        'polymorphic_identity': 'BackfillJob'
    }

    def __init__(
            self,
            dag,
            start_date=None,
            end_date=None,
            mark_success=False,
            include_adhoc=False,
            donot_pickle=False,
            ignore_dependencies=False,
            ignore_first_depends_on_past=False,
            pool=None,
            *args, **kwargs):

        super(BackfillJob, self).__init__(
            mark_success=mark_success,
            ignore_dependencies=ignore_dependencies,
            dag_ids=dag.dag_id,
            pool=pool,
            *args, **kwargs)

        self.dag = dag
        start_date = start_date or dag.start_date
        if not start_date:
            raise ValueError('Could not infer start_date')
        end_date = end_date or dag.end_date or datetime.now()
        self.bf_start_date = start_date
        self.bf_end_date = end_date
        self.include_adhoc = include_adhoc
        self.donot_pickle = donot_pickle
        self.target_runs = []
        if ignore_first_depends_on_past:
            self.ignore_dict = {dag.dag_id: start_date}
        else:
            self.ignore_dict = {}

    @provide_session
    def get_progress(self, session=None):
        if self.target_runs:
            ti_states = [s for (s, ) in
                session.query(TI.state)
                    .filter(
                        TI.dag_id.in_([r.dag_id for r in self.target_runs]),
                        TI.execution_date.in_(
                            [r.execution_date for r in self.target_runs]))
                    .all()]
        else:
            ti_states = []

        p = {}
        p['total_dagruns'] = len(self.target_runs)
        # TIs might not have been created yet, so check the number of dag tasks
        dag = self.get_dag_from_dagrun(self.target_runs[0])
        p['total_tasks'] = len(dag.tasks) * len(self.target_runs)
        p['finished'] = len([s for s in ti_states if s in State.finished()])
        p['succeeded'] = len([s for s in ti_states if s == State.SUCCESS])
        p['failed'] = len([s for s in ti_states if s == State.FAILED])
        p['skipped'] = len([s for s in ti_states if s == State.SKIPPED])
        p['queued'] = len([s for s in ti_states if s == State.QUEUED])
        if p['total_tasks']:
            p['pct_complete'] = p['finished'] / float(p['total_tasks'])
        else:
            p['pct_complete'] = 0

        return p

    def _execute(self):

        progress = {}
        self.logger.info('Starting backfill from {} to {}'.format(
            self.bf_start_date,
            self.bf_end_date))

        self.heartbeat()
        self.executor.start()

        runs = [
            DagRun(dag_id=self.dag.dag_id, execution_date=dttm)
            for dttm in self.dag.date_range(
                start_date=self.bf_start_date, end_date=self.bf_end_date)]

        self.submit_dagruns(runs)
        self.target_runs = runs

        while self.dagruns:
            self.collect_dagruns()
            self.process_dagruns(ignore_depends_on_past_dates=self.ignore_dict)
            self.executor.heartbeat()
            self.heartbeat()

            progress = self.get_progress()
            self.logger.info(' | '.join([
                '[backfill progress: {pct_complete:.1%}]',
                'total dagruns: {total_dagruns}',
                'total tasks: {total_tasks}',
                'finished: {finished}',
                'succeeded: {succeeded}',
                'skipped: {skipped}',
                'failed: {failed}',
                ]).format(**progress))


        self.executor.end()

        self.logger.info('Backfill finished.')
