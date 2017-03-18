import time
import datetime
import traceback

from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

from celery import current_app
from celery.utils.log import get_logger
from celery.beat import Scheduler, ScheduleEntry, event_t, heapq

from celerysqlalchemybeat.beat.model import PeriodicTask


class SAScheduleEntry(ScheduleEntry):
    def __init__(self, task):
        self._task = task

        self.app = current_app._get_current_object()
        self.name = self._task.name
        self.task = self._task.task

        self.schedule = self._task.schedule

        self.args = self._task.args
        self.kwargs = self._task.kwargs
        self.options = {
            'queue': self._task.queue,
            'exchange': self._task.exchange,
            'routing_key': self._task.routing_key,
            'expires': self._task.expires,
            'soft_time_limit': self._task.soft_time_limit
        }
        if self._task.total_run_count is None:
            self._task.total_run_count = 0
        self.total_run_count = self._task.total_run_count

        if not self._task.last_run_at:
            self._task.last_run_at = self._default_now()
        self.last_run_at = self._task.last_run_at

    def _default_now(self):
        return self.app.now()

    def next(self):
        self._task.last_run_at = self.app.now()
        self._task.total_run_count += 1
        self._task.run_immediately = False
        return self.__class__(self._task)

    __next__ = next

    def is_due(self):
        if hasattr(self._task, 'start_after') and self._task.start_after:
            if datetime.datetime.now() < self._task.start_after:
                return False, 5.0
        if hasattr(self._task, 'max_run_count') and self._task.max_run_count:
            if (self._task.total_run_count or 0) >= self._task.max_run_count:
                return False, 5.0
        if self._task.run_immediately:
            _, n = self.schedule.is_due(self.last_run_at)
            return True, n
        return self.schedule.is_due(self.last_run_at)

    def __repr__(self):
        return (u'<{0} ({1} {2}(*{3}, **{4}) {{5}})>'.format(
            self.__class__.__name__,
            self.name, self.task, self.args,
            self.kwargs, self.schedule,
        ))


class SAScheduler(Scheduler):
    UPDATE_INTERVAL = datetime.timedelta(seconds=5)

    Entry = SAScheduleEntry

    Model = PeriodicTask

    def __init__(self, *args, **kwargs):
        self.db_name = "celery"

        self.db_uri = "sqlite:///celery.sqlite"
        self.db_engine = create_engine(self.db_uri)
        self.session = scoped_session(sessionmaker(bind=self.db_engine))

        self.query = self.session.query

        self._schedule = {}
        self._last_updated = None
        Scheduler.__init__(self, *args, **kwargs)
        self.max_interval = (kwargs.get('max_interval')
                             or self.app.conf.CELERYBEAT_MAX_LOOP_INTERVAL or 5)

    def tick(self, event_t=event_t, min=min,
             heappop=heapq.heappop, heappush=heapq.heappush,
             heapify=heapq.heapify, mktime=time.mktime):

        adjust = self.adjust
        interval = self.max_interval

        for entry in self.schedule.values():
            is_due, next_time_to_run = self.is_due(entry)
            if is_due:
                self.apply_entry(entry, producer=self.producer)
            interval = min(adjust(next_time_to_run), interval)
        return interval

    def setup_schedule(self):
        pass

    def requires_update(self):
        if not self._last_updated:
            return True
        return self._last_updated + self.UPDATE_INTERVAL < datetime.datetime.now()

    @property
    def objects(self):
        objs = self.query(self.Model).filter_by(enabled=True).all()
        return objs

    def get_from_database(self):
        self.sync()
        records = {}
        for obj in self.objects:
            records[obj.name] = self.Entry(obj)
        return records

    @property
    def schedule(self):
        if self.requires_update():
            self._schedule = self.get_from_database()
            self._last_updated = datetime.datetime.now()
        return self._schedule

    def sync(self):
        for entry in self._schedule.values():
            try:
                if entry.total_run_count > entry._task.total_run_count:
                    entry._task.total_run_count = entry.total_run_count
                if entry.last_run_at and entry._task.last_run_at and entry.last_run_at > entry._task.last_run_at:
                    entry._task.last_run_at = entry.last_run_at
                entry._task.run_immediately = False
                self.session.commit()
            except Exception:
                get_logger(__name__).error(traceback.format_exc())
