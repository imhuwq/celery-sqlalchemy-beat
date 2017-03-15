import json
import datetime
from enum import Enum

from sqlalchemy.orm import relationship, backref
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, ForeignKey

import celery.schedules

Model = declarative_base()


class PERIODS(Enum):
    DAYS = 'days'
    HOURS = 'hours'
    MINUTES = 'minutes'
    SECONDS = 'seconds'
    MICROSECONDS = 'microseconds'


ValidationError = type("ValidationError", (BaseException,), {})


class Interval(Model):
    __tablename__ = "interval"

    id = Column(Integer, primary_key=True)
    _every = Column("every", Integer, default=0, nullable=False)
    _period = Column("period", String)

    @property
    def every(self):
        return self._every

    @every.setter
    def every(self, value):
        value = int(value)
        self._every = max(value, 0)

    @property
    def period(self):
        return PERIODS(self._period)

    @period.setter
    def period(self, value):
        if isinstance(value, PERIODS):
            self._period = value.value
        else:
            self._period = PERIODS(value).value

    @property
    def schedule(self):
        return celery.schedules.schedule(datetime.timedelta(**{self.period.value: self.every}))

    @property
    def period_singular(self):
        return self.period[:-1]


class Crontab(Model):
    __tablename__ = "crontab"

    id = Column(Integer, primary_key=True)
    minute = Column(String, default='*', nullable=False)
    hour = Column(String, default='*', nullable=False)
    day_of_week = Column(String, default='*', nullable=False)
    day_of_month = Column(String, default='*', nullable=False)
    month_of_year = Column(String, default='*', nullable=False)

    @property
    def schedule(self):
        return celery.schedules.crontab(minute=self.minute,
                                        hour=self.hour,
                                        day_of_week=self.day_of_week,
                                        day_of_month=self.day_of_month,
                                        month_of_year=self.month_of_year)


class PeriodicTask(Model):
    __tablename__ = "periodic_task"

    id = Column(Integer, primary_key=True)

    name = Column(String, unique=True)
    task = Column(String, nullable=False)

    _args = Column(Text)  # list
    _kwargs = Column(Text)  # dict

    queue = Column(String)
    exchange = Column(String)
    routing_key = Column(String)
    soft_time_limit = Column(Integer)

    expires = Column(DateTime)
    start_after = Column(DateTime)
    enabled = Column(Boolean, default=False)

    last_run_at = Column(DateTime)

    _total_run_count = Column(Integer, default=0)
    _max_run_count = Column(Integer, default=0)

    date_changed = Column(DateTime)
    description = Column(String)

    run_immediately = Column(Boolean)

    interval_id = Column(Integer, ForeignKey('interval.id'))
    interval = relationship("Interval", backref=backref('task', uselist=False))

    crontab_id = Column(Integer, ForeignKey('crontab.id'))
    crontab = relationship("Crontab", backref=backref('task', uselist=False))

    no_changes = False

    @property
    def args(self):
        ret = json.loads(self._args)
        if isinstance(ret, list):
            return ret
        return []

    @args.setter
    def args(self, value):
        if isinstance(value, str):
            value = json.loads(str)
        if isinstance(value, list):
            self._args = json.dumps(value)
        else:
            raise ValueError("args should be type of list")

    @property
    def kwargs(self):
        return self._kwargs

    @kwargs.setter
    def kwargs(self, value):
        if isinstance(value, str):
            value = json.loads(str)
        if isinstance(value, dict):
            self._kwargs = json.dumps(value)
        else:
            raise ValueError("kwargs should be type of dict")

    @property
    def total_run_count(self):
        return self._total_run_count

    @total_run_count.setter
    def total_run_count(self, value):
        self._total_run_count = max(value, 0)

    @property
    def max_run_count(self):
        return self._max_run_count

    @max_run_count.setter
    def max_run_count(self, value):
        self._max_run_count = max(value, 0)

    @property
    def schedule(self):
        if self.interval:
            return self.interval.schedule
        elif self.crontab:
            return self.crontab.schedule
        else:
            raise Exception("must define interval or crontab schedule")
