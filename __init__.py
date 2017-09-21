import threading
from queue import Queue, PriorityQueue
from functools import partial, update_wrapper
import datetime
import time
import os
import logging

__all__ = ['Scheduler', 'Task']


class Task:
    __slots__ = (
        'periodic', 'next_run', 'last_run', 'priority', 'interval',
        'unit', 'at_time', 'start_day', 'period', 'job_func', 'status', 'result')

    def __init__(self):
        self.job_func = None
        self.periodic = None
        self.next_run = None
        self.last_run = None
        self.priority = None
        self.interval = None
        self.period = None
        self.unit = None
        self.at_time = None
        self.start_day = None
        self.status = None
        self.result = None

    def __eq__(s, o):
        return (s.next_run, s.priority) == (o.next_run, o.priority)

    def __lt__(s, o):
        return (s.next_run, s.priority) < (o.next_run, o.priority)

    def __le__(s, o):
        return (s.next_run, s.priority) <= (o.next_run, o.priority)

    def __gt__(s, o):
        return (s.next_run, s.priority) > (o.next_run, o.priority)

    def __ge__(s, o):
        return (s.next_run, s.priority) >= (o.next_run, o.priority)

    def __repr__(self):
        def format_time(t):
            return t.strftime('%Y-%m-%d %H:%M:%S') if t else '[never]'

        timestats = '(last run: %s, next run: %s)' % (
            format_time(self.last_run), format_time(self.next_run))

        if hasattr(self.job_func, '__name__'):
            job_func_name = self.job_func.__name__
        else:
            job_func_name = repr(self.job_func)
        args = [repr(x) for x in self.job_func.args]
        kwargs = ['%s=%s' % (k, repr(v))
                  for k, v in self.job_func.keywords.items()]
        call_repr = job_func_name + '(' + ', '.join(args + kwargs) + ')'

        if self.at_time is not None:
            return 'Every %s %s at %s do %s %s priority: %s' % (
                self.interval,
                self.unit[:-1] if self.interval == 1 else self.unit,
                self.at_time, call_repr, timestats, self.priority)
        else:
            return 'Every %s %s do %s %s priority: %s' % (
                self.interval,
                self.unit[:-1] if self.interval == 1 else self.unit,
                call_repr, timestats, self.priority)

    def set_priority(self, priority=1):
        self.priority = priority
        return self

    def once(self, interval=1):
        self.periodic = False
        self.interval = interval
        return self

    def every(self, interval=1):
        self.interval = interval
        self.periodic = True
        return self

    def at(self, time_str):
        """
        Schedule the job every day at a specific time.
        Calling this is only valid for jobs scheduled to run
        every N day(s).
        :param time_str: A string in `XX:YY` format.
        :return: The invoked job instance
        """
        assert self.unit in ('days', 'hours') or self.start_day
        hour, minute = time_str.split(':')
        minute = int(minute)
        if self.unit == 'days' or self.start_day:
            hour = int(hour)
            assert 0 <= hour <= 23
        elif self.unit == 'hours':
            hour = 0
        assert 0 <= minute <= 59
        self.at_time = datetime.time(hour, minute)
        return self

    @property
    def second(self):
        assert self.interval == 1, 'Use seconds instead of second'
        return self.seconds

    @property
    def seconds(self):
        self.unit = 'seconds'
        return self

    @property
    def minute(self):
        assert self.interval == 1, 'Use minutes instead of minute'
        return self.minutes

    @property
    def minutes(self):
        self.unit = 'minutes'
        return self

    @property
    def hour(self):
        assert self.interval == 1, 'Use hours instead of hour'
        return self.hours

    @property
    def hours(self):
        self.unit = 'hours'
        return self

    @property
    def day(self):
        assert self.interval == 1, 'Use days instead of day'
        return self.days

    @property
    def days(self):
        self.unit = 'days'
        return self

    @property
    def week(self):
        assert self.interval == 1, 'Use weeks instead of week'
        return self.weeks

    @property
    def weeks(self):
        self.unit = 'weeks'
        return self

    @property
    def monday(self):
        assert self.interval == 1, 'Use mondays instead of monday'
        self.start_day = 'monday'
        return self.weeks

    @property
    def tuesday(self):
        assert self.interval == 1, 'Use tuesdays instead of tuesday'
        self.start_day = 'tuesday'
        return self.weeks

    @property
    def wednesday(self):
        assert self.interval == 1, 'Use wedesdays instead of wednesday'
        self.start_day = 'wednesday'
        return self.weeks

    @property
    def thursday(self):
        assert self.interval == 1, 'Use thursday instead of thursday'
        self.start_day = 'thursday'
        return self.weeks

    @property
    def friday(self):
        assert self.interval == 1, 'Use fridays instead of friday'
        self.start_day = 'friday'
        return self.weeks

    @property
    def saturday(self):
        assert self.interval == 1, 'Use saturdays instead of saturday'
        self.start_day = 'saturday'
        return self.weeks

    @property
    def sunday(self):
        assert self.interval == 1, 'Use sundays instead of sunday'
        self.start_day = 'sunday'
        return self.weeks

    def _schedule_next_run(self):
        """
        Compute the instant when this job should run next.
        """
        assert self.unit in ('seconds', 'minutes', 'hours', 'days', 'weeks')
        self.period = datetime.timedelta(**{self.unit: self.interval})
        self.next_run = datetime.datetime.now() + self.period
        if self.start_day is not None:
            assert self.unit == 'weeks'
            weekdays = (
                'monday',
                'tuesday',
                'wednesday',
                'thursday',
                'friday',
                'saturday',
                'sunday'
            )
            assert self.start_day in weekdays
            weekday = weekdays.index(self.start_day)
            days_ahead = weekday - self.next_run.weekday()
            if days_ahead <= 0:  # Target day already happened this week
                days_ahead += 7
            self.next_run += datetime.timedelta(days_ahead) - self.period
        if self.at_time is not None:
            assert self.unit in ('days', 'hours') or self.start_day is not None
            kwargs = {
                'minute': self.at_time.minute,
                'second': self.at_time.second,
                'microsecond': 0
            }
            if self.unit == 'days' or self.start_day is not None:
                kwargs['hour'] = self.at_time.hour
            self.next_run = self.next_run.replace(**kwargs)
            # If we are running for the first time, make sure we run
            # at the specified time *today* (or *this hour*) as well
            if not self.last_run:
                now = datetime.datetime.now()
                if (self.unit == 'days' and self.at_time > now.time() and
                            self.interval == 1):
                    self.next_run = self.next_run - datetime.timedelta(days=1)
                elif self.unit == 'hours' and self.at_time.minute > now.minute:
                    self.next_run = self.next_run - datetime.timedelta(hours=1)
        if self.start_day is not None and self.at_time is not None:
            # Let's see if we will still make that time we specified today
            if (self.next_run - datetime.datetime.now()).days >= 7:
                self.next_run -= self.period

    def do(self, job_func, *args, **kwargs):
        """
        Specifies the job_func that should be called every time the
        job runs.
        Any additional arguments are passed on to job_func when
        the job runs.
        :param job_func: The function to be scheduled
        :return: The invoked job instance
        """
        self.job_func = partial(job_func, *args, **kwargs)
        try:
            update_wrapper(self.job_func, job_func)
        except AttributeError:
            # job_funcs already wrapped by functools.partial won't have
            # __name__, __module__ or __doc__ and the update_wrapper()
            # call will fail.
            pass
        self._schedule_next_run()
        return self


class Worker(threading.Thread):
    """ Thread executing tasks from a given tasks queue """

    def __init__(self, tasks):
        super().__init__()
        self.tasks = tasks
        self.daemon = True
        self.start()

    def run(self):
        while True:
            task = self.tasks.get()
            func = task.job_func
            try:
                task.result = func()
                task.status = True
                logging.info('{} completed.'.format(self.__repr__()))
            except Exception as e:
                # An exception happened in this thread
                logging.error('{} at \n {}'.format(e, self.__repr__()))
                task.result = e
                task.status = False
            finally:
                # Mark this task as done, whether an exception happened or not
                self.tasks.task_done()


class ThreadPool:
    """ Pool of threads consuming tasks from a queue """

    def __init__(self, num_threads):
        self.tasks = Queue(num_threads)
        for _ in range(num_threads):
            Worker(self.tasks)

    def add_func(self, func):
        """ Add a task to the queue """
        self.tasks.put(func)

    def wait_completion(self):
        """ Wait for completion of all the tasks in the queue """
        self.tasks.join()


class Scheduler(threading.Thread):
    def __init__(self, max_workers=None, timefunc=datetime.datetime.now, delayfunc=time.sleep):
        super().__init__()
        self._queue = PriorityQueue()
        self._lock = threading.RLock()
        self.timefunc = timefunc
        self.delayfunc = delayfunc
        self.daemon = True
        self._active = True
        if max_workers is None:
            # Use this number because ThreadPoolExecutor is often
            # used to overlap I/O instead of CPU work.
            max_workers = (os.cpu_count() or 1) * 5
        if max_workers <= 0:
            raise ValueError("max_workers must be greater than 0")
        self.pool = ThreadPool(max_workers)

    def add_task(self, task):
        assert isinstance(task, Task)
        with self._lock:
            self._queue.put(task)
        logging.info('{} added.'.format(task.__repr__()))

    def cancel_task(self, task):
        with self._lock:
            try:
                self._queue.queue.remove(task)
                logging.info('{} canceled.'.format(task.__repr__()))
            except ValueError:
                logging.warning('{} not found.'.format(task.__repr__()))

    def active_tasks(self):
        return self._queue.queue[:]

    def clear(self):
        with self._lock:
            self._queue.queue = []
            logging.info('Scheduler queue is cleaned.')

    def stop(self):
        self._active = False
        logging.info('Scheduler stopped.')

    def run(self):
        logging.info('Scheduler started.')
        lock = self._lock
        q = self._queue
        delayfunc = self.delayfunc
        timefunc = self.timefunc
        pool = self.pool
        while self._active:
            if not q.empty():
                time = q.queue[0].next_run
                now = timefunc()
                if time > now:
                    delayfunc(1)
                else:
                    with lock:
                        task = q.get()
                        if task.periodic:
                            task._schedule_next_run()
                            q.put(task)
                    pool.add_func(task)

            else:
                delayfunc(1)
