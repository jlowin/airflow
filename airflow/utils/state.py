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
from __future__ import unicode_literals

from builtins import object


class State(object):
    """
    Static class with task instance states constants and color method to
    avoid hardcoding.
    """
    NONE = None
    QUEUED = "queued"
    RUNNING = "running"
    SUCCESS = "success"
    SHUTDOWN = "shutdown"  # External request to shut down
    FAILED = "failed"
    UP_FOR_RETRY = "up_for_retry"
    UPSTREAM_FAILED = "upstream_failed"
    SKIPPED = "skipped"

    state_color = {
        QUEUED: 'gray',
        RUNNING: 'lime',
        SUCCESS: 'green',
        SHUTDOWN: 'blue',
        FAILED: 'red',
        UP_FOR_RETRY: 'gold',
        UPSTREAM_FAILED: 'orange',
        SKIPPED: 'pink',
    }

    @classmethod
    def color(cls, state):
        if state in cls.state_color:
            return cls.state_color[state]
        else:
            return 'lightgray'

    @classmethod
    def color_fg(cls, state):
        color = cls.color(state)
        if color in ['green', 'red']:
            return 'white'
        else:
            return 'black'

    @classmethod
    def runnable(cls):
        return set([
            cls.NONE,
            cls.FAILED,
            cls.UP_FOR_RETRY,
            cls.UPSTREAM_FAILED,
            cls.SKIPPED,
            cls.QUEUED
        ])

    @classmethod
    def finished(cls):
        """
        A list of states indicating that a task started and completed a
        run attempt. Note that the attempt could have resulted in failure or
        have been interrupted; in any case, it is no longer running.
        """
        return set([
            cls.SUCCESS,
            cls.SHUTDOWN,
            cls.FAILED,
            cls.SKIPPED,
        ])

    @classmethod
    def unfinished(cls):
        """
        A list of states indicating that a task either has not completed
        a run or has not even started.
        """
        return set([
            cls.NONE,
            cls.QUEUED,
            cls.RUNNING,
            cls.UP_FOR_RETRY
        ])
