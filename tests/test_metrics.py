"""
Most applications use some sort of metrics that need to be created
and managed

Metrics are Stateless and fire and forget

Some Metrics are Not Stateless and need to be incremented or decremented

They also should be easily created in one place and propagated to another part of the
application
"""

import pytest


class Metric(object):
    pass

