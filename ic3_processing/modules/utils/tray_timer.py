"""
Script to time icetrays.
Credit goes to Thomas Kintscher.
Copied from:

http://code.icecube.wisc.edu/projects/icecube/browser/IceCube/sandbox/
    kintscher/projects/toolbox/python/processing_time.py
"""
import timeit

import numpy as np
import scipy.optimize

from icecube import dataclasses
from I3Tray import NaN


def benchmark(it=1000):
    # benchmark to normalize runtimes across different machines
    def complicated_function():
        return scipy.optimize.fmin(
            lambda x: np.sin(x) ** 2.0 + np.sqrt(x), x0=2.0, disp=0
        )

    # 1e3 was the number used for `it` at SPTS
    return timeit.timeit(complicated_function, number=it) * 1e3 / float(it)


# result of benchmark(it=1000) on SPTS
spts_time = 0.896
# run benchmark() on current system
local_time = benchmark()


def TimerStart(frame, timerName):
    # store current time in frame
    frame["TimerStart_" + timerName] = dataclasses.I3Double(
        timeit.default_timer()
    )


def TimerStop(frame, timerName):
    # evaluate elapsed time since TimerStart
    timerStartName = "TimerStart_" + timerName
    if frame.Has(timerStartName):
        elapsed = timeit.default_timer() - frame[timerStartName].value
        frame.Delete(timerStartName)
    else:
        # in case the timer was never started, no time is measured
        elapsed = NaN

    # maintain dict of all timers
    if "Duration" in frame:
        all_elapsed = frame["Duration"]
        frame.Delete("Duration")
    else:
        all_elapsed = {}
    all_elapsed[timerName] = elapsed

    # divide later by normalization for crude estimate of SPTS runtime
    # (Not well tested! Don't trust it!)
    all_elapsed["Normalization"] = local_time / spts_time

    frame.Put("Duration", dataclasses.I3MapStringDouble(all_elapsed))
