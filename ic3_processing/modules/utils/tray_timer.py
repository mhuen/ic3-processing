"""
Modules to time icetrays.

Adopted from:
Thomas Kintscher.
http://code.icecube.wisc.edu/projects/icecube/browser/IceCube/sandbox/
    kintscher/projects/toolbox/python/processing_time.py
"""

import timeit

import numpy as np
import scipy.optimize

from icecube import dataclasses, icetray
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


class TimerBase(icetray.I3ConditionalModule):
    """Base Module for timing of trays."""

    def __init__(self, context):
        icetray.I3ConditionalModule.__init__(self, context)
        self.AddParameter(
            "TimerName",
            "The name of the timer. This must be unique for a given I3Tray.",
        )
        self.AddParameter(
            "FrameStreams",
            "The frame streams (P, Q, ...) for which to measure the time.",
            ["Q", "P"],
        )
        self.AddParameter(
            "OutputKey",
            "The frame key under which the results will be written to.",
            "Duration",
        )

    def Configure(self):
        """Configure Module."""
        self._name = self.GetParameter("TimerName")
        self._output_key = self.GetParameter("OutputKey")
        self._streams = [
            icetray.I3Frame.Stream(stream)
            for stream in self.GetParameter("FrameStreams")
        ]


class TimerStart(TimerBase):
    """Module for timing of trays."""

    def Process(self):
        # get next frame
        frame = self.PopFrame()

        if frame.Stop in self._streams:
            # store current time in frame
            timer_name = "TimerStart_{}_".format(frame.Stop.id) + self._name
            if timer_name in frame:
                raise KeyError("Timer {} already exists!".format(timer_name))

            frame[timer_name] = dataclasses.I3Double(timeit.default_timer())

        # push frame
        self.PushFrame(frame)


class TimerStop(TimerBase):
    """Module for timing of trays."""

    def Process(self):
        # get next frame
        frame = self.PopFrame()

        if frame.Stop in self._streams:
            # store current time in frame
            timer_name = "TimerStart_{}_".format(frame.Stop.id) + self._name

            # evaluate elapsed time since TimerStart
            if frame.Has(timer_name):
                elapsed = timeit.default_timer() - frame[timer_name].value
                frame.Delete(timer_name)
            else:
                # in case the timer was never started, no time is measured
                elapsed = NaN

            # maintain dict of all timers
            output_key = self._output_key + frame.Stop.id
            if output_key in frame:
                duration = dataclasses.I3MapStringDouble(frame[output_key])
                del frame[output_key]
            else:
                duration = dataclasses.I3MapStringDouble()
            duration[self._name] = elapsed

            # divide later by normalization for crude estimate of SPTS runtime
            # (Not well tested! Don't trust it!)
            duration["Normalization"] = local_time / spts_time

            frame[output_key] = duration

        # push frame
        self.PushFrame(frame)
