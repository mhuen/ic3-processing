# -*- coding: utf-8 -*-
"""Helper functions to mask pulses
"""
import numpy as np
from icecube import dataclasses


def combine_pulses(frame, pulse_key, output_name, time_diff=2.0):
    """Combine pulses close in time

    This can be used to minimize disk usage and also reduce the required
    time of likelihood-based reconstruction methods that iterate over
    individual pulses.

    Parameters
    ----------
    frame : I3Frame
        The current I3Frame with the pulse series to combine.
    pulse_key : str
        The pulse series for which to combine pulses.
    output_name : str
        The desired output name to which the new pulse series
        with the combined pulses will be written to.
    time_diff : float, optional
        The maximum time difference for which two pulses are combined.
        Pulses with a time difference below this value are combined to a
        single pulse. This pulse will have the total charge of both
        pulses and the time of the first pulse.
    """
    pulses = frame[pulse_key]

    if isinstance(pulses, dataclasses.I3RecoPulseSeriesMapMask) or isinstance(
        pulses, dataclasses.I3RecoPulseSeriesMapUnion
    ):
        pulses = pulses.apply(frame)

    pulse_map = dataclasses.I3RecoPulseSeriesMap()

    for om_key, series in pulses.items():
        previous_times = []
        previous_charges = []
        previous_pulse = None

        pulse_series = dataclasses.I3RecoPulseSeries()
        # walk through pulses and check if previous pulse is within defined
        # maximal time difference
        for pulse in series:
            if previous_pulse is None:
                previous_pulse = dataclasses.I3RecoPulse(pulse)
                previous_times.append(pulse.time)
                previous_charges.append(pulse.charge)
            else:
                # check if this pulse needs to be combined
                if pulse.time - previous_pulse.time > time_diff:
                    # does not need to be combined, so add previous pulse
                    # adjust time to average of combined pulses
                    combined_pulse = dataclasses.I3RecoPulse(previous_pulse)
                    combined_pulse.time = np.average(
                        previous_times, weights=previous_charges
                    )
                    combined_pulse.charge = np.sum(previous_charges)
                    pulse_series.append(combined_pulse)

                    # start combining next pulses
                    previous_pulse = pulse
                    previous_times = [pulse.time]
                    previous_charges = [pulse.charge]
                else:
                    # needs to be combined
                    previous_times.append(pulse.time)
                    previous_charges.append(pulse.charge)

        # add last pulse
        if previous_pulse is not None:
            combined_pulse = dataclasses.I3RecoPulse(previous_pulse)
            combined_pulse.time = np.average(
                previous_times, weights=previous_charges
            )
            combined_pulse.charge = np.sum(previous_charges)
            pulse_series.append(combined_pulse)

        pulse_map[om_key] = pulse_series

    frame[output_name] = pulse_map
