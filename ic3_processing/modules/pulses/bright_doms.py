# -*- coding: utf-8 -*-
from icecube import dataclasses, icetray
import numpy as np


class AddBrightDOMs(icetray.I3ConditionalModule):
    """Module to add BrightDOMs for a given pulse series."""

    def __init__(self, context):
        icetray.I3ConditionalModule.__init__(self, context)
        self.AddParameter(
            "PulseKey",
            "Name of the pulses for which to add BrightDOMs.",
            "InIceDSTPulses",
        )
        self.AddParameter(
            "BrightThreshold",
            "The threshold above which a DOM is considered as a "
            "bright DOM. A bright DOM is a DOM that has a "
            "charge that is above threshold * average charge.",
            10,
        )
        self.AddParameter(
            "OutputKey",
            "The key to which to save the BrightDOMs.",
            "BrightDOMs",
        )

    def Configure(self):
        """Configure AddBrightDOMs module."""
        self._pulse_key = self.GetParameter("PulseKey")
        self._threshold = self.GetParameter("BrightThreshold")
        self._output_key = self.GetParameter("OutputKey")

    def Physics(self, frame):
        """Adds the BrightDOMs to the frame.

        Parameters
        ----------
        frame : I3Frame
            Current pyhsics I3Frame.
        """

        # get pulses
        pulse_series = frame[self._pulse_key]
        if isinstance(
            pulse_series, dataclasses.I3RecoPulseSeriesMapMask
        ) or isinstance(pulse_series, dataclasses.I3RecoPulseSeriesMapUnion):
            pulse_series = pulse_series.apply(frame)

        dom_charges = {}
        dom_charges_list = []

        # compute charge for each DOM
        for omkey, pulses in pulse_series:
            dom_charge = np.sum([p.charge for p in pulses])
            dom_charges_list.append(dom_charge)
            dom_charges[omkey] = dom_charge

        average_charge = np.mean(dom_charges_list)
        bright_doms = dataclasses.I3VectorOMKey()

        for omkey, dom_charge in dom_charges.items():
            if dom_charge > self._threshold * average_charge:
                bright_doms.append(omkey)

        # write to frame
        frame[self._output_key] = bright_doms

        # push frame
        self.PushFrame(frame)


def get_bright_pulses(
    frame,
    pulse_key,
    bright_doms_key="BrightDOMs",
    output_name="BrightPulses",
):
    """Get pulses belonging to `BrightDOMs`.

    Parameters
    ----------
    frame : I3Frame
        The current I3Frame.
    pulse_key : str
        The name of the pulse series from which to collect the bright pulses.
    bright_doms_key : str, optional
        The name of the frame key that contains the list of DOM keys that
        are considered to be bright DOMs.
    output_name : str, optional
        The output name to which the pulse series containing the bright
        pulses are written to.
    """
    if bright_doms_key in frame:
        pulses = frame[pulse_key]

        if isinstance(
            pulses, dataclasses.I3RecoPulseSeriesMapMask
        ) or isinstance(pulses, dataclasses.I3RecoPulseSeriesMapUnion):
            pulses = pulses.apply(frame)

        pulses = dict(pulses)

        bright_pulses = {}

        for om_key in frame[bright_doms_key]:
            if om_key in pulses:
                bright_pulses[om_key] = pulses[om_key]

        if bright_pulses != {}:
            bright_pulses = dataclasses.I3RecoPulseSeriesMap(bright_pulses)
            frame[output_name] = bright_pulses
