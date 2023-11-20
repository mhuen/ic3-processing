from icecube import dataclasses, icetray
import numpy as np

from ic3_processing.modules.pulses import pulse_modification_functions


class SLCPulseBugFix(icetray.I3ConditionalModule):
    """Module to fix SLC bug: shifts SLC pulses 25ns ahead in time."""

    def __init__(self, context):
        icetray.I3ConditionalModule.__init__(self, context)
        self.AddParameter(
            "PulseKey",
            "The name of the pulses that are to be modified.",
            "InIceDSTPulses",
        )
        self.AddParameter(
            "OutputKey",
            "The output key to which the modified pulses will be"
            " saved. If None: input pulses will be overridden.",
            None,
        )
        self.AddParameter(
            "RunOnPFrames",
            "True: run on P-frames; False: run on Q-Frames.",
            True,
        )

    def Configure(self):
        """Configure PulseModification."""
        self._pulse_key = self.GetParameter("PulseKey")
        self._output_key = self.GetParameter("OutputKey")
        if self._output_key is None:
            self._output_key = self._pulse_key
        self._run_on_pframe = self.GetParameter("RunOnPFrames")

    def Physics(self, frame):
        """Modifies pulses as specified in modification.

        Parameters
        ----------
        frame : I3Frame
            Current i3 frame.
        """
        if self._run_on_pframe:
            self.modify_pulses(frame)

        self.PushFrame(frame)

    def DAQ(self, frame):
        """Modifies pulses as specified in modification.

        Parameters
        ----------
        frame : I3Frame
            Current i3 frame.
        """
        if not self._run_on_pframe:
            self.modify_pulses(frame)

        self.PushFrame(frame)

    def modify_pulses(self, frame):
        """Modifies pulses as specified in modification.

        Parameters
        ----------
        frame : I3Frame
            Current i3 frame.
        """
        # get pulses
        pulses = frame[self._pulse_key]
        if isinstance(
            pulses, dataclasses.I3RecoPulseSeriesMapMask
        ) or isinstance(pulses, dataclasses.I3RecoPulseSeriesMapUnion):
            pulses = pulses.apply(frame)

        # make copy of pulses
        pulses = dataclasses.I3RecoPulseSeriesMap(pulses)

        # ----------------------------
        # apply modification to pulses
        # ----------------------------
        modified_pulses = dataclasses.I3RecoPulseSeriesMap()
        for key, dom_pulses in pulses.items():
            # collect pulse information
            times = []
            charges = []
            widths = []
            flags = []
            for pulse in dom_pulses:
                # check if SLC pulse
                if pulse.flags == 4:
                    times.append(pulse.time - 25 * icetray.I3Units.ns)
                else:
                    times.append(pulse.time)
                charges.append(pulse.charge)
                widths.append(pulse.width)
                flags.append(pulse.flags)

            charges = np.array(charges)
            times = np.array(times)
            widths = np.array(widths)
            flags = np.array(flags)

            # sort pulses in time
            sorted_indices = np.argsort(times)
            charges = charges[sorted_indices]
            times = times[sorted_indices]
            widths = widths[sorted_indices]
            flags = flags[sorted_indices]

            modified_dom_pulses = []

            for charge, time, flag, width in zip(
                charges, times, flags, widths
            ):
                # create pulse
                modified_pulse = dataclasses.I3RecoPulse()
                modified_pulse.charge = charge
                modified_pulse.time = time
                modified_pulse.flags = int(flag)
                modified_pulse.width = width

                # append pulse
                modified_dom_pulses.append(modified_pulse)

            # combine and fix potential time overlaps
            modified_pulses[key] = dataclasses.vector_I3RecoPulse(
                pulse_modification_functions.fix_time_overlap(
                    modified_dom_pulses
                )
            )
        # ----------------------------

        # write to frame
        # Note: time window might have to be adjusted as well!
        if self._output_key == self._pulse_key:
            del frame[self._output_key]
            frame[self._pulse_key] = modified_pulses
        else:
            frame[self._output_key] = modified_pulses
            frame[self._output_key + "TimeRange"] = dataclasses.I3TimeWindow(
                frame[self._pulse_key + "TimeRange"]
            )
