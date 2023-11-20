from icecube import dataclasses, icetray
import numpy as np

from ic3_processing.modules.pulses import pulse_modification_functions


class PulseModification(icetray.I3ConditionalModule):
    """Module to modify pulses for robustness tests."""

    def __init__(self, context):
        icetray.I3ConditionalModule.__init__(self, context)
        self.AddParameter(
            "PulseKey",
            "The name of the pulses that are to be modified.",
            "InIcePulses",
        )
        self.AddParameter(
            "Modification",
            "The name of the modification. This must be a name "
            "of one of the functions defined in this file.",
        )
        self.AddParameter(
            "ModificationSettings",
            "Optional arguments that can be passed on to pulse"
            " modification method",
            {},
        )
        self.AddParameter(
            "RandomSeed", "Numpy random seed to set in Configure.", 1337
        )

    def Configure(self):
        """Configure PulseModification."""
        self._pulse_key = self.GetParameter("PulseKey")
        self._modification = self.GetParameter("Modification")
        self._modification_settings = self.GetParameter("ModificationSettings")
        self._seed = self.GetParameter("RandomSeed")

        self._random_generator = np.random.RandomState(self._seed)

    def Calibration(self, frame):
        """Collect the DOM noise rates.

        Parameters
        ----------
        frame : I3Frame
            Current i3 frame.
        """
        self._dom_noise_rate = {}
        for omkey, calib in frame["I3Calibration"].dom_cal.items():
            if (
                omkey.om < 61
                and omkey.om > 0
                and omkey.string > 0
                and omkey.string < 87
            ):
                self._dom_noise_rate[omkey] = calib.dom_noise_rate

        # push frame
        self.PushFrame(frame)

    def Physics(self, frame):
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

        # get modification function
        modification_func = getattr(
            pulse_modification_functions, self._modification
        )

        # apply modification to pulses
        modified_pulses = modification_func(
            self,
            pulses,
            dom_noise_rate_dict=self._dom_noise_rate,
            frame=frame,
            **self._modification_settings,
        )

        # write to frame
        frame[self._pulse_key + "_mod"] = modified_pulses
        frame[self._pulse_key + "_modTimeRange"] = dataclasses.I3TimeWindow(
            frame[self._pulse_key + "TimeRange"]
        )

        # push frame
        self.PushFrame(frame)
