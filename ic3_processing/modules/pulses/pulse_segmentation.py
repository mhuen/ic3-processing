from icecube import dataclasses, icetray, simclasses
from ic3_labels.labels.utils import general

from ic3_processing.modules.processing.filter_events import filter_events


def convert_mcpe_to_reco_map(mcpe_map):
    """Convert an I3MCPESeriesMap to an I3RecoPulseSeriesMap

    Parameters
    ----------
    mcpe_map : I3MCPESeriesMap
        The I3MCPESeriesMap to convert.

    Returns
    -------
    I3RecoPulseSeriesMap
        A pseudo reco pulse series map containing the information
        of the original photon map.
    """
    reco_map = dataclasses.I3RecoPulseSeriesMap()

    for key, mcpes in mcpe_map.items():
        reco_map[key] = []
        for mcpe in mcpes:
            pulse = dataclasses.I3RecoPulse()
            pulse.time = mcpe.time
            pulse.charge = mcpe.npe
            reco_map[key].append(pulse)

    return reco_map


def count_pe(mcpe_map):
    """Count number of photons in map

    Parameters
    ----------
    mcpe_map : I3MCPESeriesMap
        The I3MCPESeriesMap to count photons for.

    Returns
    -------
    float
        The number of photons,
    """
    npe = 0.0
    for key, mcpes in mcpe_map.items():
        for mcpe in mcpes:
            npe += mcpe.npe
    return npe


class PulseSegmentationMapping(icetray.I3ConditionalModule):
    """Module to add and investigate pulse segmentation.

    This module segments a pulse series into components relating to each of
    the primaries in the I3MCTree and noise pulses.
    """

    def __init__(self, context):
        icetray.I3ConditionalModule.__init__(self, context)
        self.AddParameter(
            "PulseMapString",
            "The name of the pulses to segment.",
            "InIceDSTPulses",
        )
        self.AddParameter(
            "MCPESeriesMapString",
            "The name of the MC photon series map to use.",
            "I3MCPESeriesMap",
        )
        self.AddParameter(
            "OutputBase",
            "The output key to which the modified pulses will be"
            " saved. If None: input pulses will be overridden.",
            "PulseSegmentation",
        )
        self.AddParameter(
            "MaxTimeDif",
            "The maximum time difference in ns to allow for "
            "the matching of reco pulses to underlying MC "
            "photons.",
            10,
        )
        self.AddParameter(
            "MCTreeKey", "The name of the I3MCTree frame key.", "I3MCTree"
        )
        self.AddParameter(
            "RunOnPFrames",
            "True: run on P-frames; False: run on Q-Frames.",
            True,
        )
        self.AddParameter(
            "ConvertPulseMaps",
            "True: add pseudo reco pulse series to frame.",
            True,
        )
        self.AddParameter(
            "FilterEventsList", "If given, events will be filtered", []
        )

    def Configure(self):
        """Configure PulseModification."""
        self._pulse_map_string = self.GetParameter("PulseMapString")
        self._mcpe_series_map_name = self.GetParameter("MCPESeriesMapString")
        self._output_base = self.GetParameter("OutputBase")
        self._max_time_dif = self.GetParameter("MaxTimeDif")
        self._mc_tree_key = self.GetParameter("MCTreeKey")
        self._run_on_pframe = self.GetParameter("RunOnPFrames")
        self._convert_pulse_maps = self.GetParameter("ConvertPulseMaps")
        self._filter_events_list = self.GetParameter("FilterEventsList")

    def Physics(self, frame):
        """Segment pulses.

        Parameters
        ----------
        frame : I3Frame
            Current i3 frame.
        """
        if self._run_on_pframe:
            self.segment_pulses(frame)

        # filter events?
        if self._run_on_pframe:
            if filter_events(frame, filter_list=self._filter_events_list):
                self.PushFrame(frame)
        else:
            self.PushFrame(frame)

    def DAQ(self, frame):
        """Segment pulses.

        Parameters
        ----------
        frame : I3Frame
            Current i3 frame.
        """
        if not self._run_on_pframe:
            self.segment_pulses(frame)

        # filter events?
        if not self._run_on_pframe:
            if filter_events(frame, filter_list=self._filter_events_list):
                self.PushFrame(frame)
        else:
            self.PushFrame(frame)

    def segment_pulses(self, frame):
        """Segment pulses.

        Parameters
        ----------
        frame : I3Frame
            Current i3 frame.
        """

        primaries = frame[self._mc_tree_key].get_primaries()

        labels = dataclasses.I3MapStringDouble()

        labels["n_primaries"] = len(primaries)
        labels["n_pe"] = count_pe(frame[self._mcpe_series_map_name])

        if self._convert_pulse_maps:
            pulse_map = frame[self._mcpe_series_map_name]
            if isinstance(pulse_map, simclasses.I3MCPESeriesMap):
                out_key = self._mcpe_series_map_name + "_converted"
                frame[out_key] = convert_mcpe_to_reco_map(pulse_map)

        # add pulse map for each of the primary particles
        for i, primary in enumerate(primaries):
            pulse_map = general.get_pulse_map(
                frame=frame,
                particle=primary,
                pulse_map_string=self._pulse_map_string,
                mcpe_series_map_name=self._mcpe_series_map_name,
                max_time_dif=self._max_time_dif,
            )
            out_key = self._output_base + "_primary_{:03d}".format(i)
            frame[out_key] = pulse_map
            if self._convert_pulse_maps:
                frame[out_key + "_converted"] = convert_mcpe_to_reco_map(
                    pulse_map
                )

        # add noise pulses
        pulse_map = general.get_noise_pulse_map(
            frame=frame,
            pulse_map_string=self._pulse_map_string,
            mcpe_series_map_name=self._mcpe_series_map_name,
            max_time_dif=self._max_time_dif,
        )
        out_key = self._output_base + "_noise"
        frame[out_key] = pulse_map
        if self._convert_pulse_maps:
            frame[out_key + "_converted"] = convert_mcpe_to_reco_map(pulse_map)

        # add mapping from pulse -> particle
        mapping = general.get_pulse_primary_mapping(
            frame=frame,
            primaries=primaries,
            pulse_map_string=self._pulse_map_string,
            mcpe_series_map_name=self._mcpe_series_map_name,
        )
        frame[self._output_base + "_mapping"] = mapping

        # save out labels
        frame[self._output_base + "_labels"] = labels
