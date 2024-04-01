"""Helper functions to filter events and streams
"""

import numpy as np
from collections import deque

from icecube import icetray, dataclasses


class I3OrphanFrameDropper(icetray.I3ConditionalModule):
    """Module to drop orphan frames."""

    def __init__(self, context):
        icetray.I3ConditionalModule.__init__(self, context)
        self.AddParameter(
            "OrphanFrameStream",
            "The I3Frame stream that will be checked for orphans.",
        )
        self.AddParameter(
            "DiscardStreams",
            "A list of I3Frame streams which are not considered as frames that"
            " disqualify the `OrphanFrameStream` from being an orphan. In "
            "other words if the frames in between two `OrphanFrameStream` "
            "frames are only of this stream type, then this will be considered"
            " as an orphan and *all* of these frames are discarded.",
            [],
        )

    def Configure(self):
        """Configure Module."""
        orphan_stream = self.GetParameter("OrphanFrameStream")
        discard_streams = self.GetParameter("DiscardStreams")

        # remember which frames we are allowed to discard
        self.orphan_stream = icetray.I3Frame.Stream(orphan_stream)
        self.discard_streams = [
            icetray.I3Frame.Stream(stream) for stream in discard_streams
        ]

        # create variables and frame buffer
        self._got_first_candidate = False
        self._is_orphan = False
        self._frame_buffer = deque()

    def Process(self):
        # get next frame
        frame = self.PopFrame()

        if frame.Stop == self.orphan_stream:
            # we have not had a potential orphan candidate frame yet
            if not self._got_first_candidate:
                # push all frames before first candidate frame
                self._push_frame_buffer()

                # remember that we have seen the first candidate frame
                self._got_first_candidate = True

            # We have had a candidate before, so now the question is if we need
            # to push the frame buffer or discard it
            else:
                # it is an orphan, we need to clear the frame buffer
                if self._is_orphan:
                    self._clear_frame_buffer()
                else:
                    self._push_frame_buffer()

            # reset orphan check: assume orphan unless proven otherwise
            self._is_orphan = True

        else:
            # check frame type and see if this disqualifies a candidate from
            # being an orphan
            if frame.Stop not in self.discard_streams:
                self._is_orphan = False

        # push frame on buffer
        self._frame_buffer.append(frame)

    def Finish(self):
        """Clean up."""
        # Check if we need to push or discard frames left on buffer
        if self._got_first_candidate:
            if self._is_orphan:
                self._clear_frame_buffer()
            else:
                self._push_frame_buffer()
        else:
            self._push_frame_buffer()

    def _clear_frame_buffer(self):
        """Clear all frames from frame buffer"""
        self._frame_buffer = deque()

    def _push_frame_buffer(self):
        """Push all frames in frame buffer"""
        while self._frame_buffer:
            fr = self._frame_buffer.popleft()
            self.PushFrame(fr)


def filter_events(frame, filter_list):
    """Filter events

    Parameters
    ----------
    frame : I3Frame
        The current physics frame.
    filter_list : list[dict]
        A list of dictionaries. Each dictionary defines a config
        that must contain the fields:
            `key`, `column`, `value`, `option`, `combination`
        The filter `option`s are:
            'greater_than', 'less_than', 'equal_to', 'unequal_to'
        Combination options are: 'and', 'or'
        Two masks: one for each combination type will be created.
        An event passes the filter if any of these two masks is True!

    Returns
    -------
    bool
        If the criteria defined in `filter_list` are fulfilled,
        'True' is returned and the current frame is kept.
        Otherwise it is discarded

    Raises
    ------
    ValueError
        If an unknown option or combination is provided.
    """
    mask_and_exists = False
    mask_or_exists = False
    mask_and = True
    mask_or = False
    for filter_dict in filter_list:
        frame_obj = frame[filter_dict["key"]]
        col = filter_dict["column"]
        if col is None:
            value = frame_obj.value
        else:
            try:
                value = getattr(frame_obj, filter_dict["column"])
            except AttributeError:
                value = frame_obj[filter_dict["column"]]

        # check if FilterMask object
        if isinstance(value, dataclasses.I3FilterResult):
            value = value.condition_passed

        # check if condition is passed
        if filter_dict["option"] == "greater_than":
            passed_cond = value > filter_dict["value"]
        elif filter_dict["option"] == "less_than":
            passed_cond = value < filter_dict["value"]
        elif filter_dict["option"] == "equal_to":
            passed_cond = value == filter_dict["value"]
        elif filter_dict["option"] == "unequal_to":
            passed_cond = value != filter_dict["value"]
        else:
            raise ValueError(
                "Filter option {!r} unknown!".format(filter_dict["option"])
            )

        if filter_dict["combination"] == "and":
            mask_and_exists = True
            mask_and = np.logical_and(mask_and, passed_cond)
        elif filter_dict["combination"] == "or":
            mask_or_exists = True
            mask_or = np.logical_or(mask_or, passed_cond)
        else:
            raise ValueError(
                "Filter combination {!r} unknown!".format(
                    filter_dict["combination"]
                )
            )

    if mask_and_exists and mask_and:
        return True
    if mask_or_exists and mask_or:
        return True

    # no filtering applied, e.g. all events pass
    if not mask_or_exists and not mask_and_exists:
        return True

    return False


def apply_l2_filter_mask(frame, filter_base_name):
    """Discard events that did not pass the specified filter.

    Parameters
    ----------
    frame : I3Frame
        Current I3Frame.
    filter_base_name : str
        The name of the L2 filter to apply. Only events passing this
        filter will be kept, others are discarded.
        If only the base name of the filter is given, i.e. "MuonFilter"
        instead of "MuonFilter_13", then the filter will be applied
        for a filter "MuonFilter*" in the "FilterMask".

    Returns
    -------
    bool
        True if cascade or muon filter is passed.

    Raises
    ------
    ValueError
        If keys can't be found.
    """
    if "FilterMask" in frame:
        f = frame["FilterMask"]
    elif "QFilterMask" in frame:
        f = frame["QFilterMask"]
    else:
        raise ValueError("No FilterMask in {!r}".format(frame))

    filter_key = None
    for key in f.keys():
        if filter_base_name == key[: len(filter_base_name)]:
            filter_key = key

    if filter_key is None:
        raise ValueError(
            "Could not find {} in {!r}".format(filter_key, f.keys())
        )

    return f[filter_key].condition_passed


def filter_streams(frame, streams_to_remove=[], streams_to_keep=[]):
    """Remove P-frames from specified streams

    Parameters
    ----------
    frame : I3Frame
        The current P-frame.
    streams_to_remove : list[str], optional
        The list of physics sub-event streams to remove.
    streams_to_keep : list, optional
        The list of physics sub-event streams to keep.

    Returns
    -------
    bool
        Returns False if the current frame's sub-event stream is
        one of the specified streams to remove. In this case, the
        frame will be discarded.
    """
    if streams_to_remove != [] and streams_to_keep != []:
        raise ValueError(
            "Only provide one of 'streams_to_remove' or 'streams_to_keep'!"
        )

    if frame.Stop == icetray.I3Frame.Physics:
        if frame["I3EventHeader"].sub_event_stream in streams_to_remove:
            return False
        if frame["I3EventHeader"].sub_event_stream not in streams_to_keep:
            return False
    return True
