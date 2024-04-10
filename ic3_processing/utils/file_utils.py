"""
Note: this file must unfortunately be python 2.7 compatible!

Helper-functions for file utilities
"""

from __future__ import print_function, division
from icecube import dataio, icetray
import math


def file_is_readable(file_name):
    """Check if an I3 file is readable, or if it is corrupt.

    Parameters
    ----------
    file_name : str
        The path to the file.

    Returns
    -------
    int
        Number of frames. If None, then something is wrong with the file.
        For instance, it may be corrupt.
    """
    frame_counter = 0
    try:
        f = dataio.I3File(file_name)
        while f.more():
            f.pop_frame()
            frame_counter += 1
        f.close()
    except Exception as e:
        print("Caught an exception:", e)
        frame_counter = None
    return frame_counter


def filter_corrupted_files(infiles, verbose=True):
    """Select non-corrupted files from a given list

    Parameters
    ----------
    infiles : List[str]
        The list of input files.
    verbose : bool, optional
        If True, additional information is printed to the console.

    Returns
    -------
    List[str]
        Description
    """
    if verbose:
        print("Checking if files are readable...")

    filtered_infiles = []
    frame_cnt = 0
    for file_name in infiles:
        count_i = file_is_readable(file_name)
        if count_i is None:
            if verbose:
                print("Found possibly corrupt file: {}".format(file_name))
        else:
            frame_cnt += count_i
            filtered_infiles.append(file_name)
    if verbose:
        print(
            "Filtered out {} files. Processing {} frames.".format(
                len(infiles) - len(filtered_infiles), frame_cnt
            )
        )

    return filtered_infiles


def get_number_of_frames(file_name):
    """Counts number of frame types given in an i3 file.

    Parameters
    ----------
    file_name : str
        The path to the file.

    Returns
    -------
    dict
        A dictionary that contains the number of files for every frame
        type that was found in the file.
        If None, then something is wrong with the file.
        For instance, it may be corrupt.
    """
    frame_counter = {}
    try:
        f = dataio.I3File(file_name)
        while f.more():
            fr = f.pop_frame()
            if fr.Stop.id not in frame_counter:
                frame_counter[fr.Stop.id] = 0
            frame_counter[fr.Stop.id] += 1
        f.close()
    except Exception as e:
        print("Caught an exception:", e)
        frame_counter = None
    return frame_counter


def get_total_weight_n_files(file_names, assume_single_file=False):
    """Get the total number of files merged together.

    This computes the n_files parameter for weighting.

    Note: this method checks for previously written "W" frames in which
    meta information is saved on the number of processed/merges files.
    If this information is not found, an error is raised unless
    `assume_single_file` is set to True. In this case, it is assumed
    that the inputs all consist of individual runs without any previous
    merging and that no previous meta information has been written.

    Parameters
    ----------
    file_names : list[str]
        A list of file paths to the input files. Note: for correct counting
        of total number of files when `assume_single_file` is set to  True,
        this must only contain i3-files and no GCD files!
    assume_single_file : bool, optional
        If True and when no previous weights meta information is found,
        it is assumed that the total number of n_files is simply the
        number of input files.

    Returns
    -------
    int
        Number of frames. If None, then something is wrong with the file.
        For instance, it may be corrupt.
    bool
        True if meta information was found in the files, otherwise False.

    Raises
    ------
    ValueError
        if not weights meta information frame exists and `assume_single_file`
        is set to False.
        , or if `assume_single_file`
    """

    total_n_files = 0
    found_at_least_one_w_frame = False

    for file_name in file_names:
        f = dataio.I3File(file_name)
        while f.more():
            fr = f.pop_frame()
            if "weights_meta_info" in fr:
                total_n_files += fr["weights_meta_info"]["n_files"]
                found_at_least_one_w_frame = True
                break
            elif (
                fr.Stop == icetray.I3Frame.Stream("W")
                or fr.Stop == icetray.I3Frame.DAQ
            ):
                if assume_single_file:
                    total_n_files = float("nan")
                else:
                    raise ValueError("No weight meta data found!")
        f.close()

    if math.isnan(total_n_files):
        # safety check to see if any files contain weights meta information.
        # This should only happen when files are first processed with the
        # ic3-processing framework. Once previous meta information has been
        # written, it's more likely that the assumption of no previous file
        # merging may be broken.
        if found_at_least_one_w_frame:
            raise ValueError(
                "Expected no input file to have weight meta information!"
            )
        else:
            assert assume_single_file, "Something went wrong!"
            total_n_files = len(file_names)

    return total_n_files, found_at_least_one_w_frame


class PartialFileProcessing(icetray.I3ConditionalModule):
    """Module to only process part of a file"""

    def __init__(self, ctx):
        super(PartialFileProcessing, self).__init__(ctx)
        self.AddParameter("FrameType", "Frame type to count.", "Q")
        self.AddParameter("MaxFrames", "Maximum number of frames.")
        self.AddParameter(
            "FinishDependentFrames",
            "Include all frames until next frame of type as specified in "
            "`FrameType`",
            True,
        )

    def Configure(self):
        self._frame_type = self.GetParameter("FrameType")
        self._max_frames = self.GetParameter("MaxFrames")
        self._finish = self.GetParameter("FinishDependentFrames")
        self.counter = 0
        self._done = False

        if self._max_frames < 0:
            raise ValueError("Must provide max number >= 0:", self._max_frames)

    def Process(self):
        # check if we need to stop processing
        if self.counter == self._max_frames:
            if self._done:
                # discard remaining frames until requested suspension works
                self.PopFrame()
            else:
                frame = self.PopFrame()
                if self._finish and frame.Stop.id != self._frame_type:
                    self.PushFrame(frame)
                else:
                    self.RequestSuspension()
                    self._done = True
        else:
            # get next frame
            frame = self.PopFrame()

            # count frames if it's the type we are looking for
            if frame.Stop.id == self._frame_type:
                self.counter += 1

            self.PushFrame(frame)
