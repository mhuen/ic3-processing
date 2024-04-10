import numpy as np
from icecube import icetray, dataclasses, dataio


def collect_exp_livetime_data(file_names, cfg, key="ExpLivetime"):
    """Collect exp livetime data from a list of files.

    Warning: this expects an X-frame to be present before the first DAQ-frame!

    Parameters
    ----------
    file_names : list of str
        A list of input files.
    cfg : dict
        The config file which will be updated with the collected data.
    key : str, optional
        The output key to which the exp data was written.

    Returns
    -------
    dic
        The updated configuration file with the collected data.
        New keys include:
            - exp_dataset_livetime
            - exp_dataset_start_times
            - exp_dataset_end_times
            - exp_dataset_run_list
    """

    # make sure that the config does not already contain the keys
    assert "exp_dataset_livetime" not in cfg, cfg["exp_dataset_livetime"]
    assert "exp_dataset_start_times" not in cfg, cfg["exp_dataset_start_times"]
    assert "exp_dataset_end_times" not in cfg, cfg["exp_dataset_end_times"]
    assert "exp_dataset_run_list" not in cfg, cfg["exp_dataset_run_list"]

    merge_livetime = 0.0
    merge_start_times = []
    merge_end_times = []
    merge_run_list = []

    for file_name in file_names:
        f = dataio.I3File(file_name)
        while f.more():
            fr = f.pop_frame()
            if fr.Stop.id == "X":
                merge_livetime += fr[key + "_livetime"].value
                merge_start_times.extend(fr[key + "_start_times"])
                merge_end_times.extend(fr[key + "_end_times"])
                merge_run_list.extend(fr[key + "_run_list"])
                break
            elif (
                fr.Stop == icetray.I3Frame.DAQ
                or fr.Stop == icetray.I3Frame.Physics
            ):
                raise ValueError("No exp livetime data found!")
        f.close()

    # update config
    cfg["exp_dataset_livetime"] = merge_livetime
    cfg["exp_dataset_start_times"] = merge_start_times
    cfg["exp_dataset_end_times"] = merge_end_times
    cfg["exp_dataset_run_list"] = merge_run_list
    cfg["exp_dataset_merge"] = True
    return cfg


@icetray.traysegment
def write_exp_livetime_data(tray, name, cfg):
    """Write Experimental Livetime Data to I3Frame

    Parameters
    ----------
    tray : icetray
        The icetray
    name : str
        Name of this module.
    cfg : dict
        The configuration settings.
    """
    if "exp_dataset_livetime" in cfg:
        tray.AddModule(
            WriteEXPLivetimeData,
            name,
            LiveTime=cfg["exp_dataset_livetime"],
            StartTimes=cfg["exp_dataset_start_times"],
            EndTimes=cfg["exp_dataset_end_times"],
            RunList=cfg["exp_dataset_run_list"],
            Merge=cfg["exp_dataset_merge"],
        )


class WriteEXPLivetimeData(icetray.I3ConditionalModule):
    """Module to write livetime of exp data to X-frame"""

    def __init__(self, ctx):
        super(WriteEXPLivetimeData, self).__init__(ctx)
        self.AddParameter("Key", "Output base key.", "ExpLivetime")
        self.AddParameter("LiveTime", "Livetime of file in seconds.")
        self.AddParameter("StartTimes", "List of start times.")
        self.AddParameter("EndTimes", "List of end times.")
        self.AddParameter("RunList", "List of runs.")
        self.AddParameter("Merge", "Indicates whether merging files.", False)

    def Configure(self):
        self._livetime = self.GetParameter("LiveTime")
        self._start_times = self.GetParameter("StartTimes")
        self._end_times = self.GetParameter("EndTimes")
        self._run_list = self.GetParameter("RunList")
        self._key = self.GetParameter("Key")
        self._merge = self.GetParameter("Merge")
        self._frame_has_been_pushed = False

        # add variables to verify merging of livetime
        self._merge_livetime = 0.0
        self._merge_start_times = []
        self._merge_end_times = []
        self._merge_run_list = []

        assert len(self._start_times) == len(self._end_times)

    def Finish(self):
        """Verify that provided values match to calculated ones"""
        if self._merge:
            print("Exp Livetime Check:")
            print(
                "\tLivetime: {} | {}".format(
                    self._livetime, self._merge_livetime
                )
            )

            # Check if livetime match up to 0.001s or rtol of 1e-5
            if not np.allclose(
                self._livetime, self._merge_livetime, atol=1e-3
            ):
                raise ValueError(
                    "Livetime does not match up: {} != {}".format(
                        self._livetime, self._merge_livetime
                    )
                )

            # Check if start times match up
            if not np.allclose(self._start_times, self._merge_start_times):
                raise ValueError(
                    "Start times do not match up: {} != {}".format(
                        self._start_times, self._merge_start_times
                    )
                )

            # Check if end times match up
            if not np.allclose(self._end_times, self._merge_end_times):
                raise ValueError(
                    "End times do not match up: {} != {}".format(
                        self._end_times, self._merge_end_times
                    )
                )

            # Check if run list match up
            if not np.allclose(self._run_list, self._merge_run_list):
                raise ValueError(
                    "End times do not match up: {} != {}".format(
                        self._run_list, self._merge_run_list
                    )
                )

    def Process(self):
        # get next frame
        frame = self.PopFrame()

        # Remove old X-frames (if merging) / make sure they don't exist
        if self._merge:
            if frame.Stop.id == "X":
                # update internal variables to check livetime at end
                self._merge_livetime += frame[self._key + "_livetime"].value
                self._merge_run_list.extend(frame[self._key + "_run_list"])
                self._merge_end_times.extend(frame[self._key + "_end_times"])
                self._merge_start_times.extend(
                    frame[self._key + "_start_times"]
                )

                # now discard the X-frame
                return
        else:
            # There should not be an X-frame in the i3-file!
            assert frame.Stop.id != "X", frame

        if not self._frame_has_been_pushed:
            # create weight frame and push it
            exp_frame = icetray.I3Frame("X")

            exp_frame[self._key + "_livetime"] = dataclasses.I3Double(
                self._livetime
            )
            exp_frame[self._key + "_start_times"] = dataclasses.I3VectorDouble(
                self._start_times
            )
            exp_frame[self._key + "_end_times"] = dataclasses.I3VectorDouble(
                self._end_times
            )
            exp_frame[self._key + "_run_list"] = dataclasses.I3VectorInt(
                self._run_list
            )

            self.PushFrame(exp_frame)

            self._frame_has_been_pushed = True

        self.PushFrame(frame)
