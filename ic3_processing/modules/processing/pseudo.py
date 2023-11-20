from icecube import icetray
from icecube import dataclasses


class AddPseudePhysicsFrames(icetray.I3ConditionalModule):

    """Add fake physics frames to i3 file

    This module may be used to add fake/pseudo physics frames
    in an i3-file with only DAQ frames. This can be used, for instance,
    as a quick-and-dirty work-around to work with simulation files
    at generation level, where down-stream modules require physics frames.
    Note that the added physics frame is not a meaningful physics stream
    in any way!

    Attributes
    ----------
    event_id : int
        An internal counter to keep track of the events processed.
    run_id : int
        The id number of this run.
    """

    def __init__(self, context):
        """Class to add pseudo physics frames

        Parameters
        ----------
        context : TYPE
            Description
        """
        icetray.I3ConditionalModule.__init__(self, context)
        self.AddOutBox("OutBox")
        self.AddParameter("RunID", "RunID for I3EventHeader")

    def Configure(self):
        self.run_id = self.GetParameter("RunID")
        self.event_id = 0

    def DAQ(self, frame):
        """Add a pseudo physics frame after a DAQ frame

        Parameters
        ----------
        frame : icetray.I3Frame.DAQ
            An I3 q-frame.
        """
        self.PushFrame(frame)
        pseudo_frame = icetray.I3Frame()
        pseudo_frame.Stop = icetray.I3Frame.Physics

        # create pseudo event header
        event_header = dataclasses.I3EventHeader()
        event_header.run_id = self.run_id
        event_header.event_id = self.event_id
        event_header.sub_event_id = 0
        event_header.sub_run_id = 0

        pseudo_frame["I3EventHeader"] = event_header
        self.event_id += 1

        self.PushFrame(pseudo_frame)
