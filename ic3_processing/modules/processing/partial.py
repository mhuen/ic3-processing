"""
Helper-functions for file utilities
"""

from icecube import icetray


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
