from icecube import icetray, dataclasses


class AddSnowStormParameterDict(icetray.I3ConditionalModule):
    """Add SnowStorm Parameters as I3MapStringDouble"""

    def __init__(self, ctx):
        super(AddSnowStormParameterDict, self).__init__(ctx)
        self.AddParameter(
            "FrameType", "Frame type to write SnowStorm parameters to.", "M"
        )
        self.AddParameter(
            "OutputKey",
            "Key to which the SnowStorm parameters are written to",
            "SnowstormParameterDict",
        )

    def Configure(self):
        self._frame_type = self.GetParameter("FrameType")
        self._output_key = self.GetParameter("OutputKey")

    def Process(self):
        # get next frame
        frame = self.PopFrame()

        # check if this is the specified frame type to which we
        # want to write the parameters to
        if frame.Stop.id == self._frame_type:
            names = []
            for name in frame["SnowstormParametrizations"]:
                if name == "HoleIceForward_Unified":
                    names.append("HoleIceForward_Unified_p0")
                    names.append("HoleIceForward_Unified_p1")
                else:
                    names.append(name)

            ss_param_dict = dataclasses.I3MapStringDouble()
            for value, name in zip(frame["SnowstormParameters"], names):
                ss_param_dict[name] = value
            frame[self._output_key] = ss_param_dict

        self.PushFrame(frame)
