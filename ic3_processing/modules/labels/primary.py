from icecube import icetray

from ic3_labels.labels.base_module import MCLabelsBase
from ic3_labels.labels.utils import general
from ic3_labels.weights.utils import get_weighted_primary


@icetray.traysegment
def add_weighted_primary(tray, name):
    tray.AddModule(
        get_weighted_primary,
        name,
        If=lambda f: not f.Has("MCPrimary"),
    )


class AddWeightedPrimary(MCLabelsBase):
    """Add weighted primary to frame"""

    def __init__(self, context):
        MCLabelsBase.__init__(self, context)
        self.AddParameter(
            "MCTreeName",
            "The name of the I3MCTree to use",
            "I3MCTree",
        )

    def Configure(self):
        MCLabelsBase.Configure(self)
        self._i3mctree_name = self.GetParameter("MCTreeName")

    def add_labels(self, frame):
        """Run on DAQ frames.

        Parameters
        ----------
        frame : I3Frame
            The current DAQ Frame
        """
        primary = general.get_weighted_primary(
            frame=frame,
            mctree_name=self._i3mctree_name,
        )
        frame[self._output_key] = primary
