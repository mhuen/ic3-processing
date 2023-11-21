from icecube import icetray

from ic3_labels.weights.utils import get_weighted_primary


@icetray.traysegment
def add_weighted_primary(tray, name):
    tray.AddModule(
        get_weighted_primary,
        name,
        If=lambda f: not f.Has("MCPrimary"),
    )
