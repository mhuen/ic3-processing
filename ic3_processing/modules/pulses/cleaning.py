from icecube import icetray


@icetray.traysegment
def apply_time_window_cleaning(
    tray,
    name,
    InputResponse="SplitInIceDSTPulses",
    OutputResponse="SplitInIceDSTPulsesTWCleaning6000ns",
    TimeWindow=6000 * icetray.I3Units.ns,
):
    from icecube import DomTools  # noqa F401

    tray.AddModule(
        "I3TimeWindowCleaning<I3RecoPulse>",
        name,
        InputResponse=InputResponse,
        OutputResponse=OutputResponse,
        TimeWindow=TimeWindow,
    )
