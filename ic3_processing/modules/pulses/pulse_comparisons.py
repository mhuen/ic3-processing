from icecube import icetray, dataclasses


@icetray.traysegment
def select_DOM_pulses(tray, name, dom_list, pulse_key):
    """Create RecoPulseSeriesMaps with pulses from specified DOMs

    Parameters
    ----------
    tray : I3Tray
        The I3Tray to which the modules should be added.
    name : str, optional
        Name of the tray segment.
    dom_list : list of tuple(int, int)
        The list of DOMs (string, DOM) for which to retain pulses.
    pulse_key : str
        The pulse key to work on.
    """

    tray.AddModule(
        _select_DOM_pulses,
        name + "select_DOM_pulses",
        dom_list=dom_list,
        pulse_key=pulse_key,
    )

    for s, d in dom_list:
        key_name = pulse_key + "_S{:02d}D{:02d}".format(s, d)
        if key_name not in tray.context["ic3_processing"]["HDF_keys"]:
            tray.context["ic3_processing"]["HDF_keys"].append(key_name)
            tray.context["ic3_processing"]["HDF_keys"].append(
                key_name + "_Flags"
            )


@icetray.traysegment
def select_DOM_exclusions(tray, name, dom_list, tw_exclusion_keys=[]):
    """Create RecoPulseSeriesMaps with TimeWindow exclusions from
    specified DOMs and exclusions keys

    Parameters
    ----------
    tray : I3Tray
        The I3Tray to which the modules should be added.
    name : str, optional
        Name of the tray segment.
    dom_list : list of tuple(int, int)
        The list of DOMs (string, DOM) for which to retain exclusions.
    tw_exclusion_keys : list of str, optional
        The names of the time window exclusion keys to work on.
    """
    tray.AddModule(
        _select_DOM_exclusions,
        name + "select_DOM_exclusions",
        dom_list=dom_list,
        tw_exclusion_keys=tw_exclusion_keys,
    )

    for tw_key in tw_exclusion_keys:
        for s, d in dom_list:
            key_name = tw_key + "_S{:02d}D{:02d}".format(s, d)
            if key_name not in tray.context["ic3_processing"]["HDF_keys"]:
                tray.context["ic3_processing"]["HDF_keys"].append(key_name)


def _select_DOM_pulses(frame, dom_list, pulse_key):
    """Create RecoPulseSeriesMaps with pulses from specified DOMs

    Parameters
    ----------
    frame : I3Frame
        The current physics frame.
    dom_list : list of tuple(int, int)
        The list of DOMs (string, DOM) for which to retain pulses.
    pulse_key : str
        The pulse key to work on.
    """
    orig_pulse_series = frame[pulse_key]
    if isinstance(
        orig_pulse_series, dataclasses.I3RecoPulseSeriesMapMask
    ) or isinstance(orig_pulse_series, dataclasses.I3RecoPulseSeriesMapUnion):
        orig_pulse_series = orig_pulse_series.apply(frame)

    # make sure we are comparing tuples to each other
    dom_list = [(s, d) for s, d in dom_list]
    for s, d in dom_list:
        pulse_series = dataclasses.I3RecoPulseSeriesMap()
        flag_series = dataclasses.I3MapKeyVectorInt()
        omkey = icetray.OMKey(s, d)
        if omkey in orig_pulse_series:
            pulse_series[omkey] = dataclasses.I3RecoPulseSeries(
                orig_pulse_series[omkey]
            )
            flag_series[omkey] = [p.flags for p in orig_pulse_series[omkey]]

        frame[pulse_key + "_S{:02d}D{:02d}".format(s, d)] = pulse_series
        frame[pulse_key + "_S{:02d}D{:02d}_Flags".format(s, d)] = flag_series


def _select_DOM_exclusions(frame, dom_list, tw_exclusion_keys=[]):
    """Create RecoPulseSeriesMaps with TimeWindow exclusions from
    specified DOMs and exclusions keys

    Parameters
    ----------
    frame : I3Frame
        The current physics frame.
    dom_list : list of tuple(int, int)
        The list of DOMs (string, DOM) for which to retain exclusions.
    tw_exclusion_keys : list of str, optional
        The names of the time window exclusion keys to work on.
    """

    for tw_key in tw_exclusion_keys:
        # make sure we are comparing tuples to each other
        dom_list = [(s, d) for s, d in dom_list]
        for s, d in dom_list:
            pulse_series = dataclasses.I3RecoPulseSeriesMap()
            omkey = icetray.OMKey(s, d)

            if tw_key in frame:
                tws_map = frame[tw_key]
                if omkey in tws_map:
                    # create an I3RecoPulseSeries to which we
                    # will save the time windows
                    reco_pulse_series = dataclasses.I3RecoPulseSeries()

                    for time_window in tws_map[omkey]:
                        # We will mis-use the pulse-time field for the start
                        # time and the pulse-width field for the end time of
                        # the I3TimeWindow
                        pulse = dataclasses.I3RecoPulse()
                        pulse.time = time_window.start
                        pulse.width = time_window.stop
                        reco_pulse_series.append(pulse)

                    pulse_series[omkey] = reco_pulse_series

            frame[tw_key + "_S{:02d}D{:02d}".format(s, d)] = pulse_series
