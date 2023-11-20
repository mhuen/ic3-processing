from ic3_data.ext_boost import combine_exclusions

from icecube import dataclasses


def get_combined_exclusions(
    frame, dom_exclusions, partial_exclusion, output_key
):
    """Get combined exclusions and write them to the frame.

    Parameters
    ----------
    frame : I3Frame
        The frame to which to write the combined exclusions.
    dom_exclusions : list of str
        The list of DOM exclusion frame keys to combine.
    partial_exclusion : bool
        Whether or not to apply partial DOM exclusion.
    output_key : str
        The base output key to which results will be written.
    """
    if isinstance(dom_exclusions, str):
        dom_exclusions = [dom_exclusions]

    excluded_doms, excluded_tws = combine_exclusions(
        frame, dom_exclusions, partial_exclusion
    )

    frame[output_key + "_DOMs"] = excluded_doms
    frame[output_key + "_TimeWindows"] = excluded_tws


def create_hdf5_time_window_series_map_hack(frame, tws_map_name, output_key):
    """Hack to write I3TimeWindowSeriesMap to frame

    Create an I3RecoPulseSeriesMap with information from the
    I3TimeWindowSeriesMap. A tableio converter already exists for
    the I3RecoPulseSeriesMap, but not for the I3TimeWindowSeriesMap.
    So we will mis-use the I3RecoPulseSeriesMap one.

    Todo: write proper converter for this!

    Parameters
    ----------
    frame : I3Frame
        The frame to which to write the combined exclusions.
    tws_map_name : str
        The name of the I3TimeWindowSeriesMap
    output_key : str
        The key to which the created I3RecoPulseSeriesMap will be written.

    """

    tws_map = frame[tws_map_name]

    pulse_series_map = dataclasses.I3RecoPulseSeriesMap()
    for om_key, time_window_series in tws_map:
        # create an I3RecoPulseSeries to which we will save the time windows
        reco_pulse_series = dataclasses.I3RecoPulseSeries()

        for time_window in time_window_series:
            # We will mis-use the pulse-time field for the start time
            # and the pulse-width field for the end time of the I3TimeWindow
            pulse = dataclasses.I3RecoPulse()
            pulse.time = time_window.start
            pulse.width = time_window.stop
            reco_pulse_series.append(pulse)

        pulse_series_map[om_key] = reco_pulse_series

    frame[output_key] = pulse_series_map
