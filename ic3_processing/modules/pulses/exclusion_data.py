import numpy as np
from icecube import dataclasses


def gather_exclusion_data(
    frame, excluded_doms, pulse_key, partial_exclusion, output_key
):
    """Add information on excluded DOM and pulses.

    Note: this currently does not use `partial_exclusion`

    Parameters
    ----------
    frame : I3Frame
        The current I3Frame.
    excluded_doms : list of str
        List of frame keys that define DOM exclusions.
    pulse_key : str
        The name of the pulses for which exclusions were calculated.
        Note: `get_valid_pulse_map` must have been run previously
        to running this method such that `pulse_key` and
        `pulse_key`+'_masked' exist in the frame.
    partial_exclusion : bool
        Whether or not to partially exclude DOMs.
        Note: this currently has no effect on the output of this module
    output_key : str
        The frame key to which the results will be written to.
    """
    if partial_exclusion:
        raise NotImplementedError("Partial exclusion not yet supported!")

    data = dataclasses.I3MapStringDouble()

    # get number of excluded DOMs
    for exclusion_key in excluded_doms:
        num_exclusions = 0
        if exclusion_key in frame:
            if isinstance(frame[exclusion_key], dataclasses.I3VectorOMKey):
                for omkey in frame[exclusion_key]:
                    if omkey.om <= 60:
                        num_exclusions += 1
            else:
                for omkey in frame[exclusion_key].keys():
                    if omkey.om <= 60:
                        num_exclusions += 1
        data[exclusion_key] = num_exclusions

    # get total charge for excluded and non excluded pulses
    for key in [pulse_key, pulse_key + "_masked"]:
        total_charge = 0
        pulse_series_map = frame[key]
        if isinstance(
            pulse_series_map,
            (
                dataclasses.I3RecoPulseSeriesMapMask,
                dataclasses.I3RecoPulseSeriesMapUnion,
            ),
        ):
            pulse_series_map = pulse_series_map.apply(frame)

        for omkey, pulse_series in pulse_series_map:
            total_charge += np.sum([p.charge for p in pulse_series])
        data["Charge_{}".format(key)] = total_charge

    # write to frame
    frame[output_key] = data
