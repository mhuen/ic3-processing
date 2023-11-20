# -*- coding: utf-8 -*-
"""Helper functions to mask pulses
"""
from ic3_data.ext_boost import get_valid_pulse_map as get_valid_pulse_map_cpp


def get_valid_pulse_map(
    frame,
    pulse_key,
    excluded_doms,
    partial_exclusion,
    output_key=None,
    verbose=False,
):
    """Simple wrapper over c++ version.
    Necessary for I3 Magic...

    Parameters
    ----------
    frame : TYPE
        Description
    pulse_key : TYPE
        Description
    excluded_doms : TYPE
        Description
    partial_exclusion : TYPE
        Description
    output_key : None, optional
        Description
    verbose : bool, optional
        Description

    No Longer Returned
    ------------------
    TYPE
        Description
    """
    if isinstance(excluded_doms, str):
        excluded_doms = [excluded_doms]

    pulses = get_valid_pulse_map_cpp(
        frame, pulse_key, excluded_doms, partial_exclusion, verbose
    )

    if output_key is None:
        frame[pulse_key + "_masked"] = pulses
    else:
        frame[output_key] = pulses
