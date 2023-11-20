# -*- coding: utf-8 -*-
"""Helper functions to apply modifications to pulses
"""
from icecube import dataclasses
import numpy as np


def shift_pulses(
    self,
    pulses,
    charge_shift=None,
    time_shift=None,
    first_k_pulses=float("inf"),
    *args,
    **kwargs,
):
    """Shift the charges and times of the provided pulses.
    There is an option to only shift the first k number of pulses by providing
    a value to first_k_pulses.

    Parameters
    ----------
    pulses : I3RecoPulseSeriesMap
        Pulses to modify.
    charge_shift : float, optional
        The amount to shift the pulse charges.
    time_shift : float, optional
        The amount to shift the pulse times.
    first_k_pulses : int, optional
        If specified, only shift the first_k_pulses of a DOM.
    *args
        Variable length argument list.
    **kwargs
        Arbitrary keyword arguments.

    Returns
    -------
    I3RecoPulseSeriesMap
        The modified pulses.
    """
    modified_pulses = {}
    for key, dom_pulses in pulses.items():
        modified_dom_pulses = []
        pulse_counter = 0

        for pulse in dom_pulses:
            modified_pulse = dataclasses.I3RecoPulse(pulse)
            pulse_counter += 1

            # modify pulse
            if pulse_counter <= first_k_pulses:
                if time_shift is not None:
                    modified_pulse.time += time_shift
                if charge_shift is not None:
                    modified_pulse.charge = np.clip(
                        modified_pulse.charge + charge_shift, 0.0, float("inf")
                    )

            # append pulse
            modified_dom_pulses.append(modified_pulse)

        modified_pulses[key] = dataclasses.vector_I3RecoPulse(
            modified_dom_pulses
        )

    return dataclasses.I3RecoPulseSeriesMap(modified_pulses)


def gaussian_smear_pulse_times(self, pulses, scale, *args, **kwargs):
    """Smear the pulse times with a Gaussian centered at the original value.

    Parameters
    ----------
    pulses : I3RecoPulseSeriesMap
        Pulses to modify.
    scale : float
        The scale parameter of the Gaussian that is used to smear the pulse
        times.
    *args
        Variable length argument list.
    **kwargs
        Arbitrary keyword arguments.

    Returns
    -------
    I3RecoPulseSeriesMap
        The modified pulses.
    """
    assert scale >= 0, "scale {!r} must be >= zero".format(scale)

    modified_pulses = {}
    for key, dom_pulses in pulses.items():
        # smear times
        times = self._random_generator.normal(
            loc=[p.time for p in dom_pulses], scale=scale
        )
        charges = np.array([p.charge for p in dom_pulses])
        widths = np.array([p.width for p in dom_pulses])
        flags = np.array([p.flags for p in dom_pulses])

        # sort pulses in time
        sorted_indices = np.argsort(times)
        charges = charges[sorted_indices]
        times = times[sorted_indices]
        widths = widths[sorted_indices]
        flags = flags[sorted_indices]

        modified_dom_pulses = []

        for charge, time, flag, width in zip(charges, times, flags, widths):
            # create pulse
            modified_pulse = dataclasses.I3RecoPulse()
            modified_pulse.charge = charge
            modified_pulse.time = time
            modified_pulse.flags = int(flag)
            modified_pulse.width = width

            # append pulse
            modified_dom_pulses.append(modified_pulse)

        modified_pulses[key] = dataclasses.vector_I3RecoPulse(
            fix_time_overlap(modified_dom_pulses)
        )

    return dataclasses.I3RecoPulseSeriesMap(modified_pulses)


def scramble_charges(self, pulses, *args, **kwargs):
    """Scramble the charges of the pulses.

    Parameters
    ----------
    pulses : I3RecoPulseSeriesMap
        Pulses to modify.
    *args
        Variable length argument list.
    **kwargs
        Arbitrary keyword arguments.

    Returns
    -------
    I3RecoPulseSeriesMap
        The modified pulses.
    """
    modified_pulses = {}
    for key, dom_pulses in pulses.items():
        charges = [p.charge for p in dom_pulses]

        # scramble pulse charges
        self._random_generator.shuffle(charges)

        modified_dom_pulses = []

        for pulse, charge in zip(dom_pulses, charges):
            modified_pulse = dataclasses.I3RecoPulse(pulse)

            # modify pulse
            modified_pulse.charge = charge

            # append pulse
            modified_dom_pulses.append(modified_pulse)

        modified_pulses[key] = dataclasses.vector_I3RecoPulse(
            modified_dom_pulses
        )

    return dataclasses.I3RecoPulseSeriesMap(modified_pulses)


def discard_k_highest_charge_doms(self, pulses, k, *args, **kwargs):
    """Discard the top k DOMs that have the most charge.

    Parameters
    ----------
    pulses : I3RecoPulseSeriesMap
        Pulses to modify.
    k : int
        The number of DOMs to discard.
    *args
        Variable length argument list.
    **kwargs
        Arbitrary keyword arguments.

    Returns
    -------
    I3RecoPulseSeriesMap
        The modified pulses.
    """
    assert k >= 0, "k {!r} must be >= zero".format(k)

    # calculate total charge for each DOM
    dom_charges = []
    keys = []
    for key, dom_pulses in pulses.items():
        dom_charge = np.sum([p.charge for p in dom_pulses])
        dom_charges.append(dom_charge)
        keys.append(key)

    sorted_indices = np.argsort(dom_charges)
    sorted_keys = [keys[i] for i in sorted_indices]

    top_k = np.clip(k, 0, len(keys))

    modified_pulses = {}
    for key in sorted_keys[:-top_k]:
        # add DOM
        modified_pulses[key] = dataclasses.vector_I3RecoPulse(pulses[key])

    return dataclasses.I3RecoPulseSeriesMap(modified_pulses)


def discard_random_doms(self, pulses, discard_probability, *args, **kwargs):
    """Discard DOMs randomly based on the discard_probability.

    Parameters
    ----------
    pulses : I3RecoPulseSeriesMap
        Pulses to modify.
    discard_probability : float
        The probability a DOM is discarded. Must be a value between 0 and 1.
    *args
        Variable length argument list.
    **kwargs
        Arbitrary keyword arguments.

    Returns
    -------
    I3RecoPulseSeriesMap
        The modified pulses.
    """
    assert (
        discard_probability >= 0 and discard_probability <= 1
    ), "discard_probability {!r} not in [0, 1]".format(discard_probability)

    modified_pulses = {}
    for key, dom_pulses in pulses.items():
        # draw random variable and decide if DOM is discarded
        discard_dom = self._random_generator.uniform() < discard_probability

        # add DOM
        if not discard_dom:
            modified_pulses[key] = dataclasses.vector_I3RecoPulse(dom_pulses)

    return dataclasses.I3RecoPulseSeriesMap(modified_pulses)


def discard_random_pulses(self, pulses, discard_probability, *args, **kwargs):
    """Discard pulses randomly based on the discard_probability.

    Parameters
    ----------
    pulses : I3RecoPulseSeriesMap
        Pulses to modify.
    discard_probability : float
        The probability a pulse is discarded. Must be a value between 0 and 1.
    *args
        Variable length argument list.
    **kwargs
        Arbitrary keyword arguments.

    Returns
    -------
    I3RecoPulseSeriesMap
        The modified pulses.
    """
    assert (
        discard_probability >= 0 and discard_probability <= 1
    ), "discard_probability {!r} not in [0, 1]".format(discard_probability)

    modified_pulses = {}
    for key, dom_pulses in pulses.items():
        modified_dom_pulses = []

        for pulse in dom_pulses:
            modified_pulse = dataclasses.I3RecoPulse(pulse)

            # draw random variable and decide if pulse is discarded
            discard_pulse = (
                self._random_generator.uniform() < discard_probability
            )

            # append pulse
            if not discard_pulse:
                modified_dom_pulses.append(modified_pulse)

        if modified_dom_pulses:
            modified_pulses[key] = dataclasses.vector_I3RecoPulse(
                modified_dom_pulses
            )

    return dataclasses.I3RecoPulseSeriesMap(modified_pulses)


def add_random_pulses(
    self,
    pulses,
    mean_dom_pulses,
    time_range,
    charge_shape=1.8,
    *args,
    **kwargs,
):
    """Add pulses randomly to hit DOMs based on poisson expectation
    mean_dom_pulses and the provided time_range.

    Note: this does NOT add random pulses to all DOMs - just to hit ones.

    Parameters
    ----------
    pulses : I3RecoPulseSeriesMap
        Pulses to modify.
    mean_dom_pulses : float
        The poisson expectation of how many pulses to add per DOM.
    charge_shape : float
        The expectation of the charge for each added pulse. The charge is
        sampled from a gamma distribution with the charge_shape set as shape.
    time_range : [float, float]
        The time range in which the pulse times are uniformly sampled.
    *args
        Variable length argument list.
    **kwargs
        Arbitrary keyword arguments.

    Returns
    -------
    I3RecoPulseSeriesMap
        The modified pulses.
    """
    modified_pulses = {}
    for key, dom_pulses in pulses.items():
        # sample random pulses that are to be added
        num_pulses = self._random_generator.poisson(lam=mean_dom_pulses)
        if charge_shape is None or charge_shape in ["none", "", "None"]:
            charges_new = np.ones(shape=num_pulses)
        else:
            charges_new = self._random_generator.gamma(
                shape=charge_shape, size=num_pulses
            )
        times_new = self._random_generator.uniform(
            low=time_range[0], high=time_range[1], size=num_pulses
        )
        # set LC, ATWD, and FADC flag
        flags_new = [7] * num_pulses
        # set width to 2 ns (most common in pulses)
        widths_new = [2] * num_pulses

        charges = np.concatenate(([p.charge for p in dom_pulses], charges_new))
        times = np.concatenate(([p.time for p in dom_pulses], times_new))
        widths = np.concatenate(([p.width for p in dom_pulses], widths_new))
        flags = np.concatenate(([p.flags for p in dom_pulses], flags_new))

        # sort pulses in time
        sorted_indices = np.argsort(times)
        charges = charges[sorted_indices]
        times = times[sorted_indices]
        widths = widths[sorted_indices]
        flags = flags[sorted_indices]

        modified_dom_pulses = []

        for charge, time, flag, width in zip(charges, times, flags, widths):
            # create pulse
            modified_pulse = dataclasses.I3RecoPulse()
            modified_pulse.charge = charge
            modified_pulse.time = time
            modified_pulse.flags = int(flag)
            modified_pulse.width = width

            # append pulse
            modified_dom_pulses.append(modified_pulse)

        modified_pulses[key] = dataclasses.vector_I3RecoPulse(
            fix_time_overlap(modified_dom_pulses)
        )

    return dataclasses.I3RecoPulseSeriesMap(modified_pulses)


def add_white_noise(
    self,
    pulses,
    charge_range,
    time_range,
    noise_rate_factor,
    dom_noise_rate_dict,
    frame,
    *args,
    **kwargs,
):
    """Add white noise to DOMs.

    Pulses are drawn uniformly from the ranges specified in charge_range
    and time_range. The noise rate at each DOM is taken from calibration
    data and then scaled by the noise_rate_factor.

    Parameters
    ----------
    pulses : I3RecoPulseSeriesMap
        Pulses to modify.
    charge_range : [float, float]
        The charge range in which the pulse charges are uniformly sampled.
    time_range : [float, float]
        The time range in which the pulse times are uniformly sampled.
    noise_rate_factor : float
        The poisson mean for the added white noise will be the DOM noise
        rate multiplied by the noise_rate_factor, e.g. noise_rate_factor=100
        will add white noise to DOMs that corresponds to 100 times the normal
        rate.
    dom_noise_rate_dict : dict
        A dictionary {omkey: noise rate} that contains the noise rate for each
        DOM.
    frame : I3Frame
        The current I3Frame.
    *args
        Variable length argument list.
    **kwargs
        Arbitrary keyword arguments.

    Returns
    -------
    I3RecoPulseSeriesMap
        The modified pulses.
    """
    assert time_range[1] > time_range[0], "{!r} !< {!r}".format(*time_range)

    time_window = time_range[1] - time_range[0]

    modified_pulses = {}
    for key, rate in dom_noise_rate_dict.items():
        # Do not incldude DOMs if they do not belong to the
        # detector configuration, e.g. if they are marked as bad DOMs.
        if key in frame["BadDomsList"] or key in frame["BadDomsListSLC"]:
            continue

        # sample random pulses that are to be added
        num_pulses = self._random_generator.poisson(
            time_window * rate * noise_rate_factor
        )
        charges_new = self._random_generator.uniform(
            low=charge_range[0], high=charge_range[1], size=num_pulses
        )
        times_new = self._random_generator.uniform(
            low=time_range[0], high=time_range[1], size=num_pulses
        )
        # set LC, ATWD, and FADC flag
        flags_new = [7] * num_pulses
        # set width to 2 ns (most common in pulses)
        widths_new = [2] * num_pulses

        if key in pulses:
            dom_pulses = pulses[key]
        else:
            dom_pulses = []

        charges = np.concatenate(([p.charge for p in dom_pulses], charges_new))
        times = np.concatenate(([p.time for p in dom_pulses], times_new))
        widths = np.concatenate(([p.width for p in dom_pulses], widths_new))
        flags = np.concatenate(([p.flags for p in dom_pulses], flags_new))

        if len(charges) > 0:
            # sort pulses in time
            sorted_indices = np.argsort(times)
            charges = charges[sorted_indices]
            times = times[sorted_indices]
            widths = widths[sorted_indices]
            flags = flags[sorted_indices]

            modified_dom_pulses = []

            for charge, time, flag, width in zip(
                charges, times, flags, widths
            ):
                # create pulse
                modified_pulse = dataclasses.I3RecoPulse()
                modified_pulse.charge = charge
                modified_pulse.time = time
                modified_pulse.flags = int(flag)
                modified_pulse.width = width

                # append pulse
                modified_dom_pulses.append(modified_pulse)

            modified_pulses[key] = dataclasses.vector_I3RecoPulse(
                fix_time_overlap(modified_dom_pulses)
            )

    return dataclasses.I3RecoPulseSeriesMap(modified_pulses)


def fix_time_overlap(pulses):
    """Fix time overlap of pulses by reducing the width of the pulses or
    by combining the pulses if the separation is less than 1ns apart

    Parameters
    ----------
    pulses : TYPE
        Description
    """
    previous_time = -float("inf")
    previous_width = 0

    fixed_pulses = []
    for p in pulses:
        # combine pulse with previous one if less than 1ns apart
        if np.abs(p.time - previous_time) < 1:
            fixed_pulses[-1].charge += p.charge
            continue

        # check if there is overlap with previous pulse
        if previous_time + 0.9 * previous_width >= p.time:
            # there is overlap: reduce width of previous pulse
            assert (
                p.time > previous_time
            ), "Pulses not ordered: {!r} !> {!r}".format(p.time, previous_time)
            fixed_pulses[-1].width = max(0.89 * (p.time - previous_time), 0.89)

        else:
            # no overlap: we can just append the pulse
            fixed_pulses.append(p)

        previous_time = p.time
        previous_width = p.width

    return fixed_pulses
