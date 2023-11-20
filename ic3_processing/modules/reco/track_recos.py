import os

from icecube import dataclasses
from icecube import icetray
from icecube import spline_reco


@icetray.traysegment
def apply_spline_mpe(tray, name, settings, output="ApplySplineMPE"):
    """Apply SplineMPE.

    Parameters
    ----------
    tray : I3Tray
        The I3Tray to which the modules should be added.
    name : str, optional
        Name of the tray segment.
    settings : dict
        A dictionary with all configuration settings.
    output : str, optional
        The frame key to which the result will be written to.
    """
    if "SplineDir" in settings:
        SplineDir = settings.pop("SplineDir")
    else:
        SplineDir = (
            "/cvmfs/icecube.opensciencegrid.org/data/photon-tables/splines/"
        )
    BareMuAmplitudeSpline = os.path.join(
        SplineDir, "InfBareMu_mie_abs_z20a10_V2.fits"
    )
    BareMuTimingSpline = os.path.join(
        SplineDir, "InfBareMu_mie_prob_z20a10_V2.fits"
    )

    if "BareMuAmplitudeSpline" not in settings:
        settings["BareMuAmplitudeSpline"] = BareMuAmplitudeSpline
    if "BareMuTimingSpline" not in settings:
        settings["BareMuTimingSpline"] = BareMuTimingSpline

    # SplineMPE expects seed to have a certain shape and fit status
    def add_track_seed(frame, seed):
        if output + "_" + seed not in frame:
            particle = dataclasses.I3Particle(frame[seed])
            particle.shape = dataclasses.I3Particle.InfiniteTrack
            particle.fit_status = dataclasses.I3Particle.FitStatus.OK
            frame[output + "_" + seed] = particle

    track_seed_list = []
    for seed in settings.pop("TrackSeedList"):
        tray.Add(add_track_seed, output + "_add_track_seed", seed=seed)
        track_seed_list.append(output + "_" + seed)

    tray.AddSegment(
        spline_reco.SplineMPE,
        name,
        TrackSeedList=track_seed_list,
        fitname=output,
        **settings,
    )

    tray.context["ic3_processing"]["HDF_keys"].append(output)
