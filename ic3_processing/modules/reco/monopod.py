"""Script adopted from Tianlu Yuan's apply.py script to run monopod and taupede reconstructions"""

from icecube.filterscripts import filter_globals
from icecube.icetray import I3Units, I3Frame, traysegment
from icecube import linefit, dataclasses
from icecube.millipede import MonopodFit, TaupedeFit, HighEnergyExclusions
from icecube.dataclasses import (
    I3Particle,
    I3Direction,
    I3Position,
    I3Constants,
)
from icecube import photonics_service

# for level 2
from icecube.STTools.seededRT.configuration_services import (
    I3DOMLinkSeededRTConfigurationService,
)
from icecube import clast, cscd_llh  # noqa: F401
from icecube.lilliput.segments import (
    I3SinglePandelFitter,
    I3IterativePandelFitter,
)

# for level 3
from icecube.level3_filter_cascade.level3_Recos import (
    CascadeLlhVertexFit,
    SPEFit,
)

# from icecube.level3_filter_muon.level3_SplitHiveSplitter import (
#     SplitAndRecoHiveSplitter,
# )

import os
import copy
import logging
import pprint
import numpy as np


## pulse cleaning
def _weighted_quantile_arg(values, weights, q=0.5):
    indices = np.argsort(values)
    sorted_indices = np.arange(len(values))[indices]
    medianidx = (
        weights[indices].cumsum() / weights[indices].sum()
    ).searchsorted(q)
    if (0 <= medianidx) and (medianidx < len(values)):
        return sorted_indices[medianidx]
    else:
        return np.nan


def weighted_quantile(values, weights, q=0.5):
    if len(values) != len(weights):
        raise ValueError("shape of `values` and `weights` don't match!")
    index = _weighted_quantile_arg(values, weights, q=q)
    if not np.isnan(index):
        return values[index]
    else:
        return np.nan


def weighted_median(values, weights):
    return weighted_quantile(values, weights, q=0.5)


def pulse_cleaning(frame, Pulses, Residual=1.5e3 * I3Units.ns):
    pulses = dataclasses.I3RecoPulseSeriesMap.from_frame(frame, Pulses)
    mask = dataclasses.I3RecoPulseSeriesMapMask(frame, Pulses)
    counter, charge = 0, 0
    qtot = 0
    times = dataclasses.I3TimeWindowSeriesMap()
    all_ts = []
    all_cs = []
    for omkey, ps in pulses.items():
        ts = np.asarray([p.time for p in ps])
        all_ts.extend(ts)
        cs = np.asarray([p.charge for p in ps])
        all_cs.extend(cs)
    tw_start = max(
        weighted_quantile(np.asarray(all_ts), np.asarray(all_cs), 0.1) - 1000,
        min(all_ts),
    )
    tw_stop = min(
        weighted_quantile(np.asarray(all_ts), np.asarray(all_cs), 0.95) + 1000,
        max(all_ts),
    )
    for omkey, ps in pulses.items():
        ts = np.asarray([p.time for p in ps])
        cs = np.asarray([p.charge for p in ps])
        median = weighted_median(ts, cs)
        dts = np.ediff1d(ts)
        median_dts = np.median(dts)
        qtot += cs.sum()
        for p in ps:
            if median_dts > 1200 and len(dts) > 1:
                # attempt to mask out correlated noise
                mask.set(omkey, p, False)
            elif (
                p.time >= min(median + Residual, tw_stop) or p.time < tw_start
            ):
                latest_time = min(median + Residual, tw_stop)
                if omkey not in times:
                    tws = dataclasses.I3TimeWindowSeries()
                    tws.append(
                        dataclasses.I3TimeWindow(latest_time, np.inf)
                    )  # this defines the **excluded** time window
                    times[omkey] = tws
                mask.set(omkey, p, False)
                counter += 1
                charge += p.charge

    frame[Pulses + "PulseCleaned"] = mask
    frame[Pulses + "PulseCleanedTimeWindows"] = times
    frame[Pulses + "PulseCleanedTimeRange"] = dataclasses.I3TimeWindow(
        tw_start, tw_stop
    )


def define_excluded(ibr, isat, itw, idc, **millipede_params):
    _mparams = {
        k: copy.copy(v)
        for k, v in millipede_params.items()
        if k != "CascadePhotonicsService"
    }
    _mparams["CascadePhotonicsService"] = millipede_params[
        "CascadePhotonicsService"
    ]
    if ibr:
        _mparams["ExcludedDOMs"].remove("BrightDOMs")
    if isat:
        _mparams["ExcludedDOMs"].remove("SaturatedDOMs")
    if itw:
        # this can be used for testing against MCPEs
        _mparams["ExcludedDOMs"].remove("CalibrationErrata")
        _mparams["ExcludedDOMs"].remove("SaturationWindows")
    if idc:
        _mparams["ExcludedDOMs"].remove("DeepCoreDOMs")
    if ibr or isat or itw or idc:
        logging.info(
            "A flag --ibr or --isat or --itw or --idc was passed, the updated parameters are:"
        )
        pprint(_mparams)
    return _mparams


@traysegment
def Level2ReconstructionWrapper(tray, name, Pulses="SplitInIcePulses"):
    """
    SRT cleaning: standard level 2
    """
    tray.Add(
        "Rename",
        "rename",
        Keys=[
            "SRTCleanedInIcePulses",
            "SRT" + Pulses,
            Pulses + "UnmaskedTimeRange",
            Pulses + "TimeRange",
        ],
        If=lambda frame: frame.Has("SRTCleanedInIcePulses"),
    )
    seededRTConfig = I3DOMLinkSeededRTConfigurationService(
        ic_ic_RTRadius=150.0 * I3Units.m,
        ic_ic_RTTime=1000.0 * I3Units.ns,
        treat_string_36_as_deepcore=False,
        useDustlayerCorrection=False,
        allowSelfCoincidence=True,
    )
    tray.Add(
        "I3SeededRTCleaning_RecoPulseMask_Module",
        "seededrt",
        InputHitSeriesMapName=Pulses,
        OutputHitSeriesMapName="SRT" + Pulses,
        STConfigService=seededRTConfig,
        SeedProcedure="HLCCoreHits",
        NHitsThreshold=2,
        MaxNIterations=3,
        Streams=[I3Frame.Physics],
        If=lambda frame: not frame.Has("SRT" + Pulses),
    )

    """
    offline muon reconstruction of LineFit and SPEFit2: taken from standard level 2 filterscripts
    """
    tray.Add(
        linefit.simple,
        inputResponse="SRT" + Pulses,
        fitName="LineFit",
        If=lambda frame: "LineFit" not in frame,
    )
    tray.Add(
        I3SinglePandelFitter,
        "SPEFitSingle",
        fitname="SPEFitSingle",
        Pulses="SRT" + Pulses,
        seeds=["LineFit"],
        If=lambda frame: "SPEFit2" not in frame,
    )
    tray.Add(
        I3IterativePandelFitter,
        "SPEFit2",
        fitname="SPEFit2",
        Pulses="SRT" + Pulses,
        n_iterations=2,
        seeds=["SPEFitSingle"],
        If=lambda frame: "SPEFit2" not in frame,
    )

    """
    offline cascade reconstruction of CascadeLast and CascadeLlhVertexFit: taken from standard level 2 filterscripts
    """
    tray.Add(
        "I3CLastModule",
        "CascadeLast_L2",
        Name="CascadeLast_L2",
        InputReadout=Pulses,
        If=lambda frame: "CascadeLast_L2" not in frame,
    )
    tray.Add(
        "I3CscdLlhModule",
        "CascadeLlhVertexFit_L2",
        InputType="RecoPulse",  # ! Use reco pulses
        RecoSeries=Pulses,  # ! Name of input pulse series
        FirstLE=True,  # Default
        SeedWithOrigin=False,  # Default
        SeedKey="CascadeLast_L2",  # ! Seed fit - CLast reco
        MinHits=8,  # ! Require 8 hits
        AmpWeightPower=0.0,  # Default
        ResultName="CascadeLlhVertexFit_L2",  # ! Name of fit result
        Minimizer="Powell",  # ! Set the minimizer to use
        PDF="UPandel",  # ! Set the pdf to use
        ParamT="1.0, 0.0, 0.0, false",  # ! Setup parameters
        ParamX="1.0, 0.0, 0.0, false",  # ! Setup parameters
        ParamY="1.0, 0.0, 0.0, false",  # ! Setup parameters
        ParamZ="1.0, 0.0, 0.0, false",  # ! Setup parameters
        If=lambda frame: "CascadeLlhVertexFit_L2" not in frame,
    )

    """
    clean up
    """
    deletekeys = [
        "LineFit_HuberFit",
        "LineFit_Pulses_delay_cleaned",
        "LineFit_debiasedPulses",
        "LineFit_linefit_final_rusage",
        "SPEFitSingle",
        "SPEFitSingleFitParams",
    ]
    tray.Add("Delete", Keys=deletekeys)


################################################################
############## LEVEL 3 RECONSTRUCTION WRAPPER ##################
################################################################


@traysegment
def Level3ReconstructionWrapper(tray, name, Pulses="SplitInIcePulses"):
    """
    DOM selection: for cascade level 3 reconstructions DeepCore DOMs should be excluded
    """
    tray.Add(
        "I3OMSelection<I3RecoPulseSeries>",
        "omselection",
        InputResponse="SRT" + Pulses,
        OmittedStrings=[79, 80, 81, 82, 83, 84, 85, 86],
        OutputOMSelection="SRT" + Pulses + "_BadOMSelectionString",
        OutputResponse="SRT" + Pulses + "_IC_Singles",
    )

    """
    CascadeLlhVertexFit: standard level 3 fit on SRT Pulses without DeepCore
    """
    tray.Add(
        CascadeLlhVertexFit,
        "CascadeLlhVertexFit_L3",
        Pulses="SRT" + Pulses + "_IC_Singles",
        If=lambda frame: "CascadeLlhVertexFit_L3" not in frame,
    )

    """
    SPEFit: standard level 3 fit on SRT pulses without DeepCore (first guesses are SPEFit2 and LineFit from level 2)
    """
    tray.Add(
        SPEFit,
        "SPEFit16",
        Pulses="SRT" + Pulses + "_IC_Singles",
        Iterations=16,
        If=lambda frame: "SPEFit16" not in frame,
    )

    """
    make the cascade level 3 seed: take the best combination out of all level 2 and level 3 fits to build a seed
    """

    def addlevel3seed(frame, Output):

        # the seed particle
        seed = I3Particle()
        seed.pos = I3Position(0, 0, 0)
        seed.dir = I3Direction(0, 0)
        seed.time = 0
        seed.energy = 0.0
        seed.length = 0.0
        seed.speed = I3Constants.c
        seed.fit_status = I3Particle.OK
        seed.shape = I3Particle.Cascade
        seed.location_type = I3Particle.InIce

        # possible solutions (ordered, construct seed in any case, even if level 2 + 3 recos failed)
        vertexfits = [
            "CascadeLlhVertexFit_L3",
            "CascadeLlhVertexFit_L2",
            "CascadeLast_L2",
        ]
        directionfits = ["SPEFit16", "SPEFit2", "LineFit"]

        # vertex + time
        for vertexfitname in vertexfits:
            if vertexfitname not in frame:
                continue
            vertexfit = frame[vertexfitname]
            if (
                vertexfit.fit_status == I3Particle.OK
                and vertexfit.pos.r >= 0
                and vertexfit.time >= 0
            ):
                seed.pos = vertexfit.pos
                seed.time = vertexfit.time
                break

        # direction
        for directionfitname in directionfits:
            if directionfitname not in frame:
                continue
            directionfit = frame[directionfitname]
            if (
                directionfit.fit_status == I3Particle.OK
                and directionfit.dir.theta >= 0
                and directionfit.dir.phi >= 0
            ):
                seed.dir = directionfit.dir
                break

        # save it
        frame.Put(Output, seed)

    tray.Add(addlevel3seed, Output=name, If=lambda frame: name not in frame)

    """
    clean up
    """
    deletekeys = [
        "CascadeLlhVertexFit_L3_CLastSeed",
        "CascadeLlhVertexFit_L3_CLastSeedParams",
    ]
    deletekeys += [
        "SRT" + Pulses + "_BadOMSelectionString",
        "SRT" + Pulses + "_IC_Singles",
        "SRT" + Pulses + "_IC_SinglesCleanedKeys",
    ]
    tray.Add("Delete", keys=deletekeys)


def convert_to_tau_seed(frame, inkey, outkey):
    pp = dataclasses.I3Particle(frame[inkey])
    pp.length = 5
    pp.speed = I3Constants.c
    pp.location_type = I3Particle.InIce
    pp.shape = I3Particle.ParticleShape.InfiniteTrack
    frame[outkey] = pp


@traysegment
def MonopodWrapper(
    tray,
    name,
    Seed="CombinedCascadeSeed_L3",
    Iterations=4,
    PhotonsPerBin=15,
    BrightsFit=False,
    SaturatedFit=False,
    BadTimeWindowsFit=False,
    DeepCoreFit=False,
    **millipede_params
):
    isspecial = BrightsFit or SaturatedFit or BadTimeWindowsFit or DeepCoreFit
    ext_ifseed = "Seed" if isspecial else ""
    tray.Add(
        MonopodFit,
        name + ext_ifseed,
        Seed=Seed,
        Iterations=Iterations,
        PhotonsPerBin=PhotonsPerBin,
        **millipede_params
    )
    _mparams = define_excluded(
        BrightsFit,
        SaturatedFit,
        BadTimeWindowsFit,
        DeepCoreFit,
        **millipede_params
    )
    if isspecial:
        tray.Add(
            MonopodFit,
            name,
            Seed=name + ext_ifseed,
            Iterations=1,
            StepT=10,
            StepD=3,
            StepZenith=5,
            StepAzimuth=5,
            PhotonsPerBin=PhotonsPerBin,
            **_mparams
        )


@traysegment
def TaupedeWrapper(
    tray,
    name,
    Seed="CombinedCascadeSeed_L3",
    Iterations=4,
    PhotonsPerBin=15,
    BrightsFit=False,
    SaturatedFit=False,
    BadTimeWindowsFit=False,
    DeepCoreFit=False,
    **millipede_params
):
    monopod0 = name + "Monopod"
    tray.Add(
        MonopodWrapper,
        monopod0,
        Seed=Seed,
        Iterations=Iterations,
        PhotonsPerBin=PhotonsPerBin,
        BrightsFit=BrightsFit,
        SaturatedFit=SaturatedFit,
        BadTimeWindowsFit=BadTimeWindowsFit,
        DeepCoreFit=DeepCoreFit,
        **millipede_params
    )
    tray.Add(convert_to_tau_seed, inkey=monopod0, outkey=f"{name}_Seed")

    _mparams = define_excluded(
        BrightsFit,
        SaturatedFit,
        BadTimeWindowsFit,
        DeepCoreFit,
        **millipede_params
    )
    tray.Add(
        TaupedeFit,
        name + "Taupede",
        Seed=f"{name}_Seed",
        StepL=10,
        StepT=5,
        StepD=2,
        StepZenith=10,
        StepAzimuth=10,
        LengthBounds=[0, 200],
        PhotonsPerBin=PhotonsPerBin,
        **_mparams
    )


def good_frame(frame):
    if frame["I3EventHeader"].sub_event_stream != filter_globals.InIceSplitter:
        return False

    return True


def mask_ic(frame, origpulses, maskedpulses):
    frame[maskedpulses] = dataclasses.I3RecoPulseSeriesMapMask(
        frame, origpulses, lambda omkey, index, pulse: omkey.string < 79
    )


def preferred(frame, i3_particle_keys, output_key="PreferredFit"):
    bestlogl = np.inf
    pref = ""
    for pkey in i3_particle_keys:
        _logl = frame[f"{pkey}FitParams"].logl
        if _logl < bestlogl:
            bestlogl = _logl
            pref = pkey
    frame[output_key] = I3Particle(frame[pref])
    frame[output_key + "_key"] = dataclasses.I3String(pref)


@traysegment
def MonopodTaupedePreferredSegment(
    tray,
    name,
    output_key="PreferredFit",
    pulse_key="SplitInIcePulses",
    Seed="CombinedCascadeSeed_L3",
    idc=False,  # no DC for now
    spline_table_dir=os.path.expandvars("$I3_DATA/photon-tables/splines"),
    bdthres=15,
    icemodel="ftp-v1",
):

    tilttabledir = os.path.expandvars(
        f"$I3_BUILD/ice-models/resources/models/ICEMODEL/spice_{icemodel}/"
    )
    eff_distance = os.path.join(
        spline_table_dir,
        f"cascade_effectivedistance_spice_{icemodel}_z20.eff.fits",
    )
    eff_distance_prob = os.path.join(
        spline_table_dir,
        f"cascade_effectivedistance_spice_{icemodel}_z20.prob.fits",
    )
    eff_distance_tmod = os.path.join(
        spline_table_dir,
        f"cascade_effectivedistance_spice_{icemodel}_z20.tmod.fits",
    )
    bulk_ice_fmt = f"cascade_single_spice_{icemodel}_flat_z20_a5"

    assert os.path.isdir(tilttabledir) or not tilttabledir
    assert os.path.isfile(eff_distance) or not eff_distance
    assert os.path.isfile(
        os.path.join(spline_table_dir, f"{bulk_ice_fmt}.abs.fits")
    )
    assert os.path.isfile(
        os.path.join(spline_table_dir, f"{bulk_ice_fmt}.prob.v2.fits")
    )

    cs_args = dict(
        amplitudetable=os.path.join(
            spline_table_dir, f"{bulk_ice_fmt}.abs.fits"
        ),
        timingtable=os.path.join(
            spline_table_dir, f"{bulk_ice_fmt}.prob.v2.fits"
        ),
        effectivedistancetable=eff_distance,
        effectivedistancetableprob=eff_distance_prob,
        effectivedistancetabletmod=eff_distance_tmod,
        tiltTableDir=tilttabledir,
        quantileEpsilon=1,
    )
    cascade_service = photonics_service.I3PhotoSplineService(**cs_args)

    spline_table_dir = os.path.expandvars("$I3_DATA/photon-tables/splines")

    ##################################################################
    # Run
    ##################################################################
    # Note that this example illustrates two ways to use this code.  You can use the
    # code as tray segment, or as 4 modules.

    tray.Add(Level2ReconstructionWrapper, "level2reco", Pulses=pulse_key)
    tray.Add(
        Level3ReconstructionWrapper, "CombinedCascadeSeed_L3", Pulses=pulse_key
    )

    tray.Add(
        mask_ic,
        origpulses=pulse_key,
        maskedpulses=f"{pulse_key}IC",
        If=lambda frame: not frame.Has(f"{pulse_key}IC"),
    )

    pulses_for_reco = pulse_key if idc else f"{pulse_key}IC"
    tray.Add(
        pulse_cleaning,
        Pulses=pulses_for_reco,
        Residual=1500,
        If=lambda frame: not frame.Has(pulses_for_reco + "PulseCleaned"),
    )
    excludedDOMs = tray.Add(
        HighEnergyExclusions,
        Pulses=pulses_for_reco,
        BrightDOMThreshold=bdthres,
        BadDomsList="BadDomsList",
        CalibrationErrata="CalibrationErrata",
        SaturationWindows="SaturationWindows",
        If=lambda frame: not frame.Has("SaturatedDOMs"),
    )
    # this isn't placed in by default as SaturatedDOMs are excluded fully
    # here we decide later in the MonopodWrapper
    excludedDOMs.append("SaturationWindows")
    excludedDOMs.append(pulses_for_reco + "PulseCleanedTimeWindows")

    millipede_params = {
        "Pulses": f"{pulses_for_reco}PulseCleaned",
        "CascadePhotonicsService": cascade_service,
        "MuonPhotonicsService": None,
        "ExcludedDOMs": excludedDOMs,
        "ReadoutWindow": f"{pulses_for_reco}PulseCleanedTimeRange",
        "PartialExclusion": True,
        "UseUnhitDOMs": True,
        "MinTimeWidth": 8,
        "RelUncertainty": 0.05,
    }
    # Run as a tray segment
    tray.Add(
        TaupedeWrapper,
        output_key,
        Seed=Seed,
        Minimizer="iMIGRAD",
        PhotonsPerBin=0,
        BrightsFit=False,
        SaturatedFit=False,
        BadTimeWindowsFit=False,
        DeepCoreFit=idc,
        **millipede_params
    )

    prefs = [output_key + "Taupede", output_key + "Monopod"]
    tray.Add(
        preferred,
        i3_particle_keys=prefs,
        output_key=output_key,
        If=lambda f: len(prefs) > 0 and all([f.Has(_) for _ in prefs]),
    )

    tray.context["ic3_processing"]["HDF_keys"].extend(prefs)
    tray.context["ic3_processing"]["HDF_keys"].append(output_key)
