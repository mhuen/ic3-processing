import os
from copy import deepcopy

from icecube import icetray


@icetray.traysegment
def apply_dnn_reco(tray, name="ApplyDNNRecos", cfg={}):
    """Apply one or multiple DNN reco tray segments.

    Config setup:

        DNN_reco_configs: [
            {
                pulse_map_string: ,
                DNN_excluded_doms: ,
                DNN_partial_exclusion: ,
                DNN_model_names: ,
                DNN_output_keys: , [optional]
                DNN_models_dir: , [optional]
                DNN_cascade_key: , [optional]
                DNN_batch_size: , [optional]
                DNN_output_keys: , [optional]
            },
        ]

    Parameters
    ----------
    tray : I3Tray
        The I3Tray to which the modules should be added.
    name : str, optional
        Name of the tray segment.
    cfg : dict
        A dictionary with all configuration settings.
    """
    from dnn_reco.ic3.segments import ApplyDNNRecos

    # get HDF keys from context, so that we can append new outputs
    hdf_keys = tray.context["ic3_processing"]["HDF_keys"]

    if cfg["add_dnn_reco"]:
        # collect configs for different dnn reco reconstruction segments
        if "DNN_reco_configs" not in cfg:
            dnn_reco_configs = [dict(deepcopy(cfg))]
        else:
            dnn_reco_configs = cfg["DNN_reco_configs"]

        for i, dnn_cfg in enumerate(dnn_reco_configs):
            # check for pulse map
            if "pulse_map_string" not in dnn_cfg:
                dnn_cfg["pulse_map_string"] = cfg["pulse_map_string"]

            # check for DOM exclusions
            if "DNN_excluded_doms" not in dnn_cfg:
                dnn_cfg["DNN_excluded_doms"] = cfg["DNN_excluded_doms"]

            # check for partial exclusion
            if "DNN_partial_exclusion" not in dnn_cfg:
                dnn_cfg["DNN_partial_exclusion"] = cfg["DNN_partial_exclusion"]

            # check for model names
            if "DNN_model_names" not in dnn_cfg:
                dnn_cfg["DNN_model_names"] = cfg["DNN_model_names"]

            # check for output keys
            if "DNN_output_keys" not in dnn_cfg:
                if "DNN_output_keys" not in cfg:
                    dnn_cfg["DNN_output_keys"] = None
                else:
                    dnn_cfg["DNN_output_keys"] = cfg["DNN_output_keys"]

            # check for model dir
            if "DNN_models_dir" not in dnn_cfg:
                if "DNN_models_dir" not in cfg:
                    dnn_cfg["DNN_models_dir"] = (
                        "/data/user/mhuennefeld/"
                        "DNN_reco/models/exported_models"
                    )
                else:
                    dnn_cfg["DNN_models_dir"] = cfg["DNN_models_dir"]

            # check for cascade key
            if "DNN_cascade_key" not in dnn_cfg:
                if "DNN_cascade_key" not in cfg:
                    dnn_cfg["DNN_cascade_key"] = "MCCascade"
                else:
                    dnn_cfg["DNN_cascade_key"] = cfg["DNN_cascade_key"]

            # check for batch size
            if "DNN_batch_size" not in dnn_cfg:
                if "DNN_batch_size" not in cfg:
                    dnn_cfg["DNN_batch_size"] = 64
                else:
                    dnn_cfg["DNN_batch_size"] = cfg["DNN_batch_size"]

            # check for misconfigured settings list
            if "DNN_ignore_misconfigured_settings_list" not in dnn_cfg:
                if "DNN_ignore_misconfigured_settings_list" not in cfg:
                    dnn_cfg["DNN_ignore_misconfigured_settings_list"] = None
                else:
                    dnn_cfg["DNN_ignore_misconfigured_settings_list"] = cfg[
                        "DNN_ignore_misconfigured_settings_list"
                    ]

            if dnn_cfg["DNN_output_keys"] is None:
                output_names = [
                    "DeepLearningReco_{}".format(m)
                    for m in dnn_cfg["DNN_model_names"]
                ]
            else:
                output_names = dnn_cfg["DNN_output_keys"]

            for outbox in output_names:
                if outbox not in hdf_keys:
                    hdf_keys.append(outbox)
                    hdf_keys.append(outbox + "_I3Particle")

            tray.AddSegment(
                ApplyDNNRecos,
                name + "_{:03d}".format(i),
                pulse_key=dnn_cfg["pulse_map_string"],
                dom_exclusions=dnn_cfg["DNN_excluded_doms"],
                partial_exclusion=dnn_cfg["DNN_partial_exclusion"],
                model_names=dnn_cfg["DNN_model_names"],
                output_keys=dnn_cfg["DNN_output_keys"],
                models_dir=dnn_cfg["DNN_models_dir"],
                cascade_key=dnn_cfg["DNN_cascade_key"],
                batch_size=dnn_cfg["DNN_batch_size"],
                ignore_misconfigured_settings_list=dnn_cfg[
                    "DNN_ignore_misconfigured_settings_list"
                ],
            )


@icetray.traysegment
def apply_event_generator_reco(tray, name="ApplyEventGenerator", cfg={}):
    """Apply one or multiple Event-Generator reco tray segments.

    Config setup:

        egenerator_configs: [
            {
                seed_keys: ,
                model_names: ,
                model_base_dir: , [optional]
                pulse_key: , [optional]
                dom_exclusions_key: , [optional]
                time_exclusions_key: , [optional]
                add_circular_err: , [optional]
                add_covariances: , [optional]
            },
        ]

    Parameters
    ----------
    tray : I3Tray
        The I3Tray to which the modules should be added.
    name : str, optional
        Name of the tray segment.
    cfg : dict
        A dictionary with all configuration settings.

    Raises
    ------
    ValueError
        Description
    """

    if cfg["add_egenerator_reco"]:
        from egenerator.ic3.segments import ApplyEventGeneratorReconstruction

        # get HDF keys from context, so that we can append new outputs
        hdf_keys = tray.context["ic3_processing"]["HDF_keys"]

        # collect configs for different dnn reco reconstruction segments
        egenerator_configs = cfg["egenerator_configs"]

        for i, egenerator_cfg in enumerate(egenerator_configs):
            egenerator_cfg = dict(egenerator_cfg)

            if "output_key" not in egenerator_cfg:
                model_name = egenerator_cfg["model_names"]
                if isinstance(model_name, (list, tuple)):
                    output_name = "EventGenerator_{}".format(model_name[0])
                else:
                    output_name = "EventGenerator_{}".format(model_name)
            else:
                output_name = egenerator_cfg.pop("output_key")

            if output_name not in hdf_keys:
                hdf_keys.append(output_name)
                hdf_keys.append(output_name + "_I3Particle")
                # hdf_keys.append(output_name + '_GoodnessOfFit_1sided')
                # hdf_keys.append(output_name + '_GoodnessOfFit_2sided')
                for cov in [
                    "cov",
                    "cov_sand",
                    "cov_trafo",
                    "cov_sand_trafo",
                    "cov_fit",
                    "cov_sand_fit",
                    "cov_fit_trafo",
                    "cov_sand_fit_trafo",
                    "goodness_of_fit",
                ]:
                    hdf_keys.append(output_name + "_cov_matrix_" + cov)

            # get combined DOM exclusions
            if "partial_exclusion" in egenerator_cfg:
                partial_exclusion = egenerator_cfg.pop("partial_exclusion")
            else:
                partial_exclusion = False

            # safety check:
            for deprecated_k in ["dom_exclusions_key", "time_exclusions_key"]:
                if deprecated_k in egenerator_cfg:
                    msg = "Key {} with {} is deprecated! "
                    msg += "Use dom_and_tw_exclusions instead."
                    raise ValueError(
                        msg.format(deprecated_k, egenerator_cfg[deprecated_k])
                    )

            # get DOM and time window exclusions
            if "dom_and_tw_exclusions" in egenerator_cfg:
                dom_and_tw_exclusions = egenerator_cfg.pop(
                    "dom_and_tw_exclusions"
                )
            else:
                dom_and_tw_exclusions = None

            # set BrightDOMs default if not specified: No exclusion
            if "exclude_bright_doms" in egenerator_cfg:
                exclude_bright_doms = egenerator_cfg.pop("exclude_bright_doms")
            else:
                exclude_bright_doms = False

            tray.AddSegment(
                ApplyEventGeneratorReconstruction,
                name + "_{:03d}".format(i),
                dom_and_tw_exclusions=dom_and_tw_exclusions,
                partial_exclusion=partial_exclusion,
                exclude_bright_doms=exclude_bright_doms,
                output_key=output_name,
                **egenerator_cfg,
            )


@icetray.traysegment
def apply_bdts(tray, name="ApplyBDTs", bdt_configs={}):
    """Apply one or multiple ApplyMLModel reco tray segments.

    Config setup:

        bdt_configs: [
            {
                seed_keys: ,
                model_names: ,
                model_base_dir: , [optional]
                pulse_key: , [optional]
                dom_exclusions_key: , [optional]
                time_exclusions_key: , [optional]
                add_circular_err: , [optional]
                add_covariances: , [optional]
            },
        ]

    Parameters
    ----------
    tray : I3Tray
        The I3Tray to which the modules should be added.
    name : str, optional
        Name of the tray segment.
    bdt_configs : dict
        A dictionary with all configuration settings.
    """
    from dnn_cascade_selection.utils.model import ApplyMLModel

    # get HDF keys from context, so that we can append new outputs
    hdf_keys = tray.context["ic3_processing"]["HDF_keys"]

    for i, bdt_cfg in enumerate(bdt_configs):
        if "output_key" not in bdt_cfg:
            model_path = bdt_cfg["model_path"]
            output_key = "BDT_{}".format(os.path.basename(model_path))
        else:
            output_key = bdt_cfg.pop("output_key")

        if output_key not in hdf_keys:
            hdf_keys.append(output_key)

        tray.AddModule(
            ApplyMLModel,
            name + "_{:03d}".format(i),
            output_key=output_key,
            **bdt_cfg,
        )
