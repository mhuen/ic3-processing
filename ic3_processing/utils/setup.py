# Note: this file must unfortunately be python 2.7 compatible!
from __future__ import print_function, division
import os
import importlib
import warnings

import glob
from copy import deepcopy
import yaml

try:
    from ic3_processing.utils import file_utils
    from ic3_processing.utils.exp_data import livetime
    from ic3_processing.utils.exp_data import good_run_list_utils
except ImportError as e:
    warnings.warn(
        (
            "Could not import ic3_processing or icecube: {}.".format(e),
            "Continuing without optional support for i3 IO.",
        ),
        ImportWarning,
    )


def add_run_folder_vars(cfg, run_number):
    """Add variables regarding the output folder to the cfg.

    Parameters
    ----------
    cfg : dict
        The dictionary to update
    run_number : int
        The current run number. This number will be used to calculate
        the output directory.

    Returns
    -------
    dict
        The dictionary with updated values

    Raises
    ------
    ValueError
        If the specified number of output files in a directory is not allowed.
    """

    # Update config with run number specific settings if provided
    if "run_number_settings" in cfg:
        cfg.update(cfg["run_number_settings"][run_number])

    if "n_runs_per_merge" in cfg:
        factor = cfg["n_runs_per_merge"]
    else:
        factor = 1

    n_per_folder = cfg["n_jobs_per_folder"]

    # sanity check: only powers of 10 make sense here and usually it should be
    # less than 1000 files per directory.
    if n_per_folder not in [1, 10, 100, 1000, 10000]:
        raise ValueError(
            "Number of output files per folder (`n_jobs_per_folder`) must be "
            "one of [1, 10, 100, 1000, 10000], but is {}!".format(n_per_folder)
        )
    cfg["run_number"] = run_number
    cfg["folder_num_pre_offset"] = cfg["run_number"] // n_per_folder
    cfg["folder_num"] = (
        cfg["folder_offset"] + cfg["run_number"] // n_per_folder
    )
    cfg["folder_num_pre_offset_n_merged"] = (
        factor * cfg["run_number"]
    ) // n_per_folder
    cfg["folder_num_n_merged"] = (
        cfg["folder_offset"] + (factor * cfg["run_number"]) // n_per_folder
    )
    cfg["folder_pattern"] = cfg["folder_pattern"].format(**cfg)

    return cfg


def setup_job_and_config(cfg, run_number, scratch, verbose=True):
    """Setup Config and Job settings

    Parameters
    ----------
    cfg : str or dict
        File path to config file.
    run_number : int
        The runnumber.
    scratch : bool
        Whether or not to run on scratch.
    verbose : bool, optional
        If True, print additional information.

    Returns
    -------
    dict
        The dictionary with settings.
    dict
        Additional output values.

    Raises
    ------
    IOError
        Description
    """

    context = {}

    if isinstance(cfg, str):
        with open(cfg, "r") as stream:
            cfg = yaml.full_load(stream)
    else:
        cfg = deepcopy(cfg)

    # update environment variables if provided
    if "set_env_vars_from_python" in cfg and verbose:
        print("\n------------------------------------------------")
        print("Setting Environment Variables from within Python")
        print("------------------------------------------------")
        for key, value in cfg["set_env_vars_from_python"].items():
            os.environ[key] = str(value)
            print("    Setting {} to {}".format(key, value))
        print("------------------------------------------------\n")

    # add output directory for this specific run number
    cfg = add_run_folder_vars(cfg=cfg, run_number=run_number)

    # get input files
    if cfg["in_file_pattern"] is None:
        infile_patterns = []
    elif isinstance(cfg["in_file_pattern"], str):
        infile_patterns = [cfg["in_file_pattern"]]
    else:
        infile_patterns = cfg["in_file_pattern"]

    # walk through each pattern and collect files matching it
    infiles = []
    for pattern in infile_patterns:
        infiles_i = glob.glob(pattern.format(**cfg))

        if len(infiles_i) == 0:
            raise IOError(
                "No input files found for pattern: {}!".format(
                    pattern.format(**cfg)
                )
            )

        infiles.extend(infiles_i)

    # sort input files
    infiles = sorted(infiles)

    # check if input files are corrupt and filter them out
    if "filter_corrupted_files" in cfg and cfg["filter_corrupted_files"]:
        infiles = file_utils.filter_corrupted_files(
            infiles=infiles, verbose=True
        )

    # count how many input files we have found
    context["n_files"] = len(infiles)
    if context["n_files"] < 1 and cfg["in_file_pattern"] is not None:
        raise IOError(
            "No input files found for the patterns:\n\t {}".format(
                "\n\t".join(infile_patterns)
            )
        )

    if verbose:
        print("Found {} input files.".format(context["n_files"]))

    # merge experimental livetime and update meta data in X-frame
    if cfg["data_type"] == "exp":
        # get livetime information from all files
        if verbose:
            print("Collecting exp livetime info...")

        if not (
            "exp_dataset_run_glob" in cfg and "exp_dataset_livetime" in cfg
        ):
            cfg = livetime.collect_exp_livetime_data(infiles, cfg)

        if verbose:
            print(
                "Collected livetime of {} days over {} input files".format(
                    cfg["exp_dataset_livetime"] / 24.0 / 3600.0,
                    context["n_files"],
                )
            )

    # if it's not experimental data, it must be simulation.
    # In this case, keep track of the number of input files.
    elif cfg["data_type"].lower() != "non-i3":
        if not ("n_files_is_n_runs" in cfg and cfg["n_files_is_n_runs"]):
            # get total number of files
            if verbose:
                print("Computing total of n_files...")

            (
                context["total_n_files"],
                context["weights_meta_info_exists"],
            ) = file_utils.get_total_weight_n_files(
                infiles,
                assume_single_file=True,
            )

            if verbose:
                print(
                    "Merging weights with a total of n_files = "
                    "{} over {} input files".format(
                        context["total_n_files"], context["n_files"]
                    )
                )
        else:
            if verbose:
                print(
                    "Skipping calculation of n_runs. It is assumed that every "
                    "input file corresponds to only a single run since the "
                    "parameter 'n_files_is_n_runs' is set to true."
                )

            context["total_n_files"] = context["n_files"]
            context["weights_meta_info_exists"] = False

    # get GCD file for exp data run
    if "gcd" in cfg and cfg["gcd"] == "GET_GCD_FROM_GRL":
        if verbose:
            print(
                "Searching for GCD file for run {:08d}...".format(run_number)
            )

        cfg["gcd"] = good_run_list_utils.get_gcd_from_grl(
            grl_patterns=cfg["exp_dataset_grl_paths"],
            run_id=run_number,
        )
        if verbose:
            print("\tFound: {}".format(cfg["gcd"]))

    # prepend GCD file if provided
    if "gcd" in cfg and cfg["gcd"]:
        infiles = [cfg["gcd"]] + infiles

    # get output file destination
    if scratch:
        outfile = os.path.basename(cfg["out_file_pattern"].format(**cfg))
    else:
        outfile = os.path.join(
            cfg["data_folder"],
            cfg["out_dir_pattern"].format(**cfg),
            cfg["folder_pattern"],
            cfg["out_file_pattern"].format(**cfg),
        )

    output_dir = os.path.dirname(outfile)
    if output_dir and not os.path.isdir(output_dir):
        print("Creating output directory: \n    {}".format(output_dir))
        os.makedirs(output_dir)

    context["infiles"] = infiles
    context["outfile"] = outfile

    return cfg, context


def load_class(full_class_string):
    """
    dynamically load a class from a string

    Parameters
    ----------
    full_class_string : str
        The full class string to the given python class.
        Example:
            my_project.my_module.my_class

    Returns
    -------
    python class
        PYthon class defined by the 'full_class_string'
    """

    class_data = full_class_string.split(".")
    module_path = ".".join(class_data[:-1])
    class_str = class_data[-1]

    module = importlib.import_module(module_path)
    # Finally, we retrieve the Class
    return getattr(module, class_str)


def get_full_class_string_of_object(object_instance):
    """Get full class string of an object's class.

    o.__module__ + "." + o.__class__.__qualname__ is an example in
    this context of H.L. Mencken's "neat, plausible, and wrong."
    Python makes no guarantees as to whether the __module__ special
    attribute is defined, so we take a more circumspect approach.
    Alas, the module name is explicitly excluded from __qualname__
    in Python 3.

    Adopted from:
        https://stackoverflow.com/questions/2020014/
        get-fully-qualified-class-name-of-an-object-in-python

    Parameters
    ----------
    object_instance : object
        The object of which to obtain the full class string.

    Returns
    -------
    str
        The full class string of the object's class
    """
    module = object_instance.__class__.__module__
    if module is None or module == str.__class__.__module__:
        # Avoid reporting __builtin__
        return object_instance.__class__.__name__
    else:
        return module + "." + object_instance.__class__.__name__
