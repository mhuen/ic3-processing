#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import stat
import string
import warnings

import click
import yaml
import getpass
import itertools
from copy import deepcopy

import batch_processing


try:
    from ic3_processing.utils.setup import setup
    from ic3_processing.utils.exp_data.good_run_list_utils import (
        get_exp_dataset_jobs,
    )
except ImportError as e:
    warnings.warn(f"Could not import ic3_processing or icecube: {e}.")
    warnings.warn("Continuing without optional support exp datasets.")

SCRIPT_FOLDER = os.path.dirname(os.path.abspath(__file__))
ESCAPE_CHARS = ["=", " ", "\\"]


class SafeDict(dict):
    def __missing__(self, key):
        return "{" + key + "}"


def escape(file_path):
    """Escape characters from a file path. Inverse of uescape().

    Parameters
    ----------
    file_path : str
        The file path that should be escaped

    Returns
    -------
    str
        The file path with characters escaped.
    """
    for escape_char in ESCAPE_CHARS:
        file_path = file_path.replace(escape_char, "\\" + escape_char)
    return file_path


def unescape(file_path):
    """*Unescape* characters from a file path. Inverse of escape().

    Parameters
    ----------
    file_path : str
        The file path that should be uescaped

    Returns
    -------
    str
        The file path with characters uescaped.
    """
    for escape_char in ESCAPE_CHARS:
        file_path = file_path.replace("\\" + escape_char, escape_char)
    return file_path


def get_version() -> str:
    """Get version of ic3-processing framework

    Returns
    -------
    str
        The version of the ic3-processing framework
    """
    try:
        import ic3_processing

        version = "{}.{}".format(
            ic3_processing.__version_major__,
            ic3_processing.__version_minor__,
        )
    except ImportError:
        about = {}
        with open(
            os.path.join(SCRIPT_FOLDER, "../ic3_processing", "__about__.py")
        ) as fobj:
            exec(fobj.read(), about)
        version = "{}.{}".format(
            about["__version_major__"],
            about["__version_minor__"],
        )
    return version


def input_exists(param_dict: dict, run_number: int) -> bool:
    """Check if input files exist for an individual job config.

    Parameters
    ----------
    param_dict : dict
        The configuration dictionary that defines the individual job.
    run_number : int
        The current run number.

    Returns
    -------
    bool
        Returns True if the input file exists, otherwise False.
    """

    # create copy of current config to not mess anything up
    param_dict_cpy = deepcopy(param_dict)

    # set additional checks to false
    param_dict_cpy["filter_corrupted_files"] = False
    param_dict_cpy["merge_weights"] = False
    param_dict_cpy["exp_dataset_merge"] = False
    if "sframes_to_load" in param_dict_cpy:
        param_dict_cpy.pop("sframes_to_load")
    if param_dict_cpy["gcd"] == "GET_GCD_FROM_GRL":
        param_dict_cpy["gcd"] = "DUMMY_GCD"

    # get input files
    input_exists = True
    try:
        _, _, _, context = setup.setup_job_and_config(
            cfg=param_dict_cpy,
            run_number=run_number,
            scratch=False,
            verbose=False,
        )
    except KeyError:
        # no input files found, therefore skip this job
        input_exists = False
    except IOError:
        # no input files found, therefore skip this job
        input_exists = False

    # no input files found, therefore skip this job
    if context["n_files"] < 1:
        input_exists = False

    return input_exists


def write_job_files(config, check_existing=False, check_existing_input=False):
    """Write job files

    Parameters
    ----------
    config : dict
        The configuration settings.
    check_existing : bool, optional
        If true, job files will only be written if the output file does not
        exist yet.
    check_existing_input : bool, optional
        If true, job files will only be written if the input files exist.

    Returns
    -------
    list
        List of script file paths for each job.
    list
        List of run numbers for each job.

    Raises
    ------
    ValueError
        Description
    """
    with open(config["job_template"]) as f:
        template = f.read()

    scripts = []
    run_numbers = []

    # go through all datasets defined in config
    for dataset in config["datasets"]:
        print("Now creating job files for dataset: {}".format(dataset))
        dataset_dict = SafeDict(config["datasets"][dataset])
        dataset_dict["dataset_name"] = dataset

        # create dummy cycler if none is provided
        if "cycler" not in dataset_dict:
            dataset_dict["cycler"] = {"dummy": [True]}

        # create list of parameters to cycle through
        param_names = []
        param_values_list = []
        for cycle_param in dataset_dict["cycler"]:
            param_names.append(cycle_param)
            param_values_list.append(dataset_dict["cycler"][cycle_param])

        cycler_counter = 0
        # Now go through each configuration and
        # create job files
        for param_values in itertools.product(*param_values_list):
            # create param_dict for this set of configurations
            param_dict = SafeDict()

            # copy settings from main config
            # and general dataset config
            # into this dictionary
            param_dict.update(config)
            param_dict.update(dataset_dict)

            # now update parameters from cycler
            for name, value in zip(param_names, param_values):
                param_dict[name] = value

            # Check if this is an experimental data dataset and get run numbers
            if "exp_dataset_run_glob" in param_dict:
                runs, param_dict = get_exp_dataset_jobs(param_dict)
            elif "runs_list" in param_dict:
                # use the list of runs as specified in the config
                runs = param_dict["runs_list"]
            else:
                # get a list of run numbers to process
                runs = range(*param_dict["runs_range"])

            # ignore certain runs if these are provided:
            if "runs_to_ignore" in param_dict:
                runs = [
                    r for r in runs if r not in param_dict["runs_to_ignore"]
                ]

            # check if CPU or GPU
            if param_dict["resources"]["gpus"] == 0:
                param_dict["python_user_base"] = param_dict[
                    "python_user_base_cpu"
                ]
            elif param_dict["resources"]["gpus"] == 1:
                param_dict["python_user_base"] = param_dict[
                    "python_user_base_gpu"
                ]
            else:
                raise ValueError("More than 1 GPU is currently not supported!")

            # create processing, log, and base out folder
            param_dict["processing_folder"] = unescape(
                os.path.join(
                    param_dict["data_folder"] + "/processing",
                    param_dict["out_dir_pattern"].format(**param_dict),
                )
            )

            jobs_output_base = os.path.join(
                param_dict["processing_folder"], "jobs"
            )

            if not os.path.isdir(jobs_output_base):
                os.makedirs(jobs_output_base)

            log_dir_base = os.path.join(
                param_dict["processing_folder"], "logs"
            )

            if not os.path.isdir(log_dir_base):
                os.makedirs(log_dir_base)

            # update config and save individual yaml config for each dataset
            param_dict["scratchfile_pattern"] = unescape(
                os.path.basename(param_dict["out_file_pattern"])
            )

            found_unused_file_name = False
            while not found_unused_file_name:
                filled_yaml = unescape(
                    "{config_base}_{cycler_counter:04d}".format(
                        config_base=os.path.join(
                            param_dict["processing_folder"],
                            param_dict["config_base_name"],
                        ),
                        cycler_counter=cycler_counter,
                    )
                )
                if os.path.exists(filled_yaml):
                    # there is already a config file here, so increase counter
                    cycler_counter += 1
                else:
                    found_unused_file_name = True

            cycler_counter += 1
            param_dict["yaml_copy"] = filled_yaml
            with open(param_dict["yaml_copy"], "w") as yaml_copy:
                yaml.dump(
                    dict(param_dict), yaml_copy, default_flow_style=False
                )

            # iterate through runs
            completed_run_folders = []
            for run_num in runs:
                # add output directory for this specific run number
                param_dict = setup.add_run_folder_vars(
                    cfg=param_dict, run_number=run_num
                )

                # fill final output file string
                final_out = unescape(
                    os.path.join(
                        param_dict["data_folder"],
                        param_dict["out_dir_pattern"].format(**param_dict),
                    )
                )

                if param_dict["merge_files"]:
                    param_dict["log_dir"] = log_dir_base
                    jobs_output = jobs_output_base
                else:
                    # create sub directory for logs
                    param_dict["log_dir"] = os.path.join(
                        log_dir_base,
                        param_dict["run_folder"].format(**param_dict),
                    )

                    if not os.path.isdir(param_dict["log_dir"]):
                        os.makedirs(param_dict["log_dir"])

                    # create sub directory for jobs
                    jobs_output = os.path.join(
                        jobs_output_base,
                        param_dict["run_folder"].format(**param_dict),
                    )
                    if not os.path.isdir(jobs_output):
                        os.makedirs(jobs_output)

                    final_out = os.path.join(
                        final_out,
                        param_dict["run_folder"].format(**param_dict),
                    )

                final_out = os.path.join(
                    final_out,
                    param_dict["out_file_pattern"].format(**param_dict),
                )

                param_dict["final_out"] = unescape(final_out)

                if param_dict["merge_files"]:
                    if param_dict["run_folder"] in completed_run_folders:
                        # skip if the folder has already been taken care of
                        continue
                    else:
                        # remember which folders have been taken care of
                        completed_run_folders.append(param_dict["run_folder"])

                if check_existing:
                    # Assume files already exist
                    already_exists = True

                    # Does the hdf5 file already exist?
                    if param_dict["write_hdf5"]:
                        if not os.path.isfile(
                            "{}.hdf5".format(param_dict["final_out"])
                        ):
                            already_exists = False

                    # Does the i3 file already exist?
                    if param_dict["write_i3"]:
                        if not os.path.isfile(
                            "{}.{}".format(
                                param_dict["final_out"],
                                param_dict["i3_ending"],
                            )
                        ):
                            already_exists = False

                    # The files which are to be written,
                    # already exist. So skip these files
                    if already_exists:
                        continue

                # only create job file if input files exist
                if check_existing_input and not input_exists(
                    param_dict=param_dict,
                    run_number=run_num,
                ):
                    continue

                output_folder = os.path.dirname(final_out)
                if not os.path.isdir(output_folder):
                    os.makedirs(output_folder)
                param_dict["output_folder"] = output_folder
                file_config = string.Formatter().vformat(
                    template, (), param_dict
                )
                script_name = "job_{final_out_base}.sh".format(
                    final_out_base=os.path.basename(param_dict["final_out"]),
                    **param_dict,
                )
                script_path = os.path.join(jobs_output, script_name)
                with open(script_path, "w") as f:
                    f.write(file_config)
                st = os.stat(script_path)
                os.chmod(script_path, st.st_mode | stat.S_IEXEC)
                scripts.append(script_path)
                run_numbers.append(run_num)
    return scripts, run_numbers


@click.command()
@click.argument("config_file", type=click.Path(exists=True))
@click.option(
    "--data_folder",
    "-d",
    default=None,
    help="folder were all files should be placed",
)
@click.option(
    "--processing_scratch",
    "-p",
    default=None,
    help="Folder for the DAGMAN Files",
)
@click.option(
    "--dagman/--no-dagman",
    default=False,
    help="Write/Not write files to start dagman process.",
)
@click.option(
    "--pbs/--no-pbs",
    default=False,
    help="Write/Not write files to start processing on a pbs system",
)
@click.option(
    "--osg/--no-osg",
    default=False,
    help="Write/Not write files to start processing on OSG.",
)
@click.option(
    "--resume/--no-resume",
    default=False,
    help="Resume processing -> check for existing output",
)
@click.option(
    "--check_input/--no-check_input",
    default=False,
    help="Only create job files if input files exist.",
)
def main(
    data_folder,
    config_file,
    processing_scratch,
    pbs,
    osg,
    dagman,
    resume,
    check_input,
):
    config_file = click.format_filename(config_file)
    with open(config_file, "r") as stream:
        config = SafeDict(yaml.full_load(stream))
    config["script_folder"] = SCRIPT_FOLDER
    config["config_base_name"] = os.path.basename(os.path.join(config_file))

    # add ic3_processing version as a config variable
    config["ic3_processing_version"] = get_version()

    if "check_existing_input" in config:
        if config["check_existing_input"] != check_input:
            print('Found "check_existing_input" in config.')
            print(
                'Overwriting setting "check_input" from {} to {}'.format(
                    check_input, config["check_existing_input"]
                )
            )
            check_input = config["check_existing_input"]

    if "data_folder" in config:
        data_folder = config["data_folder"]
        print(
            'Found "data_folder" variable in config.\n'
            "Adjusting data output path to:\n\t{}".format(data_folder)
        )

    if data_folder is None:
        default = f"/data/user/{getpass.getuser()}/ic3_processing/data/"
        data_folder = click.prompt(
            "Please enter the dir were the files should be stored:",
            default=default,
        )
    data_folder = os.path.abspath(data_folder)
    if data_folder.endswith("/"):
        data_folder = data_folder[:-1]
    config["data_folder"] = data_folder

    if dagman or pbs or osg:
        if processing_scratch is None:
            default = "/scratch/{}/ic3_processing".format(getpass.getuser())
            processing_scratch = click.prompt(
                "Please enter a processing scratch:", default=default
            )
        config["processing_scratch"] = os.path.abspath(processing_scratch)

    script_files, run_numbers = write_job_files(
        config, check_existing=resume, check_existing_input=check_input
    )

    if dagman or pbs or osg:
        scratch_subfolder = "{}_{}".format(
            config["script_name"].replace(".py", ""),
            config["config_base_name"].replace(".yaml", ""),
        )
        scratch_folder = os.path.join(
            config["processing_scratch"], scratch_subfolder
        )

        if not os.path.isdir(scratch_folder):
            os.makedirs(scratch_folder)

        if dagman:
            batch_processing.create_dagman_files(
                config, script_files, run_numbers, scratch_folder
            )
        if pbs:
            batch_processing.create_pbs_files(
                config, script_files, run_numbers, scratch_folder
            )
        if osg:
            batch_processing.create_osg_files(
                config, script_files, run_numbers, scratch_folder
            )


if __name__ == "__main__":
    main()
