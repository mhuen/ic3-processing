#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import stat
import string
import textwrap
import warnings

import click
import yaml
import getpass
import itertools
from copy import deepcopy

from typing import List, Union

from ic3_processing.cli import batch_processing

from ic3_processing.utils import setup

try:
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


def output_exists(param_dict: dict) -> bool:
    """Check if the output files already exist

    Note that this currently only works for jobs with well defined output,
    which currently encompasses i3-processing with the options `write_i3`
    and `write_hdf5`.

    Returns
    -------
    bool
        True if both the i3 and hdf5 output file already exist.
        False if only one or none of them already exist.
    """
    # get output for last processing step
    last_step = len(param_dict["processing_steps"]) - 1
    param_dict_i = get_config_for_step(param_dict, step=last_step)

    # Assume files already exist
    already_exists = True

    # Does the hdf5 file already exist?
    if param_dict_i["write_hdf5"]:
        if not os.path.isfile("{}.hdf5".format(param_dict_i["final_out"])):
            already_exists = False

    # Does the i3 file already exist?
    if param_dict_i["write_i3"]:
        if not os.path.isfile(
            "{}.{}".format(
                param_dict_i["final_out"],
                param_dict_i["i3_ending"],
            )
        ):
            already_exists = False

    return already_exists


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
    param_dict_cpy["exp_dataset_merge"] = False
    if "sframes_to_load" in param_dict_cpy:
        param_dict_cpy.pop("sframes_to_load")
    if param_dict_cpy["gcd"] == "GET_GCD_FROM_GRL":
        param_dict_cpy["gcd"] = "DUMMY_GCD"

    # get input files
    input_exists = True
    try:
        _, context = setup.setup_job_and_config(
            cfg=param_dict_cpy,
            run_number=run_number,
            scratch=False,
            verbose=False,
        )
        # no input files found, therefore skip this job
        if context["n_files"] < 1:
            input_exists = False
    except KeyError:
        # no input files found, therefore skip this job
        input_exists = False
    except IOError:
        # no input files found, therefore skip this job
        input_exists = False

    return input_exists


def get_yaml_and_pyton_paths(
    config: dict, step: int
) -> List[Union[str, os.PathLike]]:
    """Get paths to the yaml and python files for the specified sub-step

    Parameters
    ----------
    config : dict
        The main configuration that describes each of the sub-steps.
        Must also contain the `sub_process_dir` of type str | os.PathLike
        which defines the directory to which the yaml configuration
        and python scripts will be written to.
    step : int
        The processing step (0-indexed).

    Returns
    -------
    List[str | os.PathLike]
        The path to the python and yaml file for the specified processing step.
    """
    python_script_path = os.path.join(
        config["sub_process_dir"],
        config["script_name"].replace(".py", "") + f"_step_{step:04d}.py",
    )
    yaml_config_path = os.path.join(
        config["sub_process_dir"],
        config["config_base_name"] + f"_step_{step:04d}.yaml",
    )
    return python_script_path, yaml_config_path


def get_final_out_path(param_dict: dict) -> dict:
    """Get the final output path

    Parameters
    ----------
    param_dict : dict
        The parameter config. Must already have the run number
        and folder filled in.

    Returns
    -------
    dict
        The updated parameter config with the path to the final output
        file.
    """
    # fill final output file string
    param_dict["final_out"] = unescape(
        os.path.join(
            param_dict["data_folder"],
            param_dict["out_dir_pattern"].format(**param_dict),
            param_dict["folder_pattern"].format(**param_dict),
            param_dict["out_file_pattern"].format(**param_dict),
        )
    )
    param_dict["final_out_scratch"] = os.path.basename(param_dict["final_out"])

    param_dict["output_folder"] = os.path.dirname(param_dict["final_out"])

    return param_dict


def get_config_for_step(config: dict, step: int) -> dict:
    """Get the config for a given processing step number

    Parameters
    ----------
    config : dict
        The main configuration that describes each of the sub-steps.
        Must also contain the `sub_process_dir` of type str | os.PathLike
        which defines the directory to which the yaml configuration
        and python scripts will be written to.
    step : int
        The processing step [zero based].

    Returns
    -------
    dict
        The config for this processing step
    """

    # update parameters defined globally with parameters for
    # this specific processing step
    cfg_step = SafeDict()
    cfg_step.update(config)
    cfg_step.update(config["processing_steps"][step])

    # check if CPU or GPU
    if cfg_step["resources"]["gpus"] == 0:
        cfg_step["python_user_base"] = cfg_step["python_user_base_cpu"]

    elif cfg_step["resources"]["gpus"] == 1:
        cfg_step["python_user_base"] = cfg_step["python_user_base_gpu"]

    else:
        raise ValueError("More than 1 GPU is currently not supported!")

    # modify inputs to previous intermediate outputs
    if step > 0:
        cfg_step["in_file_pattern"] = os.path.join(
            "./temp_step_files",
            cfg_step["out_dir_pattern"].format(**cfg_step),
            cfg_step["folder_pattern"],
            cfg_step["out_file_pattern"]
            + f"_step{step - 1:04d}."
            + cfg_step["i3_ending"],
        )

    # modify output pattern to an intermediate output file
    if step < len(config["processing_steps"]) - 1:
        cfg_step["out_file_pattern"] = (
            cfg_step["out_file_pattern"] + f"_step{step:04d}"
        )
        cfg_step["data_folder"] = "./temp_step_files"
        if "write_i3" not in config["processing_steps"][step]:
            cfg_step["write_i3"] = True

        if "write_hdf5" not in config["processing_steps"][step]:
            cfg_step["write_hdf5"] = False

    # fill final output file string if run_number has already been filled
    if "run_number" in cfg_step:
        cfg_step = get_final_out_path(cfg_step)

    return cfg_step


def write_sub_steps(config: dict) -> List:
    """Write yaml and python files for each defined processing step

    Parameters
    ----------
    config : dict
        The main configuration that describes each of the sub-steps.
        Must also contain the `sub_process_dir` of type str | os.PathLike
        which defines the directory to which the yaml configuration
        and python scripts will be written to.

    Returns
    -------
    list
        A list of templates for each processing step.
    """

    # create directory for yaml and its sub-steps
    if os.path.exists(config["sub_process_dir"]):
        raise IOError(f"Directory {config['sub_process_dir']} already exists!")
    os.makedirs(config["sub_process_dir"])

    # go through each defined intermediate step and write
    # individual yaml configuration files for each of these steps
    templates = []
    for i in range(len(config["processing_steps"])):
        # update parameters defined globally with parameters for
        # this specific processing step
        cfg_step = get_config_for_step(config, step=i)

        # read python script for this step
        script_path = os.path.join(
            cfg_step["script_folder"],
            "scripts",
            cfg_step["script_name"],
        )
        with open(script_path) as f:
            python_script = f.read()

        # add shebang lines defining python and cvmfs version
        python_script = (
            "#!/bin/sh /cvmfs/icecube.opensciencegrid.org/"
            f"{cfg_step['cvmfs_python']}/icetray-start\n"
            f"#METAPROJECT {cfg_step['icetray_metaproject']}\n"
        ) + python_script

        # get output paths for yaml and python files
        python_path, yaml_path = get_yaml_and_pyton_paths(cfg_step, step=i)

        # write python script
        with open(python_path, "w") as f:
            f.write(python_script)
        st = os.stat(python_path)
        os.chmod(python_path, st.st_mode | stat.S_IEXEC)

        # save config
        with open(yaml_path, "w") as yaml_file:
            yaml.dump(dict(cfg_step), yaml_file, default_flow_style=False)

        # read template for this processing step
        job_template_f = os.path.join(SCRIPT_FOLDER, cfg_step["job_template"])
        if not os.path.exists(job_template_f):
            # default assumption is that the script is given as a relative
            # path to the templates within ic3_processing.
            # If this is not the case, assume the path is an absolute path
            job_template_f = cfg_step["job_template"]

        with open(job_template_f) as f:
            templates.append(f.read())

    return templates


def write_job_shell_scripts(param_dict: dict, templates: List) -> str:
    """Write executable shell scripts for each step

    Parameters
    ----------
    param_dict : dict
        The configuration options. These must already be finalized
        with all variable parameters fixed.
    templates : list
        The list of job file templates for each processing step.
    """

    base = os.path.basename(param_dict["final_out"])
    out_dir = os.path.join(param_dict["jobs_output"], "steps_" + base)

    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    temp_files = []

    # create job files for each processing step

    # SECONDS is a bash special variable that returns the seconds since set.
    wrapper_content = "#!/bin/bash"
    wrapper_content += textwrap.dedent(
        """
        # Shell-script wrapper to execute indidividual steps

        # Start Timer
        # Note: SECONDS is a bash special variable that returns the seconds
        # since it was set.
        SECONDS=0

        # Create array and index counter to keep track of time per-step
        declare -a times
        times[0]=$SECONDS
        step_counter=1
        """
    )
    wrapper_content += f"OUT_DIR={out_dir}\n\n"
    for i, template in enumerate(templates):
        # update parameters defined globally with parameters for
        # this specific processing step
        cfg_step = get_config_for_step(param_dict, step=i)

        # point to specific yaml and python files for this processing step
        python_path, yaml_path = get_yaml_and_pyton_paths(cfg_step, step=i)
        cfg_step["yaml_path"] = yaml_path
        cfg_step["python_path"] = python_path

        # keep track of temporary files:
        if i < len(templates) - 1:
            temp_files.append(cfg_step["final_out"])

        # fill job template with configuration settings
        file_config = string.Formatter().vformat(template, (), cfg_step)

        script_base = f"job_{base}_step_{i:04d}.sh"
        script_path = os.path.join(out_dir, script_base)

        # write to file
        with open(script_path, "w") as f:
            f.write(file_config)
        st = os.stat(script_path)
        os.chmod(script_path, st.st_mode | stat.S_IEXEC)

        # keep track of commands to call in wrapper job shell script
        wrapper_content += "echo &&\n"
        wrapper_content += "echo '=========================' &&\n"
        wrapper_content += f"echo '==> Starting step {i:03} <==' &&\n"
        wrapper_content += "echo '=========================' &&\n"
        wrapper_content += "echo &&\n"
        wrapper_content += 'eval "${OUT_DIR}/' + f'{script_base}" && \n'
        wrapper_content += "times[$step_counter]=$SECONDS &&\n"
        wrapper_content += "((step_counter++)) &&\n\n"

    wrapper_content += textwrap.dedent(
        """
        echo &&
        echo '=========================================' &&
        echo '==> Successfully processed all steps! <==' &&
        echo '========================================='

        RET=$?
        if [ $RET -ne 0 ] ; then
           echo
           echo '======================='
           echo '==> Error occurred! <=='
           echo '======================='
           echo
        fi

        echo
        echo 'Runtime for each processed step:'

        for (( i=1; i<$step_counter; i++ ))
        do
          printf "    %-13s --> " "Step $i"
          printf "%+8u s\n" $(( ${times[$i]} - ${times[$i-1]}))
        done
        printf "    %13s --> %+8u s\n" "Total runtime" $SECONDS
        echo

        echo Cleaning up intermediate files ...
        """
    )
    for temp_file in temp_files:
        wrapper_content += f"echo '   ... removing {temp_file}*'\n"
        wrapper_content += f"rm {temp_file}*\n"

    # remove directory if empty

    wrapper_content += "echo '   ... checking if temp_step_files exist'\n"
    wrapper_content += "if [ -d temp_step_files ]; then\n"
    wrapper_content += (
        "   echo '   ... checking if temp_step_files is empty'\n"
    )
    wrapper_content += "   lines=$(find temp_step_files/ -type f | wc -l)\n"
    wrapper_content += "   if [ $lines -eq 0 ]; then\n"
    wrapper_content += (
        "      echo '   ... removing directory temp_step_files'\n"
    )
    wrapper_content += "      rm -r temp_step_files\n"
    wrapper_content += "   else\n"
    wrapper_content += (
        "      echo '   ...    Not deleting directory as it still "
    )
    wrapper_content += "   contains files.'\n"
    wrapper_content += "   fi\n"
    wrapper_content += "else\n"
    wrapper_content += "   echo '   ...    temp_step_files does not exist'\n"
    wrapper_content += "fi\n"
    wrapper_content += "exit $RET\n"

    # write wrapper shell script that consecutively calls each processing step
    script_path = os.path.join(param_dict["jobs_output"], f"job_{base}.sh")

    with open(script_path, "w") as f:
        f.write(wrapper_content)
    st = os.stat(script_path)
    os.chmod(script_path, st.st_mode | stat.S_IEXEC)

    return script_path


def write_job_files(
    config, check_existing_output=False, check_existing_input=False
):
    """Write job files

    Parameters
    ----------
    config : dict
        The configuration settings.
    check_existing_output : bool, optional
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
    scripts = []

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

            # provide more useful errors for missing required variable
            if "data_type" not in param_dict:
                raise KeyError(
                    "The `data_type` field must be defined in the the config."
                )

            allowed_data_types = [
                "exp",
                "nugen",
                "corsika",
                "muongun",
                "misc",
                "non-i3",
            ]
            if param_dict["data_type"].lower() not in allowed_data_types:
                raise ValueError(
                    f"Data type {param_dict['data_type']} is not understood! "
                    f"Must be one of: {allowed_data_types}"
                )

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

            # create processing, log, and base out folder
            param_dict["processing_folder"] = unescape(
                os.path.join(
                    param_dict["data_folder"] + "/processing",
                    param_dict["out_dir_pattern"].format(**param_dict),
                )
            )

            # update config and save individual yaml config for each dataset
            found_unused_file_name = False
            while not found_unused_file_name:
                sub_process_dir = unescape(
                    "{config_base}_{cycler_counter:04d}".format(
                        config_base=os.path.join(
                            param_dict["processing_folder"],
                            "processing_steps",
                        ),
                        cycler_counter=cycler_counter,
                    )
                )
                if os.path.exists(sub_process_dir):
                    # there is already a config file here, so increase counter
                    cycler_counter += 1
                else:
                    found_unused_file_name = True

            cycler_counter += 1

            # create directory and write yaml configuration files for each
            # of the defined sub-processing steps
            param_dict["sub_process_dir"] = sub_process_dir
            templates = write_sub_steps(param_dict)

            # iterate through runs
            for run_num in runs:
                param_dict_i = deepcopy(param_dict)

                # add output directory for this specific run number
                param_dict_i = setup.add_run_folder_vars(
                    cfg=param_dict_i, run_number=run_num
                )

                # create sub directory for logs
                param_dict_i["log_dir"] = os.path.join(
                    param_dict_i["processing_folder"],
                    "logs",
                    param_dict_i["folder_pattern"].format(**param_dict_i),
                )

                if not os.path.isdir(param_dict_i["log_dir"]):
                    os.makedirs(param_dict_i["log_dir"])

                # create sub directory for jobs
                param_dict_i["jobs_output"] = os.path.join(
                    param_dict_i["processing_folder"],
                    "jobs",
                    param_dict_i["folder_pattern"].format(**param_dict_i),
                )
                if not os.path.isdir(param_dict_i["jobs_output"]):
                    os.makedirs(param_dict_i["jobs_output"])

                # fill final output file string
                param_dict_i = get_final_out_path(param_dict_i)

                # only create job file if outputs do not already exist
                if check_existing_output and output_exists(param_dict_i):
                    continue

                # only create job file if input files exist
                if check_existing_input and not input_exists(
                    param_dict=param_dict_i,
                    run_number=run_num,
                ):
                    continue

                # create directory for final output files
                if not os.path.isdir(param_dict_i["output_folder"]):
                    os.makedirs(param_dict_i["output_folder"])

                # write executable job shell scripts
                script_path = write_job_shell_scripts(param_dict_i, templates)

                # keep track of generated jobs
                scripts.append(script_path)

    return scripts


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
    config["config_base_name"] = os.path.basename(config_file).replace(
        ".yaml", ""
    )

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
            "Please enter the directory where the files should be stored:",
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
                "Please enter a processing scratch:",
                default=default,
            )
        config["processing_scratch"] = os.path.abspath(processing_scratch)

    script_files = write_job_files(
        config,
        check_existing_output=resume,
        check_existing_input=check_input,
    )

    if dagman or pbs or osg:

        def get_scratch_folder(counter):
            return os.path.join(
                config["processing_scratch"],
                config["config_base_name"] + f"_{counter:04d}",
            )

        counter = 0
        while os.path.exists(get_scratch_folder(counter)):
            counter += 1
        scratch_folder = get_scratch_folder(counter)

        if not os.path.isdir(scratch_folder):
            os.makedirs(scratch_folder)

        if dagman:
            batch_processing.create_dagman_files(
                config, script_files, scratch_folder
            )
        if pbs:
            batch_processing.create_pbs_files(
                config, script_files, scratch_folder
            )
        if osg:
            batch_processing.create_osg_files(
                config, script_files, scratch_folder
            )


if __name__ == "__main__":
    main()
