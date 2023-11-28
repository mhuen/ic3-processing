# ic3-processing

General processing framework to facilitate handling of i3-files on the cluster.

# Installation
To install this project via pip for usage type

    pip install git+https://github.com/mhuen/ic3-processing

If you want to make modifications and help with development, first clone the repository

    git clone https://github.com/mhuen/ic3-processing

and then install it in editable mode

    cd ic3-processing
    pip install -e .

You are encouraged to submit pull requests for your modifications here on github.

# Usage

Once installed, create executable job files via:

    ic3_job_files --dagman PATH/TO/YAML/FILE -d PATH/TO/OUTPUT

With the optional flags `--dagman` or `--osg`, files are generated to start
a DAGMan, which will perform scheduling and submission to the NPX and grid.
The created executable job files may be executed manually in a fresh shell
on one of the interactive machines (e.g. Cobalts) or on interactive jobs
(via `condor_submit -i`). This may be helpful for debugging.
Once everything works, the created DAGMan can be started by executing
the `start_dagman.sh` script that was generated in the previous step.
For default processing paths on the submitter node this is equivalent to:

    /scratch/${USER}/ic3_processing/<config_name>_<counter:04d>/start_dagman.sh

The jobs are then submitted via the DAGMan process. Progress and completion
status of the individual jobs may be viewed on the submitter node via
`condor_q`. User priority and cluster activity can be checked via
`condor_userprio`.

# Configuration Files

The processing framework is driven by configuration files in yaml. These files
consist of configuration parameters that define everything required for
the specified processing flow including:

- Job requirements (e.g. memory and walltime)
- Inputs and outputs of the jobs
- Environment (python version, icetray environment, ...)
- The processing flow may consist of a number of different scripts
that are executed consecutively. Each of these consecutive steps may define
its own environment.

In order to define the processing flow via the individual processing steps,
a list of dictionaries defined as `processing_steps` must be defined in the
config file.
Each of the entries in this list define the script and environment to execute
for a particular processing step.

## Processing of i3-files

The framework is primarily designed to facilitate processing of i3-files
via the icetray framework including `I3TraySegment`s and `I3Module`s.
A general python script `general_i3_processing.py` (in `ic3_processing/cli/scripts`) is provided that dynamically
adds modules, defined in the configuration yaml file, to the `I3Tray`.
This python script requires the definition of a list of `tray_segments`
in the `processing_steps` entry.
Examples for such configuration files are provided in:

- `resources/configs/example_i3_exp.yaml`:
Example on how to process experimental data
- `resources/configs/example_i3_mc.yaml`:
Example on how to process simulation data

## General Job processing

The framework is in principle setup to allow for general processing on the
cluster including workflows outside of i3-files. However, as of now, there
is no unified approach implemented to facilitate this. General processing
workflows will require a separate `job_template` and/or python script to be
defined. If you are interested in these applications, please contact the
`ic3_processing` maintainers.


# Directory Structure

## ic3_processing

The python package `ic3_processing` and its modules are defined in this
directory. The `utils` sub-directory contains helper functions relevant
to the processing logic of files on the cluster. These functions are
therefore relevant to the core functionality of the `ic3_processing`
framework.
The `modules` sub-directory contains `I3TraySegment`s and `I3Module`s that
may be helpful when processing i3-files. These are intended as helper modules
that can be included in the prescription of the workflow via the
`tray_segments` entries in the configuration files. Modules that are very
specific to an individual user should not be included in `ic3_processing`,
but in a dedicated python package.
The `cli` sub-directory (see next section) contains scripts
required to generate job files from the command line.

## CLI Commands
Contains the scripts required to generate job files.
After installation the available commands are

- `ic3_job_files`:
    This is the main script utilized to write executable job files based
    on a given configuration file.
    Usage:
    ```ic3_job_files PATH/TO/YAML/FILE -d PATH/TO/OUTPUT```
    Consult `--help` for further options.

- `ic3_local`:
    This script can be used to process the previously created job files
    locally in parallel.
    Consult `--help` for further options.


## Resources

Contains example configuration files and bash scripts for job execution.
