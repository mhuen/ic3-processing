# -*- coding: utf-8 -*-
"""Helper functions for GRL reading and exp. data processing

Note: some functions are adopted from
https://code.icecube.wisc.edu/projects/icecube/browser/IceCube/sandbox/
ps_processing/trunk/resources/iceprod2_scripts/build_data_task_json.py
"""
from __future__ import print_function, division
import os
import glob
import calendar
import tarfile
import re
import numpy as np
from icecube import dataclasses, dataio


# Define columns in GRL file
GRL_COLUMNS = [
    "RunNum",
    "Good_i3",
    "Good_i",
    "LiveTime",
    "ActiveStrings",
    "ActiveDOMs",
    "ActiveInIce",
    "OutDir",
    "Comments",
]
GRL_COLUMNS_DTYPE = [
    int,
    int,
    int,
    float,
    int,
    int,
    int,
    str,
    str,
]
GRL_COLUMN_INDEX_DICT = {col: i for i, col in enumerate(GRL_COLUMNS)}


def parse_grl_lists(grl_patterns):
    """Parse GRL files

    Parameters
    ----------
    grl_patterns : str or list of str
        The GRL files or glob file patterns to parse.

    Returns
    -------
    pandas.DataFrame
        The parsed GRL data as a pandas dataframe.
    dict
        The parsed GRL data as a dictionary with the run id (int) as a key.
        Values are a list containing of (in order):
            - Good_i3
            - Good_i
            - LiveTime
            - ActiveStrings
            - ActiveDOMs
            - ActiveInIce
            - OutDir
            - Comments
    """

    if isinstance(grl_patterns, str):
        grl_patterns = [grl_patterns]

    grl_lists = []
    for grl_pattern in grl_patterns:
        grl_lists.extend(glob.glob(grl_pattern))
    grl_lists = np.unique(grl_lists)

    df_dict = {col: [] for col in GRL_COLUMNS}
    grl_dict = {}

    for grl_list in grl_lists:
        with open(grl_list, "r") as f:
            lines = f.readlines()

        # make sure the first two lines are just headers
        assert "RunNum" in lines[0], lines[0]
        if "(1=good 0=bad)" in lines[1]:
            start_index = 2
        else:
            start_index = 1

            # safety checks
            cols = re.sub(" +", " ", lines[1]).split(" ")
            assert len(cols) >= len(GRL_COLUMNS), cols
            assert cols[0].isdigit(), cols[0]

        for line in lines[start_index:]:
            # remove duplicate whitespaces
            line = re.sub(" +", " ", line)

            # separate into columns
            cols = line.split(" ")

            # combine comments back into one
            comment = cols[len(GRL_COLUMNS) - 1]
            for col in cols[len(GRL_COLUMNS) :]:
                comment += " " + col
            comment = comment.replace("\n", "")

            if len(cols) >= len(GRL_COLUMNS):
                cols = cols[: len(GRL_COLUMNS)]
                cols[len(GRL_COLUMNS) - 1] = comment

            assert len(cols) == len(GRL_COLUMNS), (cols, GRL_COLUMNS)

            # fix data types of cols
            for i in range(len(cols)):
                cols[i] = GRL_COLUMNS_DTYPE[i](cols[i])

            # check if run already exists
            run_num = int(cols[0])
            if run_num in grl_dict:
                raise ValueError(
                    "Found Duplicate: {} and {}".format(
                        grl_dict[run_num], line
                    )
                )

            grl_dict[run_num] = cols
            for col, name in zip(cols, GRL_COLUMNS):
                df_dict[name].append(col)

    return df_dict, grl_dict


def read_from_gaps(
    run,
    filenumbers,
    filelist=[],
    start=True,
    is_tar=True,
    contents=[],
    i3filelist=[],
):
    """Read GAPs files

    Read the gaps files to find the start/end times for the good selections.

    Parameters
    ----------
    run : TYPE
        Description
    filenumbers : TYPE
        Description
    filelist : list, optional
        Description
    start : bool, optional
        Description
    is_tar : bool, optional
        Description
    contents : list, optional
        Description
    i3filelist : list, optional
        Description

    Returns
    -------
    TYPE
        Description
    """
    mjd_times = []
    for fnum in filenumbers:
        subrun_text = "{:08d}_gaps.txt".format(fnum)
        gaps_file = [f for f in contents if subrun_text in f]
        if len(gaps_file) == 0:
            print("\tMissing gaps file for run {} subrun {}".format(run, fnum))
            print("\tFalling back on L2 file")
            # If this doesn't exist, we get to enjoy some pain.
            # print(sorted(i3filelist))
            print("{:08d}.i3".format(fnum))
            i3file = [f for f in i3filelist if "{:08d}.i3".format(fnum) in f]
            i3file = dataio.I3File(i3file[0], "r")
            while i3file.more():
                frame = i3file.pop_frame()
                if "I3EventHeader" not in frame:
                    continue
                header = frame["I3EventHeader"]
                if start:
                    break
            if start:
                mjd_times.append(header.start_time.mod_julian_day_double)
            else:
                mjd_times.append(header.end_time.mod_julian_day_double)
        else:
            # Otherwise, life is relatively easy
            if is_tar:
                lines = filelist.extractfile(gaps_file[0]).readlines()
            else:
                with open(gaps_file[0], "r") as file:
                    lines = file.readlines()

            # decode
            lines = [line.decode() for line in lines]

            # check last line
            last_line = lines[-1].split()
            assert "File" == last_line[0], last_line
            assert "Livetime:" == last_line[1], last_line

            if start:
                line = lines[1].split()
                i3time = dataclasses.I3Time(int(line[-2]), int(line[-1]))
                mjd_times.append(i3time.mod_julian_day_double)
            else:
                num_lines = len(lines)
                if num_lines == 4:
                    line = lines[2].split()
                    i3time = dataclasses.I3Time(int(line[-2]), int(line[-1]))
                    mjd_times.append(i3time.mod_julian_day_double)

                # If there are more lines in the gaps file, there must be gaps
                else:
                    print("\tlines:", lines)
                    print("\tFound GAPs in gaps file:")
                    for line in lines[2 : num_lines - 2]:
                        print("\t\t", line.replace("\n", ""))
                        assert "Gap Detected:" in line, line
                    print("\tWARNING: these gaps will be ignored!")

                    # check last line
                    last_line = lines[-1].split()
                    assert "File" == last_line[0], last_line
                    assert "Livetime:" == last_line[1], last_line

                    line = lines[-2].split()
                    assert "Last" == line[0], line
                    i3time = dataclasses.I3Time(int(line[-2]), int(line[-1]))
                    mjd_times.append(i3time.mod_julian_day_double)
    return mjd_times


def find_gcd_file(run_dir, gcd_pattern="Level2*GCD*.i3*"):
    """Find GCD file in specified run directory

    Parameters
    ----------
    run_dir : TYPE
        Description
    gcd_pattern : str, optional
        The glob file pattern to find the GCD files
    """
    gcd_glob = os.path.join(run_dir, gcd_pattern)
    gcd_files = sorted(glob.glob(gcd_glob))

    if len(gcd_files) == 0:
        raise ValueError("No GCD File for: {}".format(gcd_glob))
    if len(gcd_files) > 1:
        print("Found multiple GCD files. Picking first. {}".format(gcd_files))

    gcd_file = gcd_files[0]
    return gcd_file


def get_gcd_from_grl(grl_patterns, run_id, gcd_pattern="Level2*GCD*.i3*"):
    """Get GCD file from GoodRunList (GRL)

    Parameters
    ----------
    grl_patterns : str or list of str
        The GRL files or file globs to parse.
    run_id : int
        The run number of the exp data run for which to retrieve the GCD file.
    gcd_pattern : str, optional
        The glob file pattern to find the GCD files
    """

    # get info from provided GRL files
    grl_df_dict, grl_dict = parse_grl_lists(grl_patterns)

    # get exp data run directory which also contains the GCD file
    run_dir = grl_dict[run_id][GRL_COLUMN_INDEX_DICT["OutDir"]]

    # get GCD file
    gcd_file = find_gcd_file(run_dir=run_dir, gcd_pattern=gcd_pattern)

    return gcd_file


def get_run_info(
    run_dir,
    year,
    date,
    run_id,
    gcd_pattern="Level2*GCD*.i3*",
    subrun_pattern="Level2*Run*_Subrun00000000_????????.i3.zst",
    gaps_tar_bases=[
        "/data/exp/IceCube/{year}/filtered/level2pass2/{date}/"
        "Run{run:08d}_GapsTxt.tar",
        "/data/exp/IceCube/{year}/filtered/level2pass2/{date}/"
        "Run{run:08d}/Run{run:08d}_GapsTxt.tar",
    ],
    gaps_txt_base=(
        "/data/exp/IceCube/{year}/filtered/level2pass2/{date}/"
        "Run{run:08d}/*_gaps.txt"
    ),
):
    """Get Run Info

    Collects all information relevant to the run being processed:
        - GCD input file
        - List of input files
        - Livetime
        - GRL start times
        - GRL stop times

    Parameters
    ----------
    run_dir : str
        The path to the run directory.
    year : str
        The data year.
    date : str
        The date formatted as mmdd
    run_id : int or str
        The run number.
    gcd_pattern : str, optional
        The glob file pattern to find the GCD files
    subrun_pattern : str, optional
        The glob file pattern to find the subrun files.
    gaps_tar_bases : list, optional
        The file bases for the subrun gap tar files.
    gaps_txt_base : str, optional
        The file bases for the subrun gap txt files.

    Returns
    -------
    str
        The path to the GCD file.
    list of str
        A list of file input paths.
    float
        The livetime for this run.
    list of float
        The start times in mjd.
    list of float
        The stop times in mjd.
    """

    # ------------
    # get GCD file
    # ------------
    gcd_file = find_gcd_file(run_dir=run_dir, gcd_pattern=gcd_pattern)

    # -------------------
    # Get list of subruns
    # -------------------
    subrun_glob = os.path.join(run_dir, subrun_pattern)
    subrun_files = sorted(glob.glob(subrun_glob))

    # get list of subrun ids
    subrun_ids = []
    for subrun_file in subrun_files:
        basename = os.path.basename(subrun_file)
        try:
            subrun_id = int((basename.split("_")[-1]).split(".")[0])
            subrun_ids.append(subrun_id)
        except Exception as e:
            print(subrun_file, basename)
            raise e

    num_full_subruns = np.max(subrun_ids) + 1

    # -------------------
    # Get gaps in subruns
    # -------------------
    file_exists = np.zeros(num_full_subruns, dtype=int)
    file_exists[np.unique(subrun_ids)] = 1

    starts = np.arange(num_full_subruns)[
        np.diff(np.pad(file_exists, [1, 0], mode="constant")) > 0
    ]
    stops = np.arange(num_full_subruns)[
        (np.diff(np.pad(file_exists[::-1], [1, 0], mode="constant")) > 0)[::-1]
    ]
    assert starts.shape == stops.shape

    # --------------------
    # Get Start/Stop Times
    # --------------------
    # Find and read the gaps files for the start/stops
    gaps_tarfile = sorted(
        glob.glob(gaps_tar_bases[0].format(year=year, date=date, run=run_id))
        + glob.glob(gaps_tar_bases[1].format(year=year, date=date, run=run_id))
    )
    gaps_txtfiles = sorted(
        glob.glob(gaps_txt_base.format(year=year, date=date, run=run_id))
    )
    if not len(gaps_tarfile) > 0:
        gaps_files = gaps_txtfiles
        tar_contents = gaps_files
        is_tar = False
    else:
        try:
            gaps_files = tarfile.open(gaps_tarfile[0])
            tar_contents = gaps_files.getnames()
            is_tar = True
        except Exception as e:
            print(e)
            print("\tTar file {} is broken?".format(gaps_tarfile[0]))
            print("Falling back on gaps files...")
            gaps_files = gaps_txtfiles
            tar_contents = gaps_files
            is_tar = False
    if not np.all(file_exists):
        missing = np.arange(num_full_subruns)[~file_exists.astype(bool)]
        print(
            "\tRun {} appears to be missing {}/{} files".format(
                run_id, len(missing), num_full_subruns
            )
        )
        print("\t\tDirectory:", run_dir)
        print("\t\tMissing subruns:", missing)

    start_times = read_from_gaps(
        run_id, starts, gaps_files, True, is_tar, tar_contents, subrun_files
    )
    end_times = read_from_gaps(
        run_id, stops, gaps_files, False, is_tar, tar_contents, subrun_files
    )

    # ------------------
    # Calculate Livetime
    # ------------------
    livetime = 0.0
    for start, end in zip(start_times, end_times):
        assert end > start, (start, end)
        livetime += (end - start) * 24 * 3600
    return gcd_file, subrun_files, livetime, start_times, end_times


def get_exp_dataset_jobs(config):
    """Process and experimental dataset and collect jobs to run

    Parameters
    ----------
    config : dict
        The dictionary with configuration settings.

    Returns
    -------
    list of int
        The run numbers to process
    dict
        The dictionary with updated configuration settings.
        A field `run_number_settings` is added which defines the
        run number specific settings, such as the GCD file, the start/stop
        times and the total livetime.
    """

    # get info from provided GRL files
    grl_df_dict, grl_dict = parse_grl_lists(config["exp_dataset_grl_paths"])

    run_glob = config["exp_dataset_run_glob"]

    if "exp_dataset_years" in config:
        assert "year" not in config, config
        years = config["exp_dataset_years"]
    elif "year" in config:
        years = [int(config["year"])]
    else:
        years = None

    if years is None:
        year_glob = sorted(glob.glob(run_glob.split("{year}")[0] + "20??"))
        potential_years = [int(y[-4:]) for y in year_glob]

        years = []
        for year in potential_years:
            if (
                len(
                    glob.glob(
                        run_glob.replace("{month:02d}{day:02d}", "*").format(
                            year=year
                        )
                    )
                )
                > 0
            ):
                years.append(year)
        print("No years specified, found these: {}".format(years))

    months = config["exp_dataset_months"]
    if months is None:
        months = range(1, 13)
        print("No months specified, using all.")

    days = config["exp_dataset_days"]

    # now collect runs to process and information for each run
    total_livetime = 0.0
    run_numbers = []
    run_number_settings = {}

    for year in years:
        for month in months:
            # get days for this month
            if days is None:
                days = range(1, 1 + calendar.monthrange(year, month)[1])

            for day in days:
                run_glob_i = run_glob.format(year=year, month=month, day=day)
                run_file_list = sorted(glob.glob(run_glob_i))
                for run_file in run_file_list:
                    run_num = int(run_file.split("/")[-1].replace("Run", ""))

                    # --------------------------
                    # apply specified run filter
                    # --------------------------
                    passed_filter = True

                    # check if run exists in GRL list
                    if run_num not in grl_dict:
                        # this probably means that this was not a physics run
                        msg = "\tRun {} is missing in GRL list".format(run_num)
                        msg += ". Either the correct GRL lists were not loaded"
                        msg += " or this is not a Physics run. Skipping!"
                        if not (
                            "exp_data_warn_missing" in config
                            and not config["exp_data_warn_missing"]
                        ):
                            print(msg)
                        continue

                    # get info from GRL list
                    run_data = grl_dict[run_num]

                    for col, operator, cut in config["exp_dataset_filter"]:
                        value = run_data[GRL_COLUMN_INDEX_DICT[col]]
                        if operator == "!=":
                            condition = value != cut
                        elif operator == "==":
                            condition = value == cut
                        elif operator == ">=":
                            condition = value >= cut
                        elif operator == "<=":
                            condition = value <= cut
                        elif operator == ">":
                            condition = value > cut
                        elif operator == "<":
                            condition = value < cut
                        elif operator == "in":
                            condition = value in cut
                        else:
                            raise ValueError("Operator unknown:", operator)

                        if not condition:
                            passed_filter = False
                            break

                    if not passed_filter:
                        continue
                    # --------------------------

                    run_numbers.append(run_num)

                    # get info for this run
                    date = "{month:02d}{day:02d}".format(month=month, day=day)
                    if "get_run_info_kwargs" in config:
                        get_run_info_kwargs = config["get_run_info_kwargs"]
                    else:
                        get_run_info_kwargs = {}
                    (
                        gcd_file,
                        subrun_files,
                        livetime,
                        start_times,
                        end_times,
                    ) = get_run_info(
                        run_dir=run_file,
                        year=year,
                        date=date,
                        run_id=run_num,
                        **get_run_info_kwargs
                    )

                    total_livetime += livetime

                    # save information for run
                    run_number_settings[run_num] = {
                        "year": year,
                        "month": month,
                        "day": day,
                        "gcd": gcd_file,
                        "in_file_pattern": subrun_files,
                        "exp_dataset_livetime": livetime,
                        "exp_dataset_start_times": start_times,
                        "exp_dataset_end_times": end_times,
                        "exp_dataset_run_list": [run_num],
                        "exp_dataset_merge": False,
                    }

    # set run number specific settings in config
    assert "run_number_settings" not in config, config["run_number_settings"]
    config["run_number_settings"] = run_number_settings

    msg = "Found a total of {} runs with a livetime of {}s or {:3.3f} days"
    print(
        msg.format(
            len(run_numbers), total_livetime, total_livetime / 24.0 / 3600.0
        )
    )

    return run_numbers, config
