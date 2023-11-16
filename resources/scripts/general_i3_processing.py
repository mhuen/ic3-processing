import timeit
import click
import warnings

from I3Tray import I3Tray
from icecube import icetray, hdfwriter

try:
    from ic3_labels.weights.segments import UpdateMergedWeights
except ImportError as e:
    warnings.warn(f"Could not import ic3_labels: {e}.")
    warnings.warn("Continuing without support weight merging")


from ic3_processing.utils.exp_data import livetime
from ic3_processing.utils import setup


@click.command()
@click.argument("cfg", type=click.Path(exists=True))
@click.argument("run_number", type=int)
@click.option("--scratch/--no-scratch", default=True)
def main(cfg, run_number, scratch):
    # start timer
    start_time = timeit.default_timer()

    # --------------------------------
    # load configuration and setup job
    # --------------------------------
    cfg, context = setup.setup_job_and_config(cfg, run_number, scratch)
    # --------------------------------

    tray = I3Tray()

    tray.context["ic3_processing"] = context
    tray.context["ic3_processing"]["HDF_keys"] = []

    tray.Add("I3Reader", FilenameList=context["infiles"])

    # -----------------------------------------
    # Write livetime data for experimental data
    # -----------------------------------------
    livetime.write_exp_livetime_data(
        tray,
        name="write_exp_livetime_data",
        cfg=cfg,
    )

    # ----------------------------------------------
    # Add tray segments defined in configuration cfg
    # ----------------------------------------------
    for i, settings in enumerate(cfg["tray_segments"]):
        # get module/segment class
        if "." not in settings["ModuleClass"]:
            module_class = settings["ModuleClass"]
        else:
            module_class = setup.load_class(settings["ModuleClass"])

        # dynamically replace values of the form
        #       context-->a.b.c
        # with tray.context[a][b][c]
        search_key = "context-->"
        kwargs = {}
        for key, value in settings["ModuleKwargs"].items():
            # dynamically replace key
            if isinstance(value, str):
                if search_key in value:
                    context_keys = value.replace(search_key, "").split(".")
                    value = tray.context
                    for context_key in context_keys:
                        value = value[context_key]

                else:
                    # expand with parameters in config
                    value = value.format(**cfg)

            kwargs[key] = value

        # print(f'Adding tray "{module_class}" with the following keys:')
        # for key, value in kwargs.items():
        #     print(f"{key}:\t\t\t{value}")

        tray.Add(
            module_class,
            settings["ModuleClass"].split(".")[-1] + "_{:05d}".format(i),
            **kwargs,
        )

    # -----------------------------------------------------------
    # keep track of merged files and update weights if they exist
    # -----------------------------------------------------------
    if "merge_weights" in cfg and cfg["merge_weights"]:
        tray.AddModule(
            UpdateMergedWeights,
            "UpdateMergedWeights",
            TotalNFiles=context["ic3_processing"]["total_n_files"],
        )

    # --------------------------------------------------
    # Write output
    # --------------------------------------------------
    if cfg["write_i3"]:
        if "i3_streams" in cfg["write_i3_kwargs"]:
            i3_streams = [
                icetray.I3Frame.Stream(s)
                for s in cfg["write_i3_kwargs"].pop("i3_streams")
            ]
        else:
            i3_streams = [
                icetray.I3Frame.DAQ,
                icetray.I3Frame.Physics,
                icetray.I3Frame.TrayInfo,
                icetray.I3Frame.Simulation,
                icetray.I3Frame.Stream("S"),
                icetray.I3Frame.Stream("M"),
                icetray.I3Frame.Stream("m"),
                icetray.I3Frame.Stream("W"),
                icetray.I3Frame.Stream("X"),
            ]
        print("Only writing the following streams:", i3_streams)

        tray.AddModule(
            "I3Writer",
            "EventWriter",
            filename="{}.{}".format(context["outfile"], cfg["i3_ending"]),
            Streams=i3_streams,
            **cfg["write_i3_kwargs"],
        )

    if cfg["write_hdf5"]:
        keys = cfg["write_hdf5_kwargs"].pop("Keys")
        tray.AddSegment(
            hdfwriter.I3HDFWriter,
            "hdf",
            Output=f'{context["outfile"]}.hdf5',
            Keys=keys + tray.context["ic3_processing"]["HDF_Keys"],
            **cfg["write_hdf5_kwargs"],
        )
    # --------------------------------------------------

    tray.AddModule("TrashCan", "the can")
    tray.Execute()
    tray.Finish()

    end_time = timeit.default_timer()
    print("Duration: {:5.3f}s".format(end_time - start_time))


if __name__ == "__main__":
    main()
