from icecube import dataclasses, icetray


def add_2_cascade_seed_keys(
    frame,
    seed_base,
    additional_seeds=[],
    seed_distances=[30],
    add_reverse=False,
    nside=None,
    min_energy=500.0,
    cluster_settings={},
):
    from dnn_cascade_selection.utils.reconstruction import (
        add_2_cascade_model_seeds,
    )

    add_2_cascade_model_seeds(
        frame=frame,
        seed_base=seed_base,
        additional_seeds=additional_seeds,
        seed_distances=seed_distances,
        add_reverse=add_reverse,
        nside=nside,
        min_energy=min_energy,
        cluster_settings=cluster_settings,
    )


def extend_multi_cascade_seed(
    frame,
    seeds=[],
    seed_distances=[30],
    output_name="{base}_MultiSeeds",
    remove_prefix="",
):
    for base_name in seeds:
        new_seed = dataclasses.I3MapStringDouble(frame[base_name])

        if remove_prefix:
            new_seed_ = dataclasses.I3MapStringDouble()
            for key, value in new_seed.items():
                if key.startswith(remove_prefix):
                    new_seed_[key[len(remove_prefix) :]] = value
                else:
                    new_seed_[key] = value
            new_seed = new_seed_

        for i, distance in enumerate(seed_distances):
            if f"cascade_{i:05d}_distance" in new_seed:
                continue
            new_seed[f"cascade_{i:05d}_distance"] = distance
            new_seed[f"cascade_{i:05d}_energy"] = new_seed["energy"] * 0.8
        frame[output_name.format(base=base_name)] = new_seed


def combine_i3_particle(
    frame,
    output_name,
    pos_x_name=None,
    pos_y_name=None,
    pos_z_name=None,
    dir_name=None,
    time_name=None,
    energy_name=None,
    shape="Cascade",
):
    """Add an I3Particle to the frame with values from other particles.

    This function can be helpful when creating seeds for reconstruction
    methods based on previous reconstruction results.

    Parameters
    ----------
    frame : I3Frame
        The current physics frame
    output_name : str
        The frame key to which the combined I3Particle will be written to.
    pos_x_name : None, optional
        The name of the I3Particle from which to take the vertex-x position.
    pos_y_name : None, optional
        The name of the I3Particle from which to take the vertex-y position.
    pos_z_name : None, optional
        The name of the I3Particle from which to take the vertex-z position.
    dir_name : None, optional
        The name of the I3Particle from which to take the direction.
    time_name : None, optional
        The name of the I3Particle from which to take the time of the vertex.
    energy_name : None, optional
        The name of the I3Particle from which to take the energy.
    shape : str, optional
        The dataclasses.I3Particle.ParticleShape to assign.
    """
    particle = dataclasses.I3Particle()
    if pos_x_name is not None:
        particle.pos.x = frame[pos_x_name].pos.x
    if pos_y_name is not None:
        particle.pos.y = frame[pos_y_name].pos.y
    if pos_z_name is not None:
        particle.pos.z = frame[pos_z_name].pos.z
    if dir_name is not None:
        particle.dir = frame[dir_name].dir
    if time_name is not None:
        particle.time = frame[time_name].time
    if energy_name is not None:
        particle.energy = frame[energy_name].energy

    particle.fit_status = dataclasses.I3Particle.FitStatus.OK
    particle.shape = getattr(dataclasses.I3Particle.ParticleShape, shape)

    frame[output_name] = particle


@icetray.traysegment
def create_cascade_classification_base_cascades(
    tray,
    name="add_cascade_base",
    cscd_base_configs={},
):
    """Add cascade classification model base cascades to frame

    Parameters
    ----------
    tray : I3Tray
        The I3Tray to which the modules should be added.
    name : str, optional
        Name of the tray module.
    cscd_base_configs : dict or list of dict
        A dictionary (or list of dictionaries) with all
        configuration settings.
    """

    def add_cascade_base(frame, config, output_key=None):
        if config["I3ParticleBase"] in frame:
            particle = frame[config["I3ParticleBase"]]
            labels = dataclasses.I3MapStringDouble()
            labels["VertexX"] = particle.pos.x
            labels["VertexY"] = particle.pos.y
            labels["VertexZ"] = particle.pos.z
            labels["VertexTime"] = particle.time
            labels["VertexX_unc"] = config["VertexX_unc"]
            labels["VertexY_unc"] = config["VertexY_unc"]
            labels["VertexZ_unc"] = config["VertexZ_unc"]
            labels["VertexTime_unc"] = config["VertexTime_unc"]
            if output_key is None:
                output_key = (
                    "cscd_classification_base_" + config["I3ParticleBase"]
                )
            frame[output_key] = labels

    if isinstance(cscd_base_configs, dict):
        cscd_base_configs = [cscd_base_configs]

    for i, cscd_base_config in enumerate(cscd_base_configs):
        tray.AddModule(
            add_cascade_base,
            name + "_{:03d}".format(i),
            config=cscd_base_config,
        )
