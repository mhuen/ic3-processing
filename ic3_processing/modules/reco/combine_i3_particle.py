from icecube import dataclasses


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
