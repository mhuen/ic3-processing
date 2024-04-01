"""
Adopted from:

https://code.icecube.wisc.edu/projects/icecube/browser/IceCube/projects/
sim-services/trunk/python/propagation.py
"""

from icecube import icetray, dataclasses


def get_propagators():
    """
    Set up a staple of propagators for muons, taus, and cascades.
    """
    from icecube import sim_services, phys_services
    from icecube.PROPOSAL import I3PropagatorServicePROPOSAL
    from icecube.cmc import I3CascadeMCService

    propagators = sim_services.I3ParticleTypePropagatorServiceMap()
    mu_tau_prop = I3PropagatorServicePROPOSAL()
    # dummy RNG
    cprop = I3CascadeMCService(phys_services.I3GSLRandomService(1))
    for pt in ["MuMinus", "MuPlus", "TauMinus", "TauPlus"]:
        propagators[getattr(dataclasses.I3Particle.ParticleType, pt)] = (
            mu_tau_prop
        )
    for pt in [
        "DeltaE",
        "Brems",
        "PairProd",
        "NuclInt",
        "Hadrons",
        "EMinus",
        "EPlus",
    ]:
        propagators[getattr(dataclasses.I3Particle.ParticleType, pt)] = cprop
    return propagators


@icetray.traysegment
def RecreateMCTree(
    tray,
    name,
    MCTree="I3MCTree",
    RawMCTree="I3MCTreeRaw",
    RNGState="RNGState",
    Propagators=None,
    Paranoia=True,
    recreate=None,
    audit=None,
):
    """
    Recreate the results of lepton propagation and shower simulation that
    were discarded in order to save space.

    :param RawMCTree: name of the un-propagated I3MCTree to use as input
    :param MCTree: name of the re-propagated I3MCTree to put back in the frame
    :param RNGState: name of the object storing the state of the random number
                     generator
    :param Propagators: an I3ParticleTypePropagatorServiceMap giving the
                        propagators to use for each kind of particle. If this
                        is None, use PROPOSAL for muons and CMC for showers.
    :param Paranoia: if True, compare the re-propagated result to the original
                     when it is present in the frame and raise an assertion if
                     they differ.
    """
    from icecube import phys_services, sim_services  # noqa: F401

    if Propagators is None:
        Propagators = get_propagators()

    # NB: the parameters given here do not matter. If they are different
    #     from the stored state the generator will be re-initialized.
    randomService = phys_services.I3SPRNGRandomService(
        seed=2, nstreams=10000, streamnum=1
    )

    if recreate is None:

        def recreate(frame):
            return RNGState in frame and MCTree not in frame

    if audit is None:

        def audit(frame):
            return RNGState in frame and MCTree in frame

    tray.AddModule(
        "I3PropagatorModule",
        name + "Propagator",
        PropagatorServices=Propagators,
        RandomService=randomService,
        RNGStateName=RNGState,
        InputMCTreeName=RawMCTree,
        OutputMCTreeName=MCTree,
        If=recreate,
    )

    if Paranoia:
        # Paranoid mode: check results
        ParanoidMCTree = name + "MCTree"
        tray.AddModule(
            "I3PropagatorModule",
            name + "ParanoidPropagator",
            PropagatorServices=Propagators,
            RandomService=randomService,
            RNGStateName=RNGState,
            InputMCTreeName=RawMCTree,
            OutputMCTreeName=ParanoidMCTree,
            If=audit,
        )

        from unittest import TestCase
        from unittest.util import safe_repr

        def assertClose(self, a, b, rtol=1e-8, atol=0, msg=None):
            """
            a and b are equal to within tolerances

            :param rtol: relative tolerance
            :param atol: absolute tolerance
            """
            if abs(a - b) > atol + rtol * abs(b):
                if atol > 0 and abs(a - b) > atol:
                    std_msg = "%s != %s to within %s (absolute)" % (
                        safe_repr(a),
                        safe_repr(b),
                        safe_repr(atol),
                    )
                else:
                    std_msg = "%s != %s to within %s (relative)" % (
                        safe_repr(a),
                        safe_repr(b),
                        safe_repr(rtol),
                    )

                msg = self._formatMessage(msg, std_msg)
                raise self.failureException(msg)
            else:
                return

        TestCase.assertClose = assertClose

        class MCTreeAudit(TestCase):
            """
            Ensure that every entry in the re-simulated MCTree is
            identical to the original one to within round-off error.
            """

            def setUp(self):
                self.orig_tree = self.frame[MCTree]
                self.new_tree = self.frame[ParanoidMCTree]

            def testTotalSize(self):
                self.assertEquals(len(self.orig_tree), len(self.new_tree))

            def testParticleContent(self):
                for p1, p2 in zip(self.orig_tree, self.new_tree):
                    if p1.location_type != p1.InIce:
                        continue
                    self.assertEquals(p1.type, p2.type)
                    self.assertClose(
                        p1.energy, p2.energy, atol=1e-6, rtol=1e-9
                    )
                    self.assertClose(p1.time, p2.time, atol=1, rtol=1e-9)
                    self.assertClose(
                        p1.length, p2.length, atol=1e-6, rtol=1e-9
                    )
                    for d in "x", "y", "z":
                        self.assertClose(
                            getattr(p1.pos, d),
                            getattr(p2.pos, d),
                            atol=1e-6,
                            rtol=1e-9,
                        )
                    for d in "zenith", "azimuth":
                        self.assertClose(
                            getattr(p1.dir, d),
                            getattr(p2.dir, d),
                            atol=1e-6,
                            rtol=1e-9,
                        )

        tray.AddModule(
            icetray.I3TestModuleFactory(MCTreeAudit),
            name + "ParanoidCheck",
            Streams=[icetray.I3Frame.DAQ],
            If=audit,
        )
        tray.AddModule(
            "Delete",
            name + "DeleteParanoidCruft",
            Keys=[ParanoidMCTree],
            If=audit,
        )
