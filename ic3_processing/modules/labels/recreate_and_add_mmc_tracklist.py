"""
Traysegment that can be used to recreate the MMCTrackList from the
I3MCTree_preMuonProp or I3MCTree.
"""

import sys
from icecube import icetray, dataclasses, simclasses, phys_services
from icecube.icetray import i3logging
from icecube.simprod import segments

from ic3_processing.modules.labels.recreate_mctree import RecreateMCTree


@icetray.traysegment
def RerunProposal(
    tray,
    name,
    mctree_name="I3MCTree",
    merge_trees=[],
    fail_if_no_rng_found=True,
    random_service_class=None,
    proposal_kwargs={},
):
    """Re-run PROPOSAl and add MMCTrackList and I3MCTree to frame

    Only runs if either of the tracklist or the propagated I3MCTree
    are not in the frame. Will keep existing track list or I3MCTree.

    Parameters
    ----------
    tray : I3Tray
        The I3Tray.
    name : str
        The name of the tray segment.
    mctree_name : str, optional
        The name of the propagatedI3
    merge_trees : list or str, optional
        A list of I3MCTrees to merge into the newly re-created one.
    fail_if_no_rng_found : bool, optional
        If no RNG state is found, an error will be raised if set to True.
    random_service_class : str, optional
        The name of the random service class.
    proposal_kwargs : dict, optional
        Additional keyword arguments passed on to PROPOSAL.
    """

    def prepare_repropagation(frame, mctree_name="I3MCTree"):
        """Peprare frame for re-propagation via PROPOSAL

        check if I3MCTree and MMCTrackList exist
        -> save to temporary keys if PROPOSAL is rerun
        -> make sure I3MCTree_preMuonProp exists

        Parameters
        ----------
        frame : I3Frame
            The current I3Frame.
        mctree_name : str, optional
            The name of the propagated I3MCTree (without '_preMuonProp')
        """
        # check if we need to re-run PROPOSAL
        needs_rerun = not (
            frame.Has(mctree_name) and frame.Has("MMCTrackList")
        )

        if needs_rerun:
            # get meta info
            meta_info = dataclasses.I3MapStringDouble()
            meta_info["python_version"] = (
                sys.version_info.major * 10000
                + sys.version_info.minor * 100
                + sys.version_info.micro
            )

            if mctree_name + "_preMuonProp" + "_RNGState" in frame:
                meta_info["found_rng_state"] = True

            elif "RNGState" in frame:
                i3logging.log_info(
                    "Only found RNGState, make sure this is correct!"
                )
                meta_info["found_rng_state"] = 0.5

            else:
                meta_info["found_rng_state"] = False
                i3logging.log_error(
                    "Did not find rng state for PROPOSAL! Recreated MCTree"
                    " will not match previous pulses!!"
                )

            if meta_info["found_rng_state"] < 0.1 and fail_if_no_rng_found:
                raise ValueError("Could not find RNG state!")

            # save existing keys
            if frame.Has(mctree_name):
                frame[mctree_name + "_TEMP"] = dataclasses.I3MCTree(
                    frame[mctree_name]
                )
                del frame[mctree_name]
                meta_info["recreated_mctree"] = False
            else:
                meta_info["recreated_mctree"] = True

            if frame.Has("MMCTrackList"):
                frame["MMCTrackList_TEMP"] = simclasses.I3MMCTrackList(
                    frame["MMCTrackList"]
                )
                del frame["MMCTrackList"]
                meta_info["recreated_mmc_track_list"] = False
            else:
                meta_info["recreated_mmc_track_list"] = True

            # create a pre muon propagation tree if it does not exist
            if not frame.Has(mctree_name + "_preMuonProp"):
                frame[mctree_name + "_preMuonProp"] = dataclasses.I3MCTree(
                    frame[mctree_name + "_TEMP"]
                )
                meta_info["created_pre_muon_prop_tree"] = True
            else:
                meta_info["created_pre_muon_prop_tree"] = False

            # add meta info
            frame[mctree_name + "_recreation_meta_info"] = meta_info

    tray.AddModule(
        prepare_repropagation,
        "prepare_repropagation" + name,
        mctree_name=mctree_name,
        Streams=[icetray.I3Frame.DAQ],
    )

    # Re-run PROPOSAL
    if random_service_class is not None:
        # set the random service to the one created by the user
        if random_service_class == "I3SPRNGRandomService":
            randomService = phys_services.I3SPRNGRandomService(
                seed=10000,
                nstreams=200000000,
                streamnum=100014318,
            )
        elif random_service_class == "I3GSLRandomService":
            randomService = phys_services.I3GSLRandomService(seed=10000)
        else:
            raise ValueError(
                "Did not understand random service class: {}".format(
                    random_service_class
                )
            )
    else:
        # try and figure out by assuming that I3SPRNGRandomService will
        # be used if it's available
        try:
            randomService = phys_services.I3SPRNGRandomService(
                seed=10000, nstreams=200000000, streamnum=100014318
            )
        except AttributeError:
            randomService = phys_services.I3GSLRandomService(seed=10000)

    tray.Add(
        segments.PropagateMuons,
        "PropagateMuons" + name,
        RandomService=randomService,
        SaveState=True,
        InputMCTreeName=mctree_name + "_preMuonProp",
        OutputMCTreeName=mctree_name,
        If=lambda f: (not f.Has(mctree_name) or not f.Has("MMCTrackList")),
        **proposal_kwargs
    )

    # merge background trees into main I3MCTree
    def merge_background_trees(frame, mctree_name, merge_trees):
        if merge_trees:
            mc_tree = frame[mctree_name]
            for bkg_tree_name in merge_trees:
                if bkg_tree_name in frame:
                    bkg_tree = frame[bkg_tree_name]
                    mc_tree.merge(bkg_tree)
            del frame[mctree_name]
            frame[mctree_name] = mc_tree

    tray.AddModule(
        merge_background_trees,
        "merge_background_trees" + name,
        mctree_name=mctree_name,
        merge_trees=merge_trees,
        Streams=[icetray.I3Frame.DAQ],
    )

    def recover_previous_keys(frame, mctree_name="I3MCTree"):
        """Recover previous track list and I3MCTree

        Parameters
        ----------
        frame : I3Frame
            The current I3Frame.
        mctree_name : str, optional
            The name of the propagated I3MCTree (without '_preMuonProp')
        """
        if frame.Has(mctree_name + "_TEMP"):
            if frame.Has(mctree_name):
                del frame[mctree_name]
            frame[mctree_name] = frame[mctree_name + "_TEMP"]
            del frame[mctree_name + "_TEMP"]

        if frame.Has("MMCTrackList_TEMP"):
            if frame.Has("MMCTrackList"):
                del frame["MMCTrackList"]
            frame["MMCTrackList"] = frame["MMCTrackList_TEMP"]
            del frame["MMCTrackList_TEMP"]

    tray.AddModule(
        recover_previous_keys,
        "recover_previous_keys" + name,
        mctree_name=mctree_name,
        Streams=[icetray.I3Frame.DAQ],
    )


@icetray.traysegment
def AddMMCTrackList(tray, name):
    # ---------------
    # Add MMCTrackList
    # ---------------
    # Dirty Hack to recreate pre_muon_propagation MCTree:
    #   Assume only primaries and first daughters are in un-propagated tree
    def fill_tree(frame, tree, p):
        daughters = frame["I3MCTree"].get_daughters(p)
        tree.append_children(p, daughters)
        for d in daughters:
            if d.is_neutrino:
                fill_tree(frame, tree, d)

    def create_raw_mc_tree(frame):
        if "I3MCTree_preMuonProp" in frame:
            frame["I3MCTreeRaw"] = dataclasses.I3MCTree(
                frame["I3MCTree_preMuonProp"]
            )
        else:
            tree = dataclasses.I3MCTree()
            for p in frame["I3MCTree"].get_primaries():
                tree.add_primary(p)
                fill_tree(frame, tree, p)
            frame["I3MCTreeRaw"] = tree
        return True

    tray.AddModule(
        create_raw_mc_tree,
        "create_raw_mc_tree",
        Streams=[icetray.I3Frame.DAQ],
        If=lambda f: (not f.Has("I3MCTree") or not f.Has("MMCTrackList")),
    )

    tray.AddSegment(
        RecreateMCTree,
        "RecreateMCTree",
        MCTree="I3MCTree_new",
        RawMCTree="I3MCTreeRaw",
        RNGState="RNGState",
        Propagators=None,
        Paranoia=False,
        recreate=lambda f: not f.Has("MMCTrackList"),
    )

    # Add empty MMCTrackList objects for events that have none.
    def add_empty_tracklist(frame):
        if "MMCTrackList" not in frame:
            frame["MMCTrackList"] = simclasses.I3MMCTrackList()
        return True

    tray.AddModule(
        add_empty_tracklist,
        "add_empty_tracklist",
        Streams=[icetray.I3Frame.DAQ],
    )

    def rename_tree(frame):
        if "I3MCTree" not in frame:
            frame["I3MCTree"] = dataclasses.I3MCTree(frame["I3MCTree_new"])

    tray.AddModule(rename_tree, "rename_tree", Streams=[icetray.I3Frame.DAQ])
    tray.AddModule(
        "Delete", "Delete RecreateMCTree", Keys=["I3MCTreeRaw", "I3MCTree_new"]
    )
    # ---------------
