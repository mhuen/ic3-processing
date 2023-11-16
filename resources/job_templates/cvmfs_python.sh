#!/bin/bash

# Gather keys
FINAL_OUT={final_out}
KEEP_CRASHED_FILES={keep_crashed_files}
WRITE_HDF5={write_hdf5}
WRITE_I3={write_i3}
CUDA_HOME={cuda_home}
LD_LIBRARY_PATH_PREPENDS={ld_library_path_prepends}
CVMFS_PYTHON={cvmfs_python}


# load environment
echo 'Starting job on Host: '$HOSTNAME
echo 'Loading: ' ${CVMFS_PYTHON}
eval `/cvmfs/icecube.opensciencegrid.org/${CVMFS_PYTHON}/setup.sh`
export PYTHONUSERBASE={python_user_base}
echo 'Using PYTHONUSERBASE: '${PYTHONUSERBASE}

export PATH=$PYTHONUSERBASE/bin:$PATH
export PYTHONPATH=$PYTHONUSERBASE/lib/python3.7/site-packages:$PYTHONPATH

# set MPL backend for Matplotlib
export MPLBACKEND=agg

# add cuda directory
if [ "$(echo "$CUDA_HOME" | sed 's/^.\(.*\).$/\1/')" = "cuda_home" ]; then
  echo 'No cuda home provided. Not adding cuda to path.'
else
  echo 'Adding cuda dir: '$CUDA_HOME
  export PATH=$CUDA_HOME/bin:$PATH
  export LD_LIBRARY_PATH=$CUDA_HOME/lib64:$LD_LIBRARY_PATH
fi

# add additional LD_LIBRARY_PATH additions if we have them
if [ "$(echo "$LD_LIBRARY_PATH_PREPENDS" | sed 's/^.\(.*\).$/\1/')" != "ld_library_path_prepends" ]; then
  echo 'Prepending to LD_LIBRARY_PATH: '$LD_LIBRARY_PATH_PREPENDS
  export LD_LIBRARY_PATH=$LD_LIBRARY_PATH_PREPENDS:$LD_LIBRARY_PATH
fi

# start python script
echo 'Starting process for output file: '$FINAL_OUT
if [ -z ${PBS_JOBID} ] && [ -z ${_CONDOR_SCRATCH_DIR} ]
then
    echo 'Running Script w/o temporary scratch'
    {python_path} {yaml_path} {run_number} --no-scratch
    JOB_RC=$?
    echo 'Job finished with Exit Code: ' $JOB_RC
    if [ $JOB_RC -ne 0 ] && [ $KEEP_CRASHED_FILES -eq 0 ] ; then
        echo 'Deleting partially processed file! ' $FINAL_OUT

        # Clean Up
        if [ "$WRITE_HDF5" = "True" ]; then
            rm ${FINAL_OUT}.hdf5
        fi
        if [ "$WRITE_I3" = "True" ]; then
            rm ${FINAL_OUT}.i3.bz2
        fi
        if [ -f "$FINAL_OUT" ]; then
            rm $FINAL_OUT
        fi

    fi
else
    echo 'Running Script w/ temporary scratch'
    if [ -z ${_CONDOR_SCRATCH_DIR} ]
    then
        cd /scratch/${USER}
    else
        cd ${_CONDOR_SCRATCH_DIR}
    fi
    {python_path} {yaml_path} {run_number} --scratch
    JOB_RC=$?
    echo 'Job finished with Exit Code: ' $JOB_RC
    if [ $JOB_RC -eq 0 ] || [ $KEEP_CRASHED_FILES -eq 1 ]; then
        if [ "$WRITE_HDF5" = "True" ]; then
            cp {final_out_scratch}.hdf5 {output_folder}
        fi
        if [ "$WRITE_I3" = "True" ]; then
            cp {final_out_scratch}.i3.bz2 {output_folder}
        fi
        if [ -f "$FINAL_OUT" ]; then
            cp {final_out_scratch} {output_folder}
        fi
    fi

    # Clean Up
    if [ "$WRITE_HDF5" = "True" ]; then
        rm {final_out_scratch}.hdf5
    fi
    if [ "$WRITE_I3" = "True" ]; then
        rm {final_out_scratch}.i3.bz2
    fi
    if [ -f "$FINAL_OUT" ]; then
        rm {final_out_scratch}
    fi
fi
exit $JOB_RC
