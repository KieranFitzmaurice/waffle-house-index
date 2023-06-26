#!/bin/bash

cd /home/kieran/projects/waffle-house-index
source waffle-venv-1/bin/activate
STARTTIME=$SECONDS
python update_grids.py
STATUS=$?
ENDTIME=$SECONDS
ELAPSED_SECONDS=$(($ENDTIME - $STARTTIME))
ELAPSED_MINUTES=$(($ELAPSED_SECONDS / 60))
EXTRA_SECONDS=$(($ELAPSED_SECONDS - 60*$ELAPSED_MINUTES))
DATESTR=$(date "+%Y-%m-%d at %I:%M %p")
MESSAGE="Updating of grids completed on $DATESTR. Time elapsed: $ELAPSED_MINUTES  minutes, $EXTRA_SECONDS seconds. Exit code: $STATUS"
SUBJECT="Waffle House Index: Grid Update Completed"
echo $MESSAGE | mutt -s "$SUBJECT" kfitzmaurice98@gmail.com,kfitzmaurice@unc.edu,kpf10@pitt.edu
