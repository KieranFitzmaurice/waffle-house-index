#!/bin/bash

cd /home/kieran/projects/waffle-house-index
source waffle-venv-1/bin/activate
STARTTIME=$SECONDS
python scrape_data.py
ENDTIME=$SECONDS
ELAPSED_SECONDS=$(($ENDTIME - $STARTTIME))
ELAPSED_MINUTES=$(($ELAPSED_SECONDS / 60))
EXTRA_SECONDS=$(($ELAPSED_SECONDS - 60*$ELAPSED_MINUTES))
DATESTR=$(date "+%Y-%m-%d at %I:%M %p")
MESSAGE="Web scraping completed on $DATESTR. Time elapsed: $ELAPSED_MINUTES  minutes, $EXTRA_SECONDS seconds."
SUBJECT="Waffle House Index: Web Scraping Completed"
echo $MESSAGE | mutt -s "$SUBJECT" kfitzmaurice98@gmail.com,kfitzmaurice@unc.edu,kpf10@pitt.edu
