#!/bin/bash

cd /home/kieran/projects/waffle-house-index
source waffle-venv-1/bin/activate
ARCHIVE="$(date '+%Y-%m-%d')_archived_data.tar.gz"
LASTARCHIVE="$(date -d "-1 month" +%Y-%m-%d)_archived_data.tar.gz"
tar -czvf $ARCHIVE data
FILEPATH=$PWD/$ARCHIVE
cd google_drive
python upload_file.py $FILEPATH
STATUS=$?
cd ..
rm -r data
if [ $STATUS == 0 ]; then
  MESSAGE="File "$ARCHIVE" was successfully backed up to google drive."
  SUBJECT="Waffle House Index: Backup Successful"
  rm $LASTARCHIVE
else
  MESSAGE="Automatic backup of "$ARCHIVE" was unsuccessful. Please upload to google drive manually."
  SUBJECT="Waffle House Index: Backup Failed"
fi
echo $MESSAGE | mutt -s "$SUBJECT" kfitzmaurice98@gmail.com,kfitzmaurice@unc.edu,kpf10@pitt.edu
