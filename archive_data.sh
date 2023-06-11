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
  echo "Data successfully backed up to google drive"
  rm $LASTARCHIVE
else
  echo "Backup failed"
fi
