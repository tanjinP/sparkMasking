#!/bin/bash

. ~/pg.conf

export $PGPASSWORD

echo "Getting csv files from database..."
psql --host=$HOST --port=5432 --username=$USER --dbname=postgres -c "\copy (SELECT * FROM $TABLE1) to '~/$TABLE1.csv' with CSV HEADER;"
psql --host=$HOST --port=5432 --username=$USER --dbname=postgres -c "\copy (SELECT * FROM $TABLE2) to '~/$TABLE2.csv' with CSV HEADER;"


echo "Uploading to S3..."
cd ~
aws s3 cp $TABLE1.csv s3://vts-dummyData/csv/$TABLE1.csv
aws s3 cp $TABLE2.csv s3://vts-dummyData/csv/$TABLE2.csv


echo "Upload to S3 complete - removing local files"
rm ~/$TABLE1.csv
rm ~/$TABLE2.csv

echo "Done!"
