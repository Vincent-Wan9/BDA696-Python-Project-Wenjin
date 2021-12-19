#!/bin/bash

sleep 10

# Insert the SQL database (if it doesn't exist)
if ! mysql -h db -uroot -proot -e 'use baseball'; then
  echo "Baseball DOES NOT exists"
  mysql -h db -proot -u root -e "CREATE DATABASE IF NOT EXISTS baseball;"
  mysql -h db -u root -proot baseball < /Finals/baseball.sql
else
  echo "Baseball DOES exists"
fi

# Run the sql commands
mysql -h db -u root -proot baseball < /Finals/features.sql


# Save the result
mysql -h db -u root -proot baseball -e '
  SELECT * FROM final_features_60_day;' > /results/feature.csv
echo "Features have been saved"

# Run the python commands
python ./Finals/Examine_feature.py
echo "Features have been examined"

python ./Finals/ML_Model_60_day.py
echo "Prediction results are ready"