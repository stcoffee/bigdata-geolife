rm -r ./zip
mkdir ./zip
cp ./src/main.py ./zip
zip -x ./src/main.py -r ./zip/jobs.zip ./src