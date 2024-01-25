rm -r ./deploy
mkdir ./deploy
cp ./src/main.py ./deploy
zip -x ./src/main.py -r ./deploy/jobs.zip ./src