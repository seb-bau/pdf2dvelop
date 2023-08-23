#/bin/bash

for f in ./input/*.pdf
do
echo $f
bn=$(basename $f)
echo $bn

/usr/bin/ocrmypdf -d -l deu $f ./ocr/$bn
mv $f ./backup/ocr/$bn

done