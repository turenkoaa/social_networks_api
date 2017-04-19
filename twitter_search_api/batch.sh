#!/bin/bash
OUTPUTFOLDER=output

DATE[1]="Mar 22 2017"
DATE[2]="Mar 23 2017"
DATE[3]="Mar 24 2017"

HASH1[1]="#PrayForLondon"
HASH1[2]="#LondonStrong"
HASH1[3]="#WeAreNotAfraid"
HASH1[4]="#LondonIsOpen"
HASH1[5]="#WeStandTogether"
HASH1[6]="#TrafalgarSquare"
HASH1[7]="#ProudToBebritish"
HASH1[8]="#PrayforUK"

HASH2[1]="Scotland Yard"
HASH2[2]="London Ambulance Service"
HASH2[3]="#999family"
HASH2[4]="#Parliament"
HASH2[5]="#WestminsterBridge"
HASH2[6]="#UKParliament"
HASH2[7]="#londonattack"
HASH2[8]="#Westminster"
HASH2[9]="#WestminsterAttack"
HASH2[10]="#terroristattack"
HASH2[11]="#london"
HASH2[12]="#ParliamentAttack"

HASH3[1]="#londres"

for H in ${HASH1[@]}
do
    for D in ${DATE[@]}
    do
        HASHSUFF=`echo $H | sed 's/#//g'`
        DATESUFF=`echo $D | sed 's/#//g'`
        echo "$H - $D"
        nohup python3 main1.py "${H}" "${D}" > ${OUTPUTFOLDER}/out_${HASHSUFF}_${DATESUFF}.txt 2> ${OUTPUTFOLDER}/err_${HASHSUFF}_${DATESUFF}.txt &
    done

done

for H in ${HASH2[@]}
do
    for D in ${DATE[@]}
    do
        HASHSUFF=`echo $H | sed 's/#//g'`
        DATESUFF=`echo $D | sed 's/#//g'`
        echo "$H - $D"
        nohup python3 main2.py "${H}" "${D}" > ${OUTPUTFOLDER}/out_${HASHSUFF}_${DATESUFF}.txt 2> ${OUTPUTFOLDER}/err_${HASHSUFF}_${DATESUFF}.txt &
    done

done