#!/bin/bash
#PBS -N lab8
#PBS -e ./error_log.txt
#PBS -o ./outptu_log.txt

cd ~
echo Start of calculation
python flatten_v2.py
echo End of calculation
