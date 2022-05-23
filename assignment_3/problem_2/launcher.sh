#!/bin/bash -l
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=32
#SBATCH --time=2-00:00:00
#SBATCH -p batch
#SBATCH -J Ass3_Prob2
#SBATCH --output=output.log
#SBATCH --error=error.log

date

workdir="/home/users/tdeckenbrunnen/bda/assignment_3/problem_2"

cat $workdir/outlier_mvt.scala | source /home/users/tdeckenbrunnen/bda/launch-spark-shell.sh

date

exit