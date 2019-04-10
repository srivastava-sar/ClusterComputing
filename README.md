# clusterComputing
This repository is for the course COMP90024_2019_SM1 - cloud and cluster computing assignment and project work

This assignment is about leavaraging university of Melbourne HPC facility for twitter. This is going to be a joint assignment


Installation instructions:

1. You will need the following files in your working directory on SPARTTAN in order to run the assignment 1 solution
a. cloudComputingOnlyMelbGrid.py
b. bigTwitter.json
c. Python version Python/3.6.4-intel-2017.u2
d. twitter_1N1C.slurm,twitter_1N8C.slurm,twitter_2N4C.slurm

2. To execute the python code, the following command needs to be executed on the terminal without quotes:
"sbatch twitter_2N4C.slurm"

3. The output will be generated in the "slurm-<jobID>.out" file. The file will contain the following information:
a. Grid-wise total tweets
b. Grid-wise top 5 tweets hashtags
c. Total time taken to run the job.

