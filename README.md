# del-stats

A repository to download DEL statistics

## Downloading

Downloading scripts are found in the `downloading` directory. Before you run
these scripts, you'll first need to type `chmod u+x get_data.sh`. This only
needs to be done once. 

To download data, run `./get_data.sh`. This will take some minutes. 

## Cleaning

To clean the data, you'll need Python. The packages are in the `Pipfile`.
After downloading the data and installing the packages, running

`python cleaning/main.py` will do the job for you. Data will be stored in
`/data`
