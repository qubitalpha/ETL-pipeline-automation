# Data engineering challenge

The purpose of this assignment is to evaluate your ability to understand business requirements and your skills in preparing an [ETL](https://en.wikipedia.org/wiki/Extract,_transform,_load) pipeline.

In this scenario, your _clients_ are business analysts working on an analysis of lifestyle in Iceland.

# Requirements

Extend the existing ETL pipeline to ingest two new datasets into an existing database. We will execute your code as described below and expect the database to contain three tables in the `assignment-dev.iceland` schema:

- `concerts` for which all the work has already been done for you
- `earthquakes`
- `samferda_drivers`

We expect you keep your work versioned in Git; you can post it privately to a cloud service like GitHub and make us collaborators, or archive your local repository and send us a link to the file. We will treat your submission as a pull request and run a diff between the version we sent you and the one you provide back. Besides coding skills, we care about the way you word commit messages.

# Understanding the existing code

For this exercise, the pipeline is a python script that calls a variety of pre-written modules to read data from files in the `data/` directory, transform it, and store it in a postgres database.

This is an explanation of the directory structure:

- `data/` contains JSON files to be ingested. Don't make changes here.
- `data_models/` contains python classes that define how tables are created in the database. **You will need to create new data model classes for the missing tables.**
- `database/` contains the base class that defines an interface to the database. You likely don't need to make changes here.
- `etl/` contains ETL code
    - `etl/etl.py` contains base classes from which all individual ETL jobs inherit. You likely don't need to make changes here.
    - `etl/iceland_api/` contains the job files for each data set. **You will need to create new job classes for the missing jobs.**
    - `tests/` is currently unused, but this is where tests for the various classes would be stored.
- `docker-compose.yml` defines the containers for this exercise.
- `Dockerfile-python` tells Docker how to build the container for the ETL jobs.
- `python-packages.pip` lists python package dependencies.
- `README.md` is this file.
- `start_all_the_things.py` is the script that gets all things going. **You will have to modify it so your jobs are executed.**

The code is self-contained and runs using docker-compose. Make sure Docker is running on your system, then execute the following command to build and run two containers: a database, and the ETL application.

```bash
./deploy.sh
```

You should now be able to connect to the postgres database with your favorite client using the following details:

| | |
|---|---|
| host | localhost |
| port | 1234 |
| database | postgres |
| user | example |
| password | example |

# Suggested steps

Use the information below to determine the data model (which data to ingest and what to call it in the database). Then, take a look at the existing data model and job files for `concerts` and use them as a guide to write the new classes.

Make sure you don't forget to modify `start_all_the_things.py` so your jobs are executed!

You will impress us if you figure out how to write and execute tests of your code.

# Data model

The data model has been determined by analysts to be as follows:

## Drivers table

Data to be ingested: `data/api_is_samferda_drivers.json`

| Column name | Notes | Type |
| --- | --- | --- |
| from | The departure city | string |
| to | The destination city | string |
| timestamp | Put together from original `date` and `time` fields | timestamp |


## Earthquakes table

Data to be ingested: `data/api_is_earthquake.json`

| Column name | Notes | Type |
| --- | --- | --- |
| latitude | | number |
| longitude | | number |
| magnitude | Called `size` in original| number |
| timestamp | | timestamp |

## Concerts table

Data to be ingested: `data/api_is_concerts.json`

| Column name | Notes | Type |
| --- | --- | --- |
| event | Called `eventDateName` in original | string |
| band | Called `userGroupName` in original | string |
| hall | Called `eventHallName` in original| string |
| timestamp | Called `dateOfShow` in original| timestamp |
