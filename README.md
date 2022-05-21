# spothist
***

A tool to extract, process and store Spotify listening history using Python (Prefect), dbt and Postgres.

## What is this?
***
Spothist is a tool to build a complete Spotify listening history.
The Spotify API limits listening history to only the last 50 tracks played, resulting in the vast majority of listening 
history being unavailable.

Spothist calls the API on a set schedule and processes the result, and writes it to a Postgres database.
The processed listening history is structured as a dimensional data model/star schema/Kimball model, so it can be
easily queried and so further analytics can be run on the data.

Each response is also archived as a JSON in case a new database needs to be built or results need to be backfilled etc.

## Tools
***
TBC

## Architecture
***
* Cloud platform - TBC currently local
* Containerisation - TBC plan to use Docker
* Orchestration - Prefect
* Data lake - TBC currently local
* Data warehouse - TBC currently local Postgres instance
* Data visualisation - TBC

## Setup
***
TBC

## Further work
***
* The Spotify API provides multiple genres for each artist which are far too granular for analysis.
  * Therefore the multiple, granular genres need to be mapped to a single high-level genre.