# Turboingestor
General dataset ingestion for the Sustain ecosystem.


## Usage
#### Setup: 
1. `npm i`
2. `npm install variety-cli -g`
3. Download whatever data you are importing into some directory.

#### Running
`./run.sh <name_of_new_collection> <data_format> <filepath> <indexes> [-n]`, wherein:
##### `name_of_new_collection`
Name of the new collection you are creating within mongodb.
##### `data_format`
Format of data you are ingesting. Accepted values are:
- `census`, format downloaded from nhgis.org, csv format, one header line.
- `census_w_descriptive_header`, format downloaded from nhgis.org, csv format, two header lines. This will make a file in `out/` which maps the first header to the descriptive one. This also will make a new parallel copy of the csv with two headers, but delete the second header in the original.
- `neon`, Neon format, WIP.
##### `filepath`
Path to whatever file you are ingesting.
##### `indexes`
Space seperated string of indexes to create.
##### `-n` (optional)
Don't re-build. Useful when not developing.

##### Example:
`./run.sh county_race census ./path/to/censuscsv.csv "GISJOIN index2 index3"`
