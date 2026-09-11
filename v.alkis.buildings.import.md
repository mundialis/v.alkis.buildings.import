## DESCRIPTION

*v.alkis.buildings.import* downloads ALKIS building data and imports
them into GRASS. Alternatively, local data can be imported. To download
the data, the federal_state(s) of the area of interest has to be given.
Either given in a text file, comma-separated for multiple federal_states
(**file** option). Or given directly as input, comma-separated for
multiple federal_states (**federal_state** option). Note: Importing all
data for a complete federal state could fail, due to too small RAM. To
avoid this, the data can be loaded for a smaller region, either by using
the **aoi_map** option, or by using the **-r** flag. With **aoi_map**,
the data are imported only for the given vector map (given in GRASS DB).
With the **-r** flag, the data are imported only for the current set region.

Implemented federal state options are:

- Baden-Würrtemberg: only local data
- Berlin
- Brandenburg
- Hessen
- Nordrhein-Westfalen
- Sachsen

For local data import the parameter **local_data_dir** has to be given
and the folder structure has to be as follows:

```sh
/path/to/ALKIS_buildings/
├── BW/*.gpkg
└── NW/*.shp
└── ...
```

If local data does not overlap with AOI, data will be downloaded from
Open Data portals if federal state supports Open Data.

## REQUIREMENTS

```sh
py7zr
```

,

```sh
grass-gis-helpers
```

need to be installed.

## EXAMPLE

### Load all available ALKIS building data with federal state information given by **federal_state** option

```sh
v.alkis.buildings.import output=alkis_buildings federal_state=Nordrhein-Westfalen
```

### Load all available ALKIS building data with federal state information given by **file** option

```sh
v.check.bundesland input=polygon_aoi file=federal_state.txt
v.alkis.buildings.import output=alkis_buildings file=federal_state.txt
```

With file content of `federal_state.txt`:

```sh
Berlin,Brandenburg
```

### Load ALKIS building data for AOI given by `aoi_map_example`

```sh
v.alkis.buildings.import output=alkis_buildings federal_state=Nordrhein-Westfalen aoi_map=aoi_map_example
```

### Load ALKIS building data for current set region

```sh
v.alkis.buildings.import output=alkis_buildings federal_state=Nordrhein-Westfalen -r
```

### Load ALKIS building data from local file

```sh
v.alkis.buildings.import aoi_map=AOI_BW output=buildings_bw federal_state=Baden-Württemberg local_data_dir=/path/to/ALKIS_buildings/
```

## SEE ALSO

*[v.import](v.import.md)*

## AUTHORS

Lina Krisztian, [mundialis](https://www.mundialis.de/) Anika Weinmann,
[mundialis](https://www.mundialis.de/) Julia Haas,
[mundialis](https://www.mundialis.de/)
