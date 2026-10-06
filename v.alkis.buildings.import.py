#!/usr/bin/env python3
#
############################################################################
#
# MODULE:      v.alkis.buildings.import
# AUTHOR(S):   Lina Krisztian, Anika Weinmann, Julia Haas

# PURPOSE:     Downloads ALKIS building data and imports them into GRASS
#              (currently for Berlin, Brandenburg, Hessen, NRW, Sachsen)
# COPYRIGHT:   (C) 2022-2024 by mundialis GmbH & Co. KG and the GRASS
#              Development Team
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
#############################################################################

# %Module
# % description: Downloads and imports ALKIS building data (currently for Berlin, Brandenburg, Hessen, NRW, Sachsen) into GRASS DB
# % keyword: vector
# % keyword: import
# % keyword: ALKIS
# %end

# %option G_OPT_V_OUTPUT
# % key: output
# % required: yes
# %end

# %option G_OPT_V_INPUT
# % key: aoi_map
# % required: no
# % description: Vector map to restrict ALKIS building import to
# %end

# %option G_OPT_M_DIR
# % key: local_data_dir
# % required: no
# % description: Directory with vector map of ALKIS buildings to import
# %end

# %option G_OPT_F_INPUT
# % key: file
# % required: no
# % description: Text file containing comma separated federal states to load ALKIS building data for
# %end

# %option
# % type: string
# % key: federal_state
# % multiple: yes
# % required: no
# % description: Federal state to load ALKIS building data for
# % options: Brandenburg,Berlin,Baden-Württemberg,Bayern,Bremen,Hessen,Hamburg,Mecklenburg-Vorpommern,Niedersachsen,Nordrhein-Westfalen,Rheinland-Pfalz,Schleswig-Holstein,Saarland,Sachsen,Sachsen-Anhalt,Thüringen
# %end

# %option
# % key: dldir
# % label: Path of output folder
# % description: Path of folder for Download outputdata e.g./home/usr/alkis/output
# % required: no
# % multiple: yes
# %end

# %flag
# % key: d
# % description: keep downloads
# %end

# %flag
# % key: r
# % description: Restrict ALKIS building data import to current region
# %end

# %rules
# % required: federal_state, file
# %end

# %rules
# % excludes: file, federal_state
# %end

# %rules
# % excludes: aoi_map, -r
# %end

import atexit
import os
import pathlib
import sys
import zipfile
from datetime import datetime, timedelta
from io import BytesIO
from multiprocessing.pool import ThreadPool
from time import sleep
from zipfile import ZipFile
from zoneinfo import ZoneInfo

import grass.script as gs
import py7zr
import requests
from grass_gis_helpers.cleanup import general_cleanup

sys.path.insert(
    1,
    str(pathlib.Path(sys.path[0]).parent / "etc" / "v.alkis.buildings.import"),
)
# pylint: disable=wrong-import-position
from download_urls import (
    BB_DISTRICTS,
    BUILDINGS_FILENAMES,
    URLS,
    download_dict,
)
from federal_state_info import FS_ABBREVIATION

ORIG_REGION = None
OUTPUT_ALKIS_TEMP = None
DLDIR = None
PID = None
currentpath = pathlib.Path.cwd()
rm_vectors = []

# The Hessen download URLs embed a date in German local time.
BERLIN_TZ = ZoneInfo("Europe/Berlin")

HTTP_OK = 200
MAX_DOWNLOAD_ATTEMPTS = 10
DOWNLOAD_TIMEOUT = 800


def cleanup() -> None:
    """Remove created objects when finished or failed."""
    rm_dirs = []
    if not flags["d"]:
        rm_dirs.append(DLDIR)

    general_cleanup(
        orig_region=ORIG_REGION,
        rm_vectors=rm_vectors,
        rm_dirs=rm_dirs,
    )


def download_file(url, filename):
    """Write the content of a URL to a local file."""
    response = requests.get(url, stream=True, timeout=DOWNLOAD_TIMEOUT)
    response.raise_for_status()
    with pathlib.Path(filename).open("wb") as file:
        file.writelines(response.iter_content(chunk_size=8192))


def url_response(url):
    """Download requested data and retry the download if it failed."""
    filename = url[url.rfind("/") + 1 :]

    for _ in range(MAX_DOWNLOAD_ATTEMPTS):
        try:
            download_file(url, filename)
            break
        except Exception:
            gs.message(_("Retrying download."))
            sleep(10)
    else:
        gs.fatal(_("Download of {} not working.").format(url))
    return url


def administrative_boundaries(aoi_name):
    """Return a list of districts overlapping with AOI/region."""
    # url of administrative boundaries
    url = (
        "https://daten.gdz.bkg.bund.de/produkte/vg/vg5000_0101/"
        "aktuell/vg5000_01-01.utm32s.shape.ebenen.zip"
    )
    # file of administrative boundaries in zip
    filename = "vg5000_ebenen_0101/VG5000_KRS.shp"

    # check if URL is reachable
    response = requests.get(url, timeout=DOWNLOAD_TIMEOUT)
    if response.status_code != HTTP_OK:
        gs.fatal(
            _(
                "v.alkis.buildings.import was stopped. The data of the"
                " district boundaries are currently not available.",
            ),
        )

    # download and import administrative boundaries
    gs.run_command(
        "g.region",
        vect=aoi_name,
        quiet=True,
    )

    vsi_command = f"/vsizip/vsicurl/{url}/{filename}"
    districts_vec = f"all_districts_vec_{os.getpid()}"
    rm_vectors.append(districts_vec)
    gs.run_command(
        "v.import",
        input=vsi_command,
        output=districts_vec,
        extent="region",
        overwrite=True,
        quiet=True,
    )

    # get district of AOI/region-polygon
    krs_list = list(
        gs.parse_command(
            "v.db.select",
            map=districts_vec,
            columns="GEN",
            flags="c",
        ).keys(),
    )
    gs.verbose(
        _("Districts overlapping with AOI/region: {}").format(
            ", ".join(krs_list),
        ),
    )
    return krs_list


def download_alkis_buildings_bb(aoi_map):
    """Download and prepare data for Brandenburg."""
    # TODO check if data area already downloaded

    # select Landkreise
    if not aoi_map:
        aoi_map = f"aoi_region_{gs.tempname(12)}"
        rm_vectors.append(aoi_map)
        gs.run_command("v.in.region", output=aoi_map)
    krs_list = administrative_boundaries(aoi_map)
    filtered_urls = []
    kbs_zips = []
    all_urls_bl = download_dict["Brandenburg"]
    for krs in krs_list:
        for key, val in BB_DISTRICTS.items():
            if val == krs:
                kbs_url = next(
                    url
                    for url in all_urls_bl
                    if f"alkis_shape_{key.lower()}.zip" in url
                )
                kbs_zip = pathlib.Path(kbs_url).name
                kbs_zips.append(kbs_zip)
                if not (pathlib.Path(DLDIR) / kbs_zip).is_file():
                    filtered_urls.append(kbs_url)

    gs.message(
        _("Downloading {} files from {}...").format(
            len(filtered_urls),
            len(kbs_zips),
        ),
    )
    os.chdir(DLDIR)
    pool = ThreadPool(3)
    results = pool.imap_unordered(url_response, filtered_urls)
    for result in results:
        gs.verbose(result)
    pool.close()
    pool.join()
    os.chdir(currentpath)

    # for Brandenburg shape files
    shp_files = []
    globstring = "alkis_shape_*.zip"
    zip_files = list(pathlib.Path(DLDIR).glob(globstring))
    for zip_file in zip_files:
        zip_base_name = pathlib.Path(zip_file).name
        if zip_base_name in kbs_zips:
            shp_files.extend(extract_buildings_shapefile(zip_file))
    gs.message(_("Unzipping downloaded data."))
    return shp_files


def extract_buildings_shapefile(zip_file):
    """Extract the building shape file from a downloaded ALKIS zip file."""
    zip_base_name = pathlib.Path(zip_file).name
    shp_dir = pathlib.Path(DLDIR) / zip_base_name.rsplit(".", 1)[0]
    shp_dir.mkdir(parents=True, exist_ok=True)

    shp_files = []
    with ZipFile(zip_file, "r") as zip_obj:
        for file_name in zip_obj.namelist():
            # should be nutzung and nutz-nungFlurstueck
            if "GebauedeBauwerk" not in file_name:
                continue
            file_path = shp_dir / file_name
            if not file_path.is_file():
                zip_obj.extract(file_name, shp_dir)
            if file_name.endswith(".shp"):
                shp_files.append(str(file_path))
    return shp_files


def download_alkis_buildings(fs, url):
    """Download ALKIS building data."""
    # create tempdirectory for unzipping files
    # file of interest in zip
    buildings_filename = BUILDINGS_FILENAMES[fs]
    alkis_source = str(pathlib.Path(DLDIR) / buildings_filename)
    if not pathlib.Path(alkis_source).is_file():
        gs.message(_("Downloading ALKIS building data ({})...").format(fs))
        if fs == "HE":
            # insert current date into download URL
            # try dates of yesterday and tomorrow if it's not working
            now = datetime.now(BERLIN_TZ)
            today = now.strftime("%Y%m%d")
            yesterday = (now - timedelta(days=1)).strftime("%Y%m%d")
            tomorrow = (now + timedelta(days=1)).strftime("%Y%m%d")
            dates = [today, yesterday, tomorrow]
            url = url.replace("DATE", today)
            response = requests.get(url, timeout=DOWNLOAD_TIMEOUT)
            if response.status_code != HTTP_OK:
                url = url.replace(dates[0], dates[1])
                response = requests.get(url, timeout=DOWNLOAD_TIMEOUT)
            if response.status_code != HTTP_OK:
                url = url.replace(dates[1], dates[2])
                response = requests.get(url, timeout=DOWNLOAD_TIMEOUT)
        else:
            response = requests.get(url, timeout=DOWNLOAD_TIMEOUT)

        if response.status_code != HTTP_OK:
            gs.fatal(
                _(
                    "v.alkis.buildings.import was stopped. The data are"
                    " currently not available.",
                ),
            )
        # unzip boundaries
        if url.endswith(".zip"):
            zip_file = zipfile.ZipFile(BytesIO(response.content))
            zip_file.extractall(DLDIR)
        elif url.endswith(".7z"):
            zip_file = py7zr.SevenZipFile(BytesIO(response.content))
            zip_file.extractall(DLDIR)
        else:
            gs.fatal(_("Zip format not (yet) supported."))

    return alkis_source


def import_single_alkis_source(
    alkis_source,
    aoi_map,
    load_region,
    output_alkis,
    f_state,
) -> None:
    """Import a single ALKIS source."""
    alkis_source_fixed = alkis_source
    if f_state == "Hessen":
        # shapefile with missing .prj file, CRS is EPSG:25832
        alkis_source_fixed = alkis_source[:-4] + "_proj.gpkg"
        popen_s = gs.Popen(
            (
                "ogr2ogr",
                "-a_srs",
                "EPSG:25832",
                "-f",
                "GPKG",
                "-nlt",
                "PROMOTE_TO_MULTI",
                alkis_source_fixed,
                alkis_source,
            ),
        )
        returncode = popen_s.wait()
        if returncode != 0:
            gs.fatal(_("Assigning CRS to ALKIS input data failed!"))

    # snap tolerance = 0.1 to remove overlapping areas in some source datasets
    snap = -1
    if f_state == "Thüringen":
        snap = 0.1

    if aoi_map:
        # set region to aoi_map
        gs.run_command("g.region", vector=aoi_map, quiet=True)
        gs.run_command(
            "v.import",
            input=alkis_source_fixed,
            output=OUTPUT_ALKIS_TEMP,
            snap=snap,
            extent="region",
            quiet=True,
            overwrite=True,
        )
        gs.run_command(
            "v.clip",
            input=OUTPUT_ALKIS_TEMP,
            clip=aoi_map,
            output=output_alkis,
            flags="d",
            quiet=True,
        )
    elif load_region:
        gs.run_command(
            "v.import",
            input=alkis_source_fixed,
            output=output_alkis,
            snap=snap,
            extent="region",
            quiet=True,
        )
    else:
        gs.run_command(
            "v.import",
            input=alkis_source_fixed,
            output=output_alkis,
            snap=snap,
            quiet=True,
        )


def change_col_text_type(map) -> None:
    """Change column type from CHARACTER to TEXT."""
    column_list = {
        col.split("|")[1]: col.split("|")[0]
        for col in gs.parse_command("v.info", map=map, flags="cg")
    }

    for col, col_type in column_list.items():
        if col_type == "CHARACTER":
            tmp_col = f"{col}_tmp_{PID}"

            gs.run_command(
                "v.db.addcolumn",
                map=map,
                columns=f"{tmp_col} TEXT",
                quiet=True,
            )

            gs.run_command(
                "v.db.update",
                map=map,
                column=tmp_col,
                query_column=col,
                quiet=True,
            )

            gs.run_command(
                "v.db.dropcolumn",
                map=map,
                column=col,
                quiet=True,
            )

            gs.run_command(
                "v.db.renamecolumn",
                map=map,
                column=f"{tmp_col},{col}",
                quiet=True,
            )


def import_shapefiles(shape_files, output_alkis, aoi_map=None) -> None:
    """Import shapefiles (for Brandenburg)."""
    if aoi_map:
        gs.run_command("g.region", vector=aoi_map, quiet=True)
    out_tempall = []
    for shape_file in shape_files:
        gs.message(_("Importing {}...").format(shape_file))
        out_temp = f"out_temp_{PID}_{pathlib.Path(shape_file).stem}"
        rm_vectors.append(out_temp)
        gs.run_command(
            "v.import",
            input=shape_file,
            output=out_temp,
            extent="region",
            quiet=True,
        )
        out_tempall.append(out_temp)
        # check columns
        change_col_text_type(out_temp)
        column_list = {
            col.split("|")[1]: col.split("|")[0]
            for col in gs.parse_command("v.info", map=out_temp, flags="cg")
        }
        drop_columns = [
            el
            for el in column_list
            if el
            not in {
                "cat",
                "aktualit",
                "gebnutzbez",
                "funktion",
                "anzahlgs",
                "lagebeztxt",
            }
        ]
        gs.run_command(
            "v.db.dropcolumn",
            map=out_temp,
            columns=drop_columns,
            quiet=True,
        )
    out = output_alkis
    if aoi_map:
        out = OUTPUT_ALKIS_TEMP
    patch_vector(out_tempall, out)
    if aoi_map:
        gs.run_command(
            "v.clip",
            input=OUTPUT_ALKIS_TEMP,
            clip=aoi_map,
            output=output_alkis,
            flags="d",
            quiet=True,
        )


def patch_vector(vector_list, output) -> None:
    """Patch vectors from several federal states into one vector."""
    # patch output from several federal states
    if len(vector_list) > 1:
        gs.run_command(
            "v.patch",
            input=vector_list,
            output=output,
            flags="e",
            quiet=True,
        )
    else:
        gs.run_command("g.rename", vector=f"{vector_list[0]},{output}")


def import_local_data(aoi_map, local_data_dir, fs, output_alkis_fs):
    """Import data from a local file path.

    Args:
        aoi_map (str): name of vector map defining AOI
        local_data_dir (str): path to local data
        fs (str): federal state abbreviation
        output_alkis_fs (str): output for federal state

    Returns:
        imported_local_data (bool): True if local data imported, otherwise False

    """
    imported_local_data = False
    # get files (GPKG or SHP)
    fs_dir = pathlib.Path(local_data_dir) / fs
    buildings_files = list(fs_dir.rglob("*.gpkg"))
    shp_files = list(fs_dir.rglob("*.shp"))
    buildings_files.extend(shp_files)

    # import data for AOI
    imported_buildings_list = []
    for i, buildings_file in enumerate(buildings_files):
        if aoi_map:
            gs.run_command(
                "g.region",
                vector=aoi_map,
                quiet=True,
            )
        gs.run_command(
            "v.import",
            input=buildings_file,
            output=f"{output_alkis_fs}_{i}",
            extent="region",
            quiet=True,
        )
        imported_buildings_list.append(f"{output_alkis_fs}_{i}")
        rm_vectors.append(f"{output_alkis_fs}_{i}")

    # patch outputs
    patch_vector(imported_buildings_list, output_alkis_fs)

    # check if result is not empty
    buildings_info = gs.parse_command(
        "v.info",
        map=output_alkis_fs,
        flags="gt",
    )
    if int(buildings_info["centroids"]) == 0 and fs == "BW":
        gs.fatal(_("Local data does not overlap with AOI."))
    elif int(buildings_info["centroids"]) == 0:
        gs.message(
            _(
                "Local data does not overlap with AOI. Data will be downloaded"
                " from Open Data portal.",
            ),
        )
    else:
        imported_local_data = True

    return imported_local_data


def cleanup_columns(out_alkis) -> None:
    """Remove additional columns."""
    cols = gs.vector_columns(out_alkis)
    rm_cols = [col for col in cols if col not in {"cat", "AGS", "OI", "GFK"}]
    for needed_col in ["AGS", "OI", "GFK"]:
        tmp_col = None
        if needed_col in cols:
            tmp_col = f"{needed_col}_tmp"
            rm_cols.append(tmp_col)
            gs.run_command(
                "v.db.renamecolumn",
                map=out_alkis,
                column=f"{needed_col},{tmp_col}",
                quiet=True,
            )
        gs.run_command(
            "v.db.addcolumn",
            map=out_alkis,
            columns=f"{needed_col} TEXT",
            quiet=True,
        )
        if tmp_col:
            gs.run_command(
                "v.db.update",
                map=out_alkis,
                column=needed_col,
                query_column=tmp_col,
                quiet=True,
            )
    if len(rm_cols) > 0:
        gs.run_command(
            "v.db.dropcolumn",
            map=out_alkis,
            columns=rm_cols,
            quiet=True,
        )


def main() -> None:
    """Process the requested ALKIS import."""
    global DLDIR, ORIG_REGION, OUTPUT_ALKIS_TEMP, PID
    PID = os.getpid()

    # parser options:
    aoi_map = options["aoi_map"]
    file_federal_state = options["file"]
    load_region = flags["r"]
    local_data_dir = options["local_data_dir"]
    DLDIR = options["dldir"]
    OUTPUT_ALKIS_TEMP = f"OUTPUT_ALKIS_TEMP_{PID}"
    rm_vectors.append(OUTPUT_ALKIS_TEMP)
    output_alkis = options["output"]

    # temp download path, if not explicit path given
    if not DLDIR:
        DLDIR = gs.tempdir()
    elif not pathlib.Path(DLDIR).exists():
        gs.message(
            _("Download folder {} does not exist. Creating it...").format(
                DLDIR,
            ),
        )
        pathlib.Path(DLDIR).mkdir(parents=True)

    # get federal state
    if file_federal_state:
        federal_states = (
            pathlib.Path(file_federal_state)
            .read_text(
                encoding="utf-8",
            )
            .strip()
        )
    else:
        federal_states = options["federal_state"].strip()

    # get list of local input folders for federal states
    local_fs_list = []
    if local_data_dir:
        local_fs_list = [
            p.name for p in pathlib.Path(local_data_dir).iterdir()
        ]

    # region
    ORIG_REGION = f"ORIG_REGION{PID}"
    # save current region for setting back later in cleanup
    gs.run_command("g.region", save=ORIG_REGION, quiet=True)

    # loop over federal state and import data
    output_alkis_list = []
    for federal_state in federal_states.split(","):
        if federal_state not in FS_ABBREVIATION:
            gs.fatal(
                _("Non valid name of federal state: {}").format(federal_state),
            )
        fs = FS_ABBREVIATION[federal_state]
        output_alkis_fs = f"{output_alkis}_{fs}"
        output_alkis_list.append(output_alkis_fs)
        rm_vectors.append(output_alkis_fs)

        # check if local data for federal state given
        imported_local_data = False
        if fs in local_fs_list:
            imported_local_data = import_local_data(
                aoi_map,
                local_data_dir,
                fs,
                output_alkis_fs,
            )
        elif fs == "BW":
            gs.fatal(
                _(
                    "No local data for {} available. Is the path correct?",
                ).format(fs),
            )

        # check if federal state is supported
        if not imported_local_data:
            if fs in {"NW", "BE", "HE", "TH", "SN"}:
                url = URLS[fs]
            elif fs == "BB":
                pass
            else:
                gs.warning(
                    _("Support for {} is not yet implemented.").format(fs),
                )

            # so far, just Berlin, Brandenburg, Hessen, NRW and Sachsen are implemented;
            # in case single federal state given, and not NRW:
            #   skips following part
            #   + gs.message: see above
            # in case multiple federal states given, and at least one of them is NRW:
            #   import data only for NRW area
            if fs == "BB":
                alkis_source = download_alkis_buildings_bb(aoi_map)
            else:
                alkis_source = download_alkis_buildings(fs, url)

            # import to GRASS DB
            gs.message(_("Importing ALKIS buildings data ({})...").format(fs))
            if isinstance(alkis_source, str):
                import_single_alkis_source(
                    alkis_source,
                    aoi_map,
                    load_region,
                    output_alkis_fs,
                    federal_state,
                )
            else:
                import_shapefiles(alkis_source, output_alkis_fs, aoi_map)

    # cleanup columns of different federal state data
    for out_alkis in output_alkis_list:
        cleanup_columns(out_alkis)

    # patch output from several federal states
    patch_vector(output_alkis_list, output_alkis)

    gs.message(
        _("Importing ALKIS buildings data <{}> done.").format(output_alkis),
    )


if __name__ == "__main__":
    options, flags = gs.parser()
    atexit.register(cleanup)
    main()
