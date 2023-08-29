#!/usr/bin/env python3
#
############################################################################
#
# MODULE:      v.alkis.buildings.import
# AUTHOR(S):   Lina Krisztian, Anika Weinmann

# PURPOSE:     Downloads ALKIS building data and imports them into GRASS
#              (currently for Berlin, Hessen, NRW)
# COPYRIGHT:   (C) 2022-2023 by mundialis GmbH & Co. KG and the GRASS
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
# % description: Downloads and imports ALKIS building data (currently for Berlin, Hessen, NRW) into GRASS DB
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

import requests
import zipfile
import py7zr
import os
import sys
import atexit
import glob
from io import BytesIO
from zipfile import ZipFile
from time import sleep
import shutil
import grass.script as grass
from multiprocessing.pool import ThreadPool
from datetime import datetime
from datetime import timedelta
from subprocess import Popen, PIPE


sys.path.insert(
    1,
    os.path.join(
        os.path.dirname(sys.path[0]), "etc", "v.alkis.buildings.import"
    ),
)
from download_urls import URLS, filenames, BB_districts, download_dict

orig_region = None
output_alkis_temp = None
dldir = None
pid = None
currentpath = os.getcwd()
rmvecmaps = []


def cleanup():
    grass.message(_("Cleaning up..."))
    os.chdir(currentpath)
    nulldev = open(os.devnull, "w")
    # if aoi_map is given:
    if orig_region:
        # set region back and delete saved region:
        grass.run_command("g.region", region=orig_region)
        grass.run_command(
            "g.remove",
            type="region",
            name=orig_region,
            flags="f",
            quiet=True,
            stderr=nulldev,
        )
        # remove temp_output (if aoi_map given)
        grass.run_command(
            "g.remove",
            type="vector",
            name=output_alkis_temp,
            flags="f",
            quiet=True,
            stderr=nulldev,
        )
    for rm_v in rmvecmaps:
        grass.run_command(
            "g.remove",
            flags="f",
            type="vector",
            name=rm_v,
            quiet=True,
            stderr=nulldev,
        )
    if not flags["d"]:
        shutil.rmtree(dldir)


def url_response(url):
    filename_start_pos = url.rfind("/") + 1
    filename = url[filename_start_pos:]

    trydownload = True
    count = 0
    while trydownload:
        try:
            count += 1
            response = requests.get(url, stream=True, timeout=800)
            response.raise_for_status()
            with open(str(filename), "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            trydownload = False
        except Exception:
            grass.message(_("retry download"))
            if count > 10:
                trydownload = False
                grass.fatal(f"download of {url} not working")
            sleep(10)
    return url


def administrative_boundaries(polygon_name):
    # # # returns list of districts, for AOI/region

    # url of administrative boundaries
    URL = (
        "https://daten.gdz.bkg.bund.de/produkte/vg/vg5000_0101/"
        "aktuell/vg5000_01-01.utm32s.shape.ebenen.zip"
    )
    # file of administrative boundaries in zip
    filename = os.path.join(
        "vg5000_01-01.utm32s.shape.ebenen",
        "vg5000_ebenen_0101",
        "VG5000_KRS.shp",
    )

    # create tempdirectory for unzipping files
    path_admin_boundaries = grass.tempdir()
    # download administrative boundaries as .zip
    response = requests.get(URL)
    if not response.status_code == 200:
        sys.exit(
            (
                "v.alkis.nutzung.import was stopped. The data of the"
                "district boundaries are currently not available."
            )
        )
    # unzip boundaries
    zip_file = ZipFile(BytesIO(response.content))
    zip_file.extractall(path_admin_boundaries)

    # import district shapefile
    vec_all_district_vec = f"all_districts_vec_{os.getpid()}"
    rmvecmaps.append(vec_all_district_vec)
    grass.run_command(
        "v.import",
        input=os.path.join(path_admin_boundaries, filename),
        output=vec_all_district_vec,
        quiet=True,
    )

    # get district of AOI/region-polygon
    vec_poly_district_vec = f"poly_districts_vec_{os.getpid()}"
    rmvecmaps.append(vec_poly_district_vec)
    grass.run_command(
        "v.clip",
        input=vec_all_district_vec,
        clip=polygon_name,
        output=vec_poly_district_vec,
        flags="d",
        quiet=True,
    )

    if vec_poly_district_vec:
        KRS_list = list(
            grass.parse_command(
                "v.db.select",
                map=vec_poly_district_vec,
                columns="GEN",
                flags="c",
            ).keys()
        )
        grass.message(KRS_list)
    return KRS_list


def download_brandenburg(aoi_map):
    """Download and prepare data for Brandenburg"""
    # TODO check if data area already downloaded

    # select Landkreise
    if not aoi_map:
        aoi_map = f"aoi_region_{grass.tempname(12)}"
        rmvecmaps.append(aoi_map)
        grass.run_command("v.in.region", output=aoi_map)
    KRS_list = administrative_boundaries(aoi_map)
    filtered_urls = []
    kbs_zips = []
    all_urls_bl = download_dict["Brandenburg"]
    for KRS in KRS_list:
        for key, val in BB_districts.items():
            if val == KRS:
                kbs_url = [
                    url
                    for url in all_urls_bl
                    if f"ALKIS_Shape_{key}.zip" in url
                ][0]
                kbs_zip = os.path.basename(kbs_url)
                kbs_zips.append(kbs_zip)
                if not os.path.isfile(os.path.join(dldir, kbs_zip)):
                    filtered_urls.append(kbs_url)

    grass.message(
        _(f"Downloading {len(filtered_urls)} files from {len(kbs_zips)}...")
    )
    os.chdir(dldir)
    pool = ThreadPool(3)
    results = pool.imap_unordered(url_response, filtered_urls)
    for r in results:
        print(r)
    pool.close()
    pool.join()
    os.chdir(currentpath)

    # for Brandenburg shape files
    shp_files = []
    globstring = "ALKIS_Shape_*.zip"
    zip_files = glob.glob(os.path.join(dldir, globstring))
    for zip_file in zip_files:
        zip_base_name = os.path.basename(zip_file)
        shp_dir = os.path.join(dldir, zip_base_name.rsplit(".", 1)[0])
        if not os.path.isdir(shp_dir):
            os.makedirs(shp_dir)
        if zip_base_name in kbs_zips:
            with ZipFile(zip_file, "r") as zipObj:
                # Extract only building-file in download directory
                listOfFileNames = zipObj.namelist()
                for fileName in listOfFileNames:
                    # should be nutzung and nutzungFlurstueck
                    if "gebauedeBauwerk" in fileName:
                        file_path = os.path.join(shp_dir, fileName)
                        if not os.path.isfile(file_path):
                            zipObj.extract(fileName, shp_dir)
                        if fileName.endswith(".shp"):
                            shp_files.append(file_path)
    grass.message(_("unzip downloaded zip folder"))
    return shp_files


def import_single_alkis_source(
    alkis_source, aoi_map, load_region, output_alkis, fs
):
    """Importing single ALKIS source"""
    flags = ""
    if fs == "Hessen":
        flags = "o"
    if aoi_map:
        # set region to aoi_map
        grass.run_command("g.region", vector=aoi_map, quiet=True)
        grass.message(f"alkis_source: {alkis_source}")
        if fs == "Thüringen":
            # parse CRS of current location
            proj_location = grass.parse_command("g.proj", flags="g")["srid"]
            # change CRS of alkis_source vector data from epsg:4647 to proj_location
            alkis_source_proj = alkis_source[:-4] + "_proj.gpkg"
            ps = grass.Popen((
                    "ogr2ogr",
                    "-s_srs",
                    "EPSG:4647",
                    "-t_srs",
                    proj_location,
                    "-f", "GPKG",
                    alkis_source_proj,
                    alkis_source
                ))
            returncode = ps.wait()
            if returncode != 0:
                grass.message(_("Assigning new CRS to ALKIS input data failed!"))
            # while schleife bis 0.001 abhängig von features, die silver polygone sind
            # run v.in.ogr with snap tolerance to identify silver polygons
            # if there are silver polygons remove them
            snap = 0.00000001
            read_out = grass.read_command(
                "v.in.ogr",
                input=alkis_source_proj,
                output=output_alkis_temp,
                snap=snap,
                flags="r",
                verbose=True,
                quiet=True,
            )
            import pdb; pdb.set_trace()
            # overlap_areas = int((str(grass.parse_command(
                # "v.in.ogr",
                # input=alkis_source_proj,
                # output=output_alkis_temp,
                # snap=snap,
                # flags="r",
                # verbose=True,
                # quiet=True,
            #     )["Überlappende Fläche"]).split("(")[1].split(" ")[0]))
            parse_args = ["v.in.ogr", f"input={alkis_source_proj}", f"output={output_alkis_temp}", f"snap={snap}", "-r", "--verbose", "--quiet"]
            process = Popen(parse_args, stdout=PIPE, stderr=PIPE)
            stdout = process.communicate()[0].decode("utf-8").strip()
            if overlap_areas > 0:
                snap = snap * 10
                # run command until snap tolerance is 0.001
                while snap > 0.01:
                    grass.run_command(
                        "v.in.ogr",
                        input=alkis_source_proj,
                        output=output_alkis_temp,
                        snap=snap,
                        flags="r",
                        overwrite=True,
                        verbose=True,
                        quiet=True,
                    )
                    snap = snap * 10
                # find amount of silver polygons if still existent
                overlap_areas = int((str(grass.parse_command(
                    "v.in.ogr",
                    input=alkis_source_proj,
                    output=output_alkis_temp,
                    snap=snap,
                    flags="r",
                    verbose=True,
                    quiet=True,
                    )["Überlappende Fläche"]).split("(")[1].split(" ")[0]))
                # if there are still silver polygons remove them with runnging v.clean
                if overlap_areas > 0:
                    grass.run_command(
                        "v.clean",
                        input=output_alkis_temp,
                        output=output_alkis_temp,
                        tool="rmarea",
                        threshold=1,
                        overwrite=True,
                        quiet=True,
                    )
        else:
            grass.run_command(
                "v.import",
                input=alkis_source,
                output=output_alkis_temp,
                extent="region",
                flags=flags,
                quiet=True,
            )
        grass.run_command(
            "v.clip",
            input=output_alkis_temp,
            clip=aoi_map,
            output=output_alkis,
            flags="d",
            quiet=True,
        )
    elif load_region:
        grass.run_command(
            "v.import",
            input=alkis_source,
            output=output_alkis,
            extent="region",
            flags=flags,
            quiet=True,
        )
    else:
        grass.run_command(
            "v.import",
            input=alkis_source,
            output=output_alkis,
            flags=flags,
            quiet=True,
        )


def change_col_text_type(map):
    """Change column type from CHARACTER to TEXT"""
    column_list = {
        col.split("|")[1]: col.split("|")[0]
        for col in grass.parse_command("v.info", map=map, flags="cg")
    }
    for col, col_type in column_list.items():
        if col_type == "CHARACTER":
            grass.run_command(
                "v.db.addcolumn",
                map=map,
                column=f"{col},{col}_tmp_{pid}",
                quiet=True,
            )
            grass.run_command(
                "v.db.addcolumn",
                map=map,
                columns=f"{col} TEXT",
                quiet=True,
            )
            grass.run_command(
                "v.db.update",
                map=map,
                layer=1,
                column=col,
                query_column=f"{col}_tmp_{pid}",
                quiet=True,
            )


def import_shapefiles(shape_files, output_alkis, aoi_map=None):
    """Import shapefiles (for Brandenburg)"""
    if aoi_map:
        grass.run_command("g.region", vector=aoi_map, quiet=True)
    out_tempall = list()
    for shape_file in shape_files:
        grass.message(_(f"Importing {shape_file}"))
        out_temp = f"out_temp_{pid}_{os.path.splitext(os.path.basename(shape_file))[0]}"
        rmvecmaps.append(out_temp)
        grass.run_command(
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
            for col in grass.parse_command("v.info", map=out_temp, flags="cg")
        }
        drop_columns = [
            el
            for el in column_list
            if el
            not in [
                "cat",
                "aktualit",
                "gebnutzbez",
                "funktion",
                "anzahlgs",
                "lagebeztxt",
            ]
        ]
        grass.run_command(
            "v.db.dropcolumn",
            map=out_temp,
            columns=drop_columns,
            quiet=True,
        )
    out = output_alkis
    if aoi_map:
        out = output_alkis_temp
    if len(out_tempall) > 1:
        grass.run_command(
            "v.patch", input=out_tempall, output=out, flags="e", quiet=True
        )
    else:
        grass.run_command("g.rename", vector=f"{out_tempall[0]},{out}")
    if aoi_map:
        grass.run_command(
            "v.clip",
            input=output_alkis_temp,
            clip=aoi_map,
            output=output_alkis,
            flags="d",
            quiet=True,
        )


def main():
    global orig_region, output_alkis_temp, pid, dldir
    pid = os.getpid()

    # parser options:
    aoi_map = options["aoi_map"]
    file_federal_state = options["file"]
    load_region = flags["r"]
    dldir = options["dldir"]
    output_alkis_temp = f"output_alkis_temp_{pid}"
    rmvecmaps.append(output_alkis_temp)
    output_alkis = options["output"]

    # temp download path, if not explicite path given
    if not dldir:
        dldir = grass.tempdir()
    else:
        if not os.path.exists(dldir):
            grass.message(
                _(f"Download folder {dldir} does not exist. Creating it...")
            )
            os.makedirs(dldir)

    # get federal state
    if file_federal_state:
        with open(file_federal_state) as f:
            federal_states = f.read()
    else:
        federal_states = options["federal_state"]

    # region
    orig_region = f"orig_region_{pid}"
    # save current region for setting back later in cleanup
    grass.run_command("g.region", save=orig_region, quiet=True)

    # get URL for corresponding federal state
    URL = None
    fs = None
    for federal_state in federal_states.split(","):
        if federal_state in URLS:
            if federal_state in ["Nordrhein-Westfalen", "Berlin", "Hessen", "Thüringen"]:
                URL = URLS[federal_state]
                fs = federal_state
            elif federal_state == "Brandenburg":
                fs = federal_state
                URL = URLS[federal_state]
            else:
                grass.warning(
                    _(f"Support for {federal_state} is not yet implemented.")
                )
        else:
            if options["file"]:
                grass.fatal(
                    _(
                        "Non valid name of federal state,"
                        " in 'file'-option given"
                    )
                )
            elif options["federal_state"]:
                grass.fatal(
                    _(
                        "Non valid name of federal state,"
                        " in 'federal_states'-option given"
                    )
                )
    # so far, just Berlin, Brandenburg, Hessen and NRW are implemented;
    # in case single federal state given, and not NRW:
    #   skips following part
    #   + grass.message: see above
    # in case multiple federal states given, and at least one of them is NRW:
    #   import data only for NRW area
    if not URL and fs == "Brandenburg":
        alkis_source = download_brandenburg(aoi_map)
    elif not URL and not fs:
        grass.fatal(
            _(
                "AOI is located in federal state(s),"
                "which are not yet implemented."
            )
        )

    if URL:
        """download alkis building data"""
        # create tempdirectory for unzipping files
        # file of interest in zip
        filename = filenames[fs]
        alkis_source = os.path.join(dldir, filename)
        if not os.path.isfile(alkis_source):
            grass.message(_("Downloading ALKIS building data..."))
            if federal_state == "Hessen":
                # insert current date into download URL
                # try dates of yesterday and tomorrow if its not working
                today = datetime.now().strftime("%Y%m%d")
                yesterday = (datetime.now() - timedelta(days=1)).strftime(
                    "%Y%m%d"
                )
                tomorrow = (datetime.now() + timedelta(days=1)).strftime(
                    "%Y%m%d"
                )
                dates = [today, yesterday, tomorrow]
                URL = URL.replace("DATE", today)
                response = requests.get(URL)
                if not response.status_code == 200:
                    URL = URL.replace(dates[0], dates[1])
                    response = requests.get(URL)
                if not response.status_code == 200:
                    URL = URL.replace(dates[1], dates[2])
                    response = requests.get(URL)
            else:
                response = requests.get(URL)

            if not response.status_code == 200:
                sys.exit(
                    (
                        "v.alkis.buildings.import was stopped."
                        "The data are currently not available."
                    )
                )
            # unzip boundaries
            if federal_state in ["Nordrhein-Westfalen", "Hessen", "Thüringen"]:
                zip_file = zipfile.ZipFile(BytesIO(response.content))
                zip_file.extractall(dldir)
            elif federal_state == "Berlin":
                zip_file = py7zr.SevenZipFile(BytesIO(response.content))
                zip_file.extractall(dldir)
    """ import to GRASS DB
    """
    grass.message(_("Importing ALKIS building data..."))
    if isinstance(alkis_source, str):
        import_single_alkis_source(
            alkis_source, aoi_map, load_region, output_alkis, federal_state
        )
    else:
        import_shapefiles(alkis_source, output_alkis, aoi_map)

    grass.message(_("Done importing ALKIS building data."))


if __name__ == "__main__":
    options, flags = grass.parser()
    atexit.register(cleanup)
    main()
