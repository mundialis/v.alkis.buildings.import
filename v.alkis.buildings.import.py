#!/usr/bin/env python3
#
############################################################################
#
# MODULE:      v.alkis.buildings.import
# AUTHOR(S):   Lina Krisztian

# PURPOSE:     Downloads ALKIS building data and imports them into GRASS
#              (currently for Berlin, Hessen, NRW)
# COPYRIGHT:   (C) 2022 by mundialis GmbH & Co. KG and the GRASS Development Team
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
# % options: Brandenburg,Berlin,Baden-Württemberg,Bayern,Bremen,Hessen,Hamburg,Mecklenburg-Vorpommern,Niedersachsen,Nordrhein-Westfalen,Rheinland-Pfalz,Schleswig-Holstein,Saarland,Sachsen,Sachsen-Anhalt
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
from io import BytesIO
import glob
from multiprocessing.pool import ThreadPool
from zipfile import ZipFile
import gzip
from time import sleep
import shutil
from subprocess import Popen, PIPE
import grass.script as grass

sys.path.insert(
    1,
    os.path.join(
        os.path.dirname(sys.path[0]), "etc", "v.alkis.buildings.import"
    ),
)
from download_urls import URLS, filenames, download_dict, bb_layer


rm_vec = [] #WIRD GARNICHT DEFINIERT ODER IM CLEANUP BERÜCKSICHTIGT
path_to_tempdir = None
temp_region = None
output_alkis_temp = None
pid = None
currentpath = os.getcwd()
rmvecmaps = []


def cleanup():
    grass.message(_("Cleaning up..."))
    os.chdir(currentpath)
    # remove unzipped federal state data in tempdir-folder
    if path_to_tempdir:
        if os.path.exists(path_to_tempdir):
            grass.try_rmdir(path_to_tempdir)
    # if aoi_map is given:
    if temp_region:
        # set region back and delete saved region:
        grass.run_command("g.region", region=temp_region)
        grass.run_command(
            "g.remove", type="region", name=temp_region, flags="f", quiet=True
        )
        # remove temp_output (if aoi_map given)
        grass.run_command(
            "g.remove",
            type="vector",
            name=output_alkis_temp,
            flags="f",
            quiet=True,
        )
    for rm_v in rmvecmaps:
        grass.run_command(
            "g.remove", flags="f", type="vector", name=rm_v, quiet=True, stderr=nulldev
        )


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
        except:
            grass.message(_("retry download"))
            if count > 10:
                trydownload = False
                grass.fatal(f"download of {url} not working")
            sleep(10)
    return url

def main():
    global rm_vec, path_to_tempdir, temp_region, output_alkis_temp, pid
    pid = os.getpid()
    """ parser options:
    """
    aoi_map = options["aoi_map"]
    file_federal_state = options["file"]
    load_region = flags["r"]
    """ get federal state
    """
    if file_federal_state:
        with open(file_federal_state) as f:
            federal_states = f.read()
    else:
        federal_states = options["federal_state"]
    """ get URL for corresponding federal state
    """
    URL = None
    fs = None
    for federal_state in federal_states.split(","):
        if federal_state in URLS:
            if federal_state in ["Nordrhein-Westfalen", "Berlin", "Hessen"]:
                URL = URLS[federal_state]
                fs = federal_state
            elif federal_state == "Brandenburg":
                fs =federal_state
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
    if not URL:
        if fs:
            """calculate Brandenburg buildings with Alkis usetypes"""

            global currentpath, rmvecmaps, downloadpath, dldir
            filepath = file_federal_state
            output = "output_alkis_usetype_buildings_BB"
            #dldir = gleicher_path_wie hier

            '''
            if dldir is None or dldir == "":
                dldir = grass.tempdir()
                downloadpath = dldir
            '''
            grass.run_command("g.region", vector=aoi_map)

            all_urls_bl = download_dict["Brandenburg"]
            globstring = "alkis_*.zip"

            # download url, müsste eigentlich auch einfach mit wget gehen
            pool = ThreadPool(3)
            results = pool.imap_unordered(url_response, all_urls_bl)
            for r in results:
                print(r)
            pool.close()
            pool.join()

            # converting ALKIS data from NAS to GPKG
            # unzip downloaded folder
            nas_filenames = glob.glob(globstring)
            for zip_file in nas_filenames:
                with ZipFile(zip_file, "r") as zipObj:
                    # Extract all the contents of zip file in current directory
                    zipObj.extractall()
                    os.remove(zip_file)
            grass.message(_("unzip downloaded zip folder"))

            for zip_file2 in glob.glob("**/*.xml.gz", recursive=True):
                unziped2 = os.path.splitext(zip_file2)[0]
                with gzip.open(zip_file2, "rb") as f_in:
                    # Extract all the contents of zip file in current directory
                    with open(unziped2, "wb") as f_out:
                        shutil.copyfileobj(f_in, f_out)
                os.remove(zip_file2)
            grass.message(_("unzip gzip folder"))

               # convert NAS .xml files to one single GPKG with OGR library
            grass.message(_("convert NAS .xml files to a single GPKG with OGR library"))
            xml_files = glob.glob("**/*.xml", recursive=True)
            epsg = grass.parse_command("g.proj", flags="g")["srid"]

            for xml_file in xml_files:
                process = grass.Popen(
                    [
                        "ogr2ogr",
                        "alkis.gpkg",
                        "-f",
                        "GPKG",
                        "-append",
                        "-update",
                        "-a_srs",
                        epsg,
                        xml_file,
                    ]
                )
                process.wait()
                print(xml_file)

            gfs_files = glob.glob("**/*gfs", recursive=True)
            for gfs in gfs_files:
                os.remove(gfs)

            # import ALKIS GPKG files to GRASS
            grass.message(_("Import ALKIS.gpkg to GRASS"))
            importfile = "alkis.gpkg"
            importfilestring = "".join(importfile)
            filename = os.path.splitext(importfilestring)[0]
            print(filename)

            layernames = []
            layernames_BL = bb_layer["layernames_bl"]
            layernames.extend(layernames_BL)

            # write existing layers of vectormaps to list
            args_ogr = ["ogrinfo", importfilestring]
            process = Popen(args_ogr, stdout=PIPE, stderr=PIPE)

            stdout = process.communicate()[0].decode("utf-8").strip()

            layernames_in_file = [layername for layername in layernames if layername in stdout]

            # import gpkg file as seperate vectormaps for every layer
            vectormaps = []
            for nutzungsart in layernames_in_file:
                grass.message(f"Importing {nutzungsart}...")
                vectormap = filename.replace("-", "_") + str(nutzungsart)
                objektart_name = nutzungsart.split("_", 1)[1]

                grass.run_command(
                    "v.in.ogr",
                    input=importfile,
                    layer=nutzungsart,
                    # quiet=True,
                    output=vectormap,
                    flags="r",
                )

                grass.run_command(
                    "v.db.addcolumn",
                    map=vectormap,
                    columns="objektart VARCHAR(30)",
                    quiet=False,
                )

                grass.run_command(
                    "v.db.update",
                    map=vectormap,
                    layer=1,
                    column="objektart",
                    value=objektart_name,
                    quiet=False,
                )

                # Get the list of columns from v.info output
                vinfo_output = grass.read_command("v.info", map=vectormap, flags="c")

                column_list = [
                    line.split()[0] for line in vinfo_output.splitlines() if line.strip()
                ]

                # Filter the column list to exclude the 'objektart', 'id' and 'name' columns
                column_list_filtered = [
                    col
                    for col in column_list
                    if col
                    not in [
                        "INTEGER|cat",
                        "CHARACTER|identifier",
                        "CHARACTER|gml_id",
                        "CHARACTER|objektart",
                    ]
                ]

                colname_list_filtered = []
                for string in column_list_filtered:
                    colname = string.split("|")[1]
                    colname_list_filtered.append(colname)

                # Join the column names into a string separated by commas
                column_names = ",".join(colname_list_filtered)

                # Call v.db.drop command with the filtered column names
                grass.run_command("v.db.dropcolumn", map=vectormap, columns=column_names)

                grass.message(f"Importing {nutzungsart} done.")

                vectormaps.append(vectormap)

            grass.run_command(
                "v.patch", input=vectormaps, output=output, flags="e", quiet=True
            )


        if not fs:
            grass.fatal(
                _(
                    "AOI is located in federal state(s),"
                    "which are not yet implemented."
                )
            )

    if URL:
        """load data:"""
        # create tempdirectory for unzipping files
        path_to_tempdir = grass.tempdir()

        # file of interest in zip
        filename = filenames[fs]
        alkis_source = os.path.join(path_to_tempdir, filename)
        # final output map
        output_alkis_temp = f"output_alkis_temp_{pid}"
        output_alkis = options["output"]
        """ download alkis building data
        """
        grass.message(_("Downloading ALKIS building data..."))
        response = requests.get(URL)
        if not response.status_code == 200:
            sys.exit(
                (
                    "v.alkis.buildings.import was stopped."
                    "The data are currently not available."
                )
            )
        # unzip boundaries
        if federal_state == "Nordrhein-Westfalen" or federal_state == "Hessen":
            zip_file = zipfile.ZipFile(BytesIO(response.content))
            zip_file.extractall(f"{path_to_tempdir}")
        elif federal_state == "Berlin":
            zip_file = py7zr.SevenZipFile(BytesIO(response.content))
            zip_file.extractall(f"{path_to_tempdir}")
        """ import to GRASS DB
        """
        grass.message(_("Importing ALKIS building data..."))
        if aoi_map:
            # region
            temp_region = f"temp_region_{pid}"
            # save current region for setting back later in cleanup
            grass.run_command("g.region", save=temp_region, quiet=True)
            # set region to aoi_map
            grass.run_command("g.region", vector=aoi_map, quiet=True)
            grass.run_command(
                "v.import",
                input=alkis_source,
                output=output_alkis_temp,
                extent="region",
                flags="o",
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
                flags="o",
                quiet=True,
            )
        else:
            grass.run_command(
                "v.import",
                input=alkis_source,
                output=output_alkis,
                flags="o",
                quiet=True,
            )
    grass.message(_("Done importing ALKIS building data."))

    


if __name__ == "__main__":
    options, flags = grass.parser()
    atexit.register(cleanup)
    main()
