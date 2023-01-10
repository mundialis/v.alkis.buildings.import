#!/usr/bin/env python3
#
############################################################################
#
# MODULE:      v.alkis.buildings.import
# AUTHOR(S):   Lina Krisztian

# PURPOSE:     Downloads ALKIS building data and imports them into GRASS
#              (currently only for NRW)
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
# % description: Downloads and imports ALKIS building data (currently only for NRW) into GRASS DB
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
# % key: filepath
# % required: no
# % description: Text file containing federal state to load ALKIS building data for
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
# % required: federal_state, filepath
# %end

# %rules
# % excludes: filepath, federal_state
# %end

# %rules
# % excludes: aoi_map, -r
# %end


import requests
import zipfile
import py7zr
import os
import sys
import shutil
import atexit
from io import BytesIO
import grass.script as grass
sys.path.insert(1, os.path.join(os.path.dirname(sys.path[0]),
                                'etc',
                                'v.alkis.buildings.import'))
from download_urls import URLS, filenames


rm_vec = []
path_to_tempdir = None
temp_region = None
output_alkis_temp = None
pid = None


def cleanup():
    # remove unzipped federal state data in tempdir-folder
    if path_to_tempdir:
        if os.path.exists(path_to_tempdir):
            grass.try_rmdir(path_to_tempdir)
    # if aoi_map is given:
    if temp_region:
        # set region back and delete saved region:
        grass.run_command("g.region", region=temp_region)
        grass.run_command("g.remove",
                          type='region',
                          name=temp_region,
                          flags='f',
                          quiet=True)
        # remove temp_output (if aoi_map given)
        grass.run_command("g.remove",
                          type='vector',
                          name=output_alkis_temp,
                          flags='f',
                          quiet=True)


def main():
    global rm_vec, path_to_tempdir, temp_region, output_alkis_temp, pid
    pid = os.getpid()
    """ parser options:
    """
    aoi_map = options['aoi_map']
    file_federal_state = options['filepath']
    load_region = flags['r']
    """ get federal state
    """
    if file_federal_state:
        with open(file_federal_state) as f:
            federal_states = f.read()
    else:
        federal_states = options['federal_state']
    """ get URL for corresponding federal state
    """
    URL = None
    fs = None
    for federal_state in federal_states.split(','):
        if federal_state in URLS:
            if federal_state in ['Nordrhein-Westfalen', 'Berlin']:
                URL = URLS[federal_state]
                fs = federal_state
            else:
                grass.warning(_(f"Support for {federal_state} is not yet implemented."))
        else:
            if options['filepath']:
                grass.fatal(_("Non valid name of federal state,"
                              " in 'filepath'-option given"))
            elif options['federal_state']:
                grass.fatal(_("Non valid name of federal state,"
                              " in 'federal_states'-option given"))
    # so far, just NRW implemented;
    # in case single federal state given, and not NRW:
    #   skips following part
    #   + grass.message: see above
    # in case multiple federal states given, and at least one of them is NRW:
    #   import data only for NRW area
    if not URL:
        grass.fatal(_("AOI is located in federal state(s),"
                      "which are not yet implemented."))
    if URL:
        """ load data:
        """
        # create tempdirectory for unzipping files
        path_to_tempdir = grass.tempdir()

        # file of interest in zip
        filename = filenames[fs]
        alkis_source = os.path.join(path_to_tempdir, filename)
        # final output map
        output_alkis_temp = f'output_alkis_temp_{pid}'
        output_alkis = options['output']
        """ download alkis building data
        """
        grass.message(_("Downloading ALKIS building data..."))
        response = requests.get(URL)
        if not response.status_code == 200:
            sys.exit(("v.alkis.buildings.import was stopped."
                      "The data are currently not available."))
        # unzip boundaries
        if federal_state == 'Nordrhein-Westfalen':
            zip_file = zipfile.ZipFile(BytesIO(response.content))
            zip_file.extractall(f"{path_to_tempdir}")
        elif federal_state == 'Berlin':
            zip_file = py7zr.SevenZipFile(BytesIO(response.content))
            zip_file.extractall(f"{path_to_tempdir}")
        """ import to GRASS DB
        """
        grass.message(_("Importing ALKIS building data..."))
        if aoi_map:
            # region
            temp_region = f'temp_region_{pid}'
            # save current region for setting back later in cleanup
            grass.run_command("g.region", save=temp_region, quiet=True)
            # set region to aoi_map
            grass.run_command("g.region", vector=aoi_map, quiet=True)
            grass.run_command("v.import",
                              input=alkis_source,
                              output=output_alkis_temp,
                              extent='region',
                              quiet=True)
            grass.run_command("v.clip",
                              input=output_alkis_temp,
                              clip=aoi_map,
                              output=output_alkis,
                              flags='d',
                              quiet=True)
        elif load_region:
            grass.run_command("v.import",
                              input=alkis_source,
                              output=output_alkis,
                              extent='region',
                              quiet=True)
        else:
            grass.run_command("v.import",
                              input=alkis_source,
                              output=output_alkis,
                              quiet=True)
    grass.message(_("Done importing ALKIS building data."))


if __name__ == "__main__":
    options, flags = grass.parser()
    atexit.register(cleanup)
    main()
