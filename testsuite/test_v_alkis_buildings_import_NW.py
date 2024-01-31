#!/usr/bin/env python3
#
############################################################################
#
# MODULE:      v.alkis.buildings.import test for NW
# AUTHOR(S):   Lina Krisztian, Anika Weinmann
# PURPOSE:     Tests v.alkis.buildings.import for NW
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

from grass.gunittest.main import test

from v_alkis_buildings_import_base import VAlkisBuildingsImportTestFsBase


class VAlkisBuildingsImportTestNW(VAlkisBuildingsImportTestFsBase):
    fs = "NW"
    federal_state = "Nordrhein-Westfalen"

    def test_option_aoi_map(self):
        """Tests aoi_map as optional input"""
        self.option_aoi_map()

    def test_flag(self):
        """Tests region as AOI input"""
        self.flag()

    def test_file_input_single(self):
        """Tests aoi_map as optional input and federal state input file"""
        self.file_input_single()

if __name__ == "__main__":
    test()
