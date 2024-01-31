#!/usr/bin/env python3
#
############################################################################
#
# MODULE:      v.alkis.buildings.import test base
# AUTHOR(S):   Lina Krisztian, Anika Weinmann
# PURPOSE:     Testbase v.alkis.buildings.import
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

import os

from grass.gunittest.case import TestCase
from grass.gunittest.main import test
from grass.gunittest.gmodules import SimpleModule
import grass.script as grass

from v_alkis_buildings_import_base import VAlkisBuildingsImportTestBase


class VAlkisBuildingsImportTestMulitpleFS(VAlkisBuildingsImportTestBase):
    """Test base for the test functionality of v.alkis.buildings.import with
    multiple federal states"""

    aoi_map_multi_data = os.path.join("data", "area_nw_he.geojson")
    aoi_map_multi_county_data = os.path.join(
        "data", "area_germany_netherlands.geojson"
    )

    def test_option_aoi_map_multi_fs(self):
        """Tests aoi_map as optional input
        with aoi located in multiple federal states (NW, HE)
        """
        print(
            "Running tests with AOI in multiple federal states (NW and HE)..."
        )
        self.runModule(
            "v.import",
            input=self.aoi_map_multi_data,
            output=self.aoi_map,
            overwrite=True,
        )
        v_check = SimpleModule(
            "v.alkis.buildings.import",
            output=self.test_output,
            federal_state=["Nordrhein-Westfalen", "Hessen"],
            aoi_map=self.aoi_map,
        )
        self.assertModule(
            v_check,
            "Using aoi_map, which is located in"
            "multiple federal states, fails",
        )
        # Data should have following columns:
        # cat, AGS, OI, GFK
        atr_dict = grass.parse_command(
            "v.info", map=self.test_output, flags="c"
        )
        atr = list(atr_dict.keys())
        self.assertTrue(
            "AGS" in atr[1], "Module failed, because of missins key 'AGS'"
        )
        print(
            "Running tests with AOI in multiple federal states (NW and HE) "
            "done."
        )

    def test_option_aoi_map_multi_country(self):
        """Tests aoi_map as optional input
        with aoi located only partly in Germany
        """
        print("Running tests with AOI only partly in Germany...")
        self.runModule(
            "v.import",
            input=self.aoi_map_multi_county_data,
            output=self.aoi_map,
            overwrite=True,
        )
        v_check = SimpleModule(
            "v.alkis.buildings.import",
            output=self.test_output,
            federal_state="Nordrhein-Westfalen",
            aoi_map=self.aoi_map,
        )
        self.assertModule(
            v_check,
            "Using aoi_map, which is located in"
            "in Germany and the Netherlands fails",
        )
        # Data should have following columns:
        # cat, AGS, OI, GFK
        atr_dict = grass.parse_command(
            "v.info", map=self.test_output, flags="c"
        )
        atr = list(atr_dict.keys())
        self.assertTrue(
            "AGS" in atr[1], "Module failed, because of missins key 'AGS'"
        )
        print("Running tests with AOI only partly in Germany done.")

    def test_file_input_multi(self):
        """tests file as input option for federal state(s) information
        multiple federal state - case
        """
        print(
            "Running tests with AOI only partly in Germany and federal "
            "state input file..."
        )
        self.runModule(
            "v.import",
            input=self.aoi_map_multi_data,
            output=self.aoi_map,
            overwrite=True,
        )
        # multiple (two) federal states in file
        v_check = SimpleModule(
            "v.alkis.buildings.import",
            output=self.test_output,
            aoi_map=self.aoi_map,
            file=os.path.join("data", "multiFs.txt"),
        )
        self.assertModule(
            v_check,
            "Module fails, when file-input"
            "with multiple federal states given",
        )
        # Data should have following columns:
        # cat, AGS, OI, GFK
        atr_dict = grass.parse_command(
            "v.info", map=self.test_output, flags="c"
        )
        atr = list(atr_dict.keys())
        self.assertTrue(
            "AGS" in atr[1], "Module failed, because of missins key 'AGS'"
        )
        print(
            "Running tests with AOI only partly in Germany and federal "
            "state input file done."
        )

if __name__ == "__main__":
    test()
