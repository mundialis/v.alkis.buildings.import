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

from grass_gis_helpers.location import (
    create_tmp_location,
    get_current_location,
)
from grass_gis_helpers.cleanup import cleaning_tmp_location


class VAlkisBuildingsImportTestBase(TestCase):
    """Test base for the test functionality of v.alkis.buildings.import"""

    pid = os.getpid()
    test_output = f"test_output_{pid}"
    orig_region = f"orig_region_{pid}"
    aoi_map = f"aoi_map_{pid}"

    GISDBASE = None
    TGTGISRC = None
    TMPLOC = None
    SRCGISRC = None

    @classmethod
    # pylint: disable=invalid-name
    def setUpClass(cls):
        # switch location
        _, _, cls.GISDBASE, cls.TGTGISRC = get_current_location()
        cls.TMPLOC, cls.SRCGISRC = create_tmp_location(epsg=25832)
        # save region
        grass.run_command("g.region", save=cls.orig_region)

    @classmethod
    # pylint: disable=invalid-name
    def tearDownClass(cls):
        # remove AOI
        if grass.find_file(name=cls.aoi_map, element="vector")["file"]:
            cls.runModule(
                "g.remove", type="vector", name=cls.aoi_map, flags="f"
            )
        # cleanup temporary location
        cleaning_tmp_location(
            cls.TGTGISRC, cls.TMPLOC, cls.SRCGISRC, cls.SRCGISRC
        )

    # pylint: disable=invalid-name
    def tearDown(self):
        """Remove the outputs created
        This is executed after each test run.
        """
        if grass.find_file(name=self.test_output, element="vector")["file"]:
            self.runModule(
                "g.remove", type="vector", name=self.test_output, flags="f"
            )


class VAlkisBuildingsImportTestFsBase(VAlkisBuildingsImportTestBase):
    """Test base for the test functionality of v.alkis.buildings.import
    testing one federal state example"""

    fs = ""
    federal_state = ""


    @classmethod
    # pylint: disable=invalid-name
    def setUpClass(cls):
        # switch location and save region
        super().setUpClass()
        # import vector map to set as region
        cls.runModule(
            "v.import",
            input=os.path.join("data", f"test_aoi_{cls.fs}.geojson"),
            output=cls.aoi_map
        )
        # set region
        grass.run_command("g.region", vector=cls.aoi_map, flags="a")
        grass.run_command("g.region", n="n+200", s="n-100", w="e-100")

    def option_aoi_map(self):
        """Tests aoi_map as optional input"""
        print(f"Running test for {self.fs} AOI...")
        # import pdb; pdb.set_trace()
        # get this info before running the addon
        region_exp = self.runModule("g.region", flags="g")
        # run module
        v_check = SimpleModule(
            "v.alkis.buildings.import",
            output=self.test_output,
            federal_state=self.federal_state,
            aoi_map=self.aoi_map,
            # dldir=
        )
        self.assertModule(v_check, "Using aoi_map as optional input fails")
        # Data should have following columns:
        # cat, AGS, OI, GFK, AKTUALITAE
        atr_dict = grass.parse_command(
            "v.info", map=self.test_output, flags="c"
        )
        atr = list(atr_dict.keys())
        self.assertTrue(
            "AGS" in atr[1], "Module failed, because of missins key 'AGS'"
        )
        # check if current region is expected region
        region_is = self.runModule("g.region", flags="g")
        self.assertTrue(
            region_exp == region_is, "Region was modified within addon."
        )
        print(f"Running test for {self.fs} AOI done.")

    def flag(self):
        """Tests -r flag"""
        print(f"Running test for {self.fs} region flag...")
        # set some region in setUp class (here some region in cologne):
        v_check = SimpleModule(
            "v.alkis.buildings.import",
            output=self.test_output,
            federal_state="Nordrhein-Westfalen",
            flags="r",
        )
        self.assertModule(v_check, "Using -r flag fails")
        # Data should have following columns:
        # cat, AGS, OI, GFK, AKTUALITAE
        atr_dict = grass.parse_command(
            "v.info", map=self.test_output, flags="c"
        )
        atr = list(atr_dict.keys())
        self.assertTrue(
            "AGS" in atr[1], "Module failed, because of missins key 'AGS'"
        )
        # check extend of output
        out_data_reg =  grass.parse_command("v.info", map=self.test_output, flags="g")
        g_reg = grass.region()
        self.assertTrue(
            (
                float(out_data_reg["north"]) >= g_reg["n"] and
                float(out_data_reg["south"]) <= g_reg["s"] and
                float(out_data_reg["east"]) >= g_reg["e"] and
                float(out_data_reg["west"]) <= g_reg["w"]

            ),
            "Output data extend is wrong.",
        )

        print(f"Running test for {self.fs} region flag done.")

    def file_input_single(self):
        """Tests file as input option for federal state(s) information
        single federal state - case
        """
        print(f"Running test for {self.fs} AOI and federal state file input...")
        # single federal state in file
        v_check = SimpleModule(
            "v.alkis.buildings.import",
            output=self.test_output,
            aoi_map=self.aoi_map,
            file=os.path.join("data", f"fs_file_{self.fs}.txt"),
        )
        self.assertModule(
            v_check,
            "Module fails, when file-input with single federal state given",
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
        print(f"Running test for {self.fs} AOI and federal state file input done.")