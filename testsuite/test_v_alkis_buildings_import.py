#!/usr/bin/env python3
#
############################################################################
#
# MODULE:      v.alkis.buildings.import test
# AUTHOR(S):   Lina Krisztian

# PURPOSE:     Tests v.alkis.buildings.import
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

import os

from grass.gunittest.case import TestCase
from grass.gunittest.main import test
from grass.gunittest.gmodules import SimpleModule
import grass.script as grass


class TestVGetAlkisBuildings(TestCase):
    pid = os.getpid()
    aoi_map_data = os.path.join('data', 'area_beuel.geojson')
    aoi_map = f'aoi_map_{pid}'
    test_output = f"test_output_{pid}"
    region_map_data = os.path.join('data', 'area_cologne.geojson')
    region_map = f"region_map_{pid}"
    aoi_map_multi_data = os.path.join('data',
                                      'area_nw_rp.geojson')
    aoi_map_multi = f'aoi_map_multi_{pid}'
    aoi_map_multi_c_data = os.path.join('data',
                                        'area_germany_netherlands.geojson')
    aoi_map_multi_c = f"aoi_map_multi_c_{pid}"

    @classmethod
    def setUpClass(self):
        """Ensures expected computational region and generated data"""
        # import vector map to set as region
        self.runModule("v.import",
                       input=self.region_map_data,
                       output=self.region_map)
        self.runModule("g.region",
                       vector=self.region_map)
        # import vector map to test aoi_map option
        self.runModule("v.import",
                       input=self.aoi_map_data,
                       output=self.aoi_map)
        # import vector map to test aoi_map located in multiple federal states
        self.runModule("v.import",
                       input=self.aoi_map_multi_data,
                       output=self.aoi_map_multi)
        # import vector map to test aoi_map located not only in Germany
        self.runModule("v.import",
                       input=self.aoi_map_multi_c_data,
                       output=self.aoi_map_multi_c)

    @classmethod
    def tearDownClass(self):
        """Remove the temporary region and generated data"""
        self.runModule("g.remove",
                       type="vector",
                       name=self.region_map,
                       flags="f")
        self.runModule("g.remove",
                       type="vector",
                       name=self.aoi_map,
                       flags="f")
        self.runModule("g.remove",
                       type="vector",
                       name=self.aoi_map_multi,
                       flags="f")
        self.runModule("g.remove",
                       type="vector",
                       name=self.aoi_map_multi_c,
                       flags="f")

    def tearDown(self):
        """Remove the outputs created
        This is executed after each test run.
        """
        self.runModule("g.remove",
                       type="vector",
                       name=self.test_output,
                       flags="f")

    def test_option_aoi_map(self):
        """ tests aoi_map as optional input
        """
        v_check = SimpleModule("v.alkis.buildings.import",
                               output=self.test_output,
                               federal_state='Nordrhein-Westfalen',
                               aoi_map=self.aoi_map)
        self.assertModule(v_check, 'Using aoi_map as optional input fails')
        # Data should have following columns:
        # cat, AGS, OI, GFK, AKTUALITAE
        atr_dict = grass.parse_command("v.info",
                                       map=self.test_output,
                                       flags='c')
        atr = list(atr_dict.keys())
        self.assertTrue('AGS' in atr[1],
                        "Module failed, because of missins key 'AGS'")

    def test_option_aoi_map_mutli_fs(self):
        """ tests aoi_map as optional input
            with aoi located in multiple federal states
        """
        v_check = SimpleModule("v.alkis.buildings.import",
                               output=self.test_output,
                               federal_state=['Nordrhein-Westfalen',
                                              'Rheinland-Pfalz'],
                               aoi_map=self.aoi_map_multi)
        self.assertModule(v_check, 'Using aoi_map, which is located in'
                                   'multiple federal states, fails')
        # Data should have following columns:
        # cat, AGS, OI, GFK, AKTUALITAE
        atr_dict = grass.parse_command("v.info",
                                       map=self.test_output,
                                       flags='c')
        atr = list(atr_dict.keys())
        self.assertTrue('AGS' in atr[1],
                        "Module failed, because of missins key 'AGS'")

    def test_option_aoi_map_mutli_country(self):
        """ tests aoi_map as optional input
            with aoi located only partly in Germany
        """
        v_check = SimpleModule("v.alkis.buildings.import",
                               output=self.test_output,
                               federal_state='Nordrhein-Westfalen',
                               aoi_map=self.aoi_map_multi_c)
        self.assertModule(v_check, 'Using aoi_map, which is located in'
                                   'in Germany and the Netherlands fails')
        # Data should have following columns:
        # cat, AGS, OI, GFK, AKTUALITAE
        atr_dict = grass.parse_command("v.info",
                                       map=self.test_output,
                                       flags='c')
        atr = list(atr_dict.keys())
        self.assertTrue('AGS' in atr[1],
                        "Module failed, because of missins key 'AGS'")

    def test_flag(self):
        """ tests -r flag
        """
        # set some region in setUp class (here some region in cologne):
        v_check = SimpleModule("v.alkis.buildings.import",
                               output=self.test_output,
                               federal_state='Nordrhein-Westfalen',
                               flags='r')
        self.assertModule(v_check, 'Using -r flag fails')
        # Data should have following columns:
        # cat, AGS, OI, GFK, AKTUALITAE
        atr_dict = grass.parse_command("v.info",
                                       map=self.test_output,
                                       flags='c')
        atr = list(atr_dict.keys())
        self.assertTrue('AGS' in atr[1],
                        "Module failed, because of missins key 'AGS'")

    def test_correct_region(self):
        """ tests if region is set back correctly
        """
        # set some region in setUp class (here some region in cologne):
        # get this info before running the addon
        region_exp = self.runModule("g.region",
                                    flags='g')
        # then run v.alkis.buildings.import with aoi_map (which changes region)
        self.runModule("v.alkis.buildings.import",
                       output=self.test_output,
                       federal_state='Nordrhein-Westfalen',
                       aoi_map=self.aoi_map)
        # region after running module
        region_is = self.runModule("g.region",
                                   flags='g')
        # check if current region is expected region
        self.assertTrue(region_exp == region_is,
                        "Region was modified within addon.")
        # Data should have following columns:
        # cat, AGS, OI, GFK, AKTUALITAE
        atr_dict = grass.parse_command("v.info",
                                       map=self.test_output,
                                       flags='c')
        atr = list(atr_dict.keys())
        self.assertTrue('AGS' in atr[1],
                        "Module failed, because of missins key 'AGS'")

    def test_file_input_single(self):
        """ tests file as input option for federal state(s) information
            single federal state - case
        """
        # single federal state in file
        v_check = SimpleModule("v.alkis.buildings.import",
                               output=self.test_output,
                               aoi_map=self.aoi_map,
                               file=os.path.join('data', 'singleFs'))
        self.assertModule(v_check,
                          "Module fails, when file-input"
                          "with single federal state given")
        # Data should have following columns:
        # cat, AGS, OI, GFK, AKTUALITAE
        atr_dict = grass.parse_command("v.info",
                                       map=self.test_output,
                                       flags='c')
        atr = list(atr_dict.keys())
        self.assertTrue('AGS' in atr[1],
                        "Module failed, because of missins key 'AGS'")

    def test_file_input_multi(self):
        """ tests file as input option for federal state(s) information
            multiple federal state - case
        """
        # multiple (two) federal states in file
        v_check = SimpleModule("v.alkis.buildings.import",
                               output=self.test_output,
                               aoi_map=self.aoi_map,
                               file=os.path.join('data', 'multiFs'))
        self.assertModule(v_check,
                          "Module fails, when file-input"
                          "with multiple federal states given")
        # Data should have following columns:
        # cat, AGS, OI, GFK, AKTUALITAE
        atr_dict = grass.parse_command("v.info",
                                       map=self.test_output,
                                       flags='c')
        atr = list(atr_dict.keys())
        self.assertTrue('AGS' in atr[1],
                        "Module failed, because of missins key 'AGS'")


if __name__ == "__main__":
    test()
