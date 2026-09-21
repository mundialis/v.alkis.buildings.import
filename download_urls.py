#!/usr/bin/env python3
#
############################################################################
#
# MODULE:      download_urls
# AUTHOR(S):   Anika Weinmann, Julia Haas

# PURPOSE:     German Federal State download urls for ALKIS buildings
# COPYRIGHT:   (C) 2024 by mundialis GmbH & Co. KG and the GRASS
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

URLS = {
    "BW": None,
    "BY": None,
    "BE": "https://datenbox.stadt-berlin.de/ssf/s/readFile/share"
    "/14254/8799702476865788292/publicLink/SHP_BE_ALKIS.7z",
    "BB": None,
    "HB": None,
    "HH": None,
    "HE": "https://gds.hessen.de/downloadcenter/DATE/"
    "Liegenschaftskataster/Hausumringe%20(shape)/Hausumringe%20Hessen.zip",
    "MV": None,
    "NI": None,
    "NW": "https://www.opengeodata.nrw.de/produkte/geobasis"
    "/lk/akt/hu_shp/hu_EPSG4647_Shape.zip",
    "RP": None,
    "SL": None,
    "SN": "https://geocloud.landesvermessung.sachsen.de/index.php/s"
    "/YgBfai4gXoiExJx/download?path=%2F&files=hu_sn_shape.zip",
    "ST": None,
    "SH": None,
    "TH": "https://geoportal.geoportal-th.de/hausko_umr/HU-TH.zip",
}

BUILDINGS_FILENAMES = {
    "BW": None,
    "BY": None,
    "BE": "SHP_BE_ALKIS_Merged/Gebaeude_Bauteile_Flaechen.shp",
    "BB": None,
    "HB": None,
    "HH": None,
    "HE": "gebaeude-he.shp",
    "MV": None,
    "NI": None,
    "NW": "hu_shp.shp",
    "RP": None,
    "SL": None,
    "SN": "hu_sn_gebaeude_20240118.shp",
    "ST": None,
    "SH": None,
    "TH": "gebaeude-th.shp",
}

BB_districts = {
    "BAR": "Barnim",
    "BRB": "Brandenburg an der Havel",
    "CB": "Cottbus",
    "EE": "Elbe-Elster",
    "FF": "Frankfurt (Oder)",
    "HVL": "Havelland",
    "LDS": "Dahme-Spreewald",
    "LOS": "Oder-Spree",
    "MOL": "Märkisch-Oderland",
    "OHV": "Oberhavel",
    "OPR": "Ostprignitz-Ruppin",
    "OSL": "Oberspreewald-Lausitz",
    "P": "Potsdam",
    "PM": "Potsdam-Mittelmark",
    "PR": "Prignitz",
    "SPN": "Spree-Neiße",
    "TF": "Teltow-Fläming",
    "UM": "Uckermark",
}

BB_BASE_URL = (
    "https://data.geobasis-bb.de/geobasis/daten/alkis/Vektordaten/shape/"
)
download_dict = {
    "Brandenburg": [
        f"{BB_BASE_URL}alkis_shape_bar.zip",
        f"{BB_BASE_URL}alkis_shape_brb.zip",
        f"{BB_BASE_URL}alkis_shape_cb.zip",
        f"{BB_BASE_URL}alkis_shape_ee.zip",
        f"{BB_BASE_URL}alkis_shape_ff.zip",
        f"{BB_BASE_URL}alkis_shape_hvl.zip",
        f"{BB_BASE_URL}alkis_shape_lds.zip",
        f"{BB_BASE_URL}alkis_shape_los.zip",
        f"{BB_BASE_URL}alkis_shape_mol.zip",
        f"{BB_BASE_URL}alkis_shape_ohv.zip",
        f"{BB_BASE_URL}alkis_shape_opr.zip",
        f"{BB_BASE_URL}alkis_shape_osl.zip",
        f"{BB_BASE_URL}alkis_shape_p.zip",
        f"{BB_BASE_URL}alkis_shape_pm.zip",
        f"{BB_BASE_URL}alkis_shape_pr.zip",
        f"{BB_BASE_URL}alkis_shape_spn.zip",
        f"{BB_BASE_URL}alkis_shape_tf.zip",
        f"{BB_BASE_URL}alkis_shape_um.zip",
    ],
}
