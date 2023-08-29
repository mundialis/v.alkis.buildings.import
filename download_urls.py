#!/usr/bin/env python3

URLS = {
    "Brandenburg": None,
    "Berlin": "https://datenbox.stadt-berlin.de"
    "/ssf/s/readFile/share/14254/8799702476865788292/publicLink/SHP_BE_ALKIS.7z",
    "Baden-Württemberg": None,
    "Bayern": None,
    "Bremen": None,
    "Hessen": "https://gds.hessen.de/downloadcenter/DATE/Liegenschaftskataster/Hausumringe%20(shape)/Hausumringe%20Hessen.zip",
    "Hamburg": None,
    "Mecklenburg-Vorpommern": None,
    "Niedersachsen": None,
    "Nordrhein-Westfalen": "https://www.opengeodata.nrw.de/produkte/geobasis"
    "/lk/akt/hu_shp/hu_EPSG4647_Shape.zip",
    "Rheinland-Pfalz": None,
    "Schleswig-Holstein": None,
    "Saarland": None,
    "Sachsen": None,
    "Sachsen-Anhalt": None,
    "Thüringen": "https://geoportal.geoportal-th.de/hausko_umr/HU-TH.zip",
}

filenames = {
    "Brandenburg": None,
    "Berlin": "SHP_BE_ALKIS_Merged/Gebaeude_Bauteile_Flaechen.shp",
    "Baden-Württemberg": None,
    "Bayern": None,
    "Bremen": None,
    "Hessen": "gebaeude-he.shp",
    "Hamburg": None,
    "Mecklenburg-Vorpommern": None,
    "Niedersachsen": None,
    "Nordrhein-Westfalen": "hu_shp.shp",
    "Rheinland-Pfalz": None,
    "Schleswig-Holstein": None,
    "Saarland": None,
    "Sachsen": None,
    "Sachsen-Anhalt": None,
    "Thüringen": "gebaeude-th.shp"
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

bb_base_url = (
    "https://data.geobasis-bb.de/geobasis/daten/alkis/Vektordaten/shape/"
)
download_dict = {
    "Brandenburg": [
        f"{bb_base_url}ALKIS_Shape_BAR.zip",
        f"{bb_base_url}ALKIS_Shape_BRB.zip",
        f"{bb_base_url}ALKIS_Shape_CB.zip",
        f"{bb_base_url}ALKIS_Shape_EE.zip",
        f"{bb_base_url}ALKIS_Shape_FF.zip",
        f"{bb_base_url}ALKIS_Shape_HVL.zip",
        f"{bb_base_url}ALKIS_Shape_LDS.zip",
        f"{bb_base_url}ALKIS_Shape_LOS.zip",
        f"{bb_base_url}ALKIS_Shape_MOL.zip",
        f"{bb_base_url}ALKIS_Shape_OHV.zip",
        f"{bb_base_url}ALKIS_Shape_OPR.zip",
        f"{bb_base_url}ALKIS_Shape_OSL.zip",
        f"{bb_base_url}ALKIS_Shape_P.zip",
        f"{bb_base_url}ALKIS_Shape_PM.zip",
        f"{bb_base_url}ALKIS_Shape_PR.zip",
        f"{bb_base_url}ALKIS_Shape_SPN.zip",
        f"{bb_base_url}ALKIS_Shape_TF.zip",
        f"{bb_base_url}ALKIS_Shape_UM.zip",
    ],
}
