MODULE_TOPDIR = ../..

PGM = v.alkis.buildings.import

ETCFILES = download_urls federal_state_info

include $(MODULE_TOPDIR)/include/Make/Python.make
include $(MODULE_TOPDIR)/include/Make/Script.make

python-requirements:
	pip3 install -r requirements.txt

default: python-requirements script
