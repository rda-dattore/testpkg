# doi_manager

This tool mints (creates) a DOI for an NG-GDEX dataset and manages the metadata and registered URL associated with the DOI.

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install doi_manager.

From within your python environment:
1. Download the `settings.txt` from this repository and replace all <values> with real data
2. Run `pip install git+https://github.com/rda-dattore/testpkg#subdirectory=doi_manager`
3. Configure the settings by running `doi_manage <authorization_key> configure settings.txt`
   - whatever you enter for <authorization_key> will become the new authorization key for the tool

## Usage

Run `doi_manage` with no arguments to get usage information
