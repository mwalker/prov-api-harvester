# PROV API Harvester

## Description

PROV API Harvester is a  collection of python scripts designed to harvest metadata records from the [Public Record Office Victoria (PROV) API](https://prov.vic.gov.au/prov-collection-api).

## Requirements

- Python 3
- requests
- xattr
- zstandard

uv makes it easier.

## Usage

The easiest way to run the scripts from the command line is using uv:

```
uv run prov-api-harvest.py <options>
```

## Licence

This project is open source and is licensed under the Apache License, Version 2.0, ([LICENSE](LICENSE) or
https://www.apache.org/licenses/LICENSE-2.0).

The data is licenced by PROV for non-commercial purposes under a (CC-BY-NC licence)[https://creativecommons.org/licenses/by-nc/4.0/]


## Feed

[Atom](https://github.com/mwalker/prov-api-harvester/commits/main.atom)
