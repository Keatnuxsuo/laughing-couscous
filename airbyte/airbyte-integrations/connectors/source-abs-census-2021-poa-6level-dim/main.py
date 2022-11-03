#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_abs_census_2021_poa_6level_dim import SourceAbsCensus_2021Poa_6levelDim

if __name__ == "__main__":
    source = SourceAbsCensus_2021Poa_6levelDim()
    launch(source, sys.argv[1:])
