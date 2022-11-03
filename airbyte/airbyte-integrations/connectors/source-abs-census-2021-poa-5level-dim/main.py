#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_abs_census_2021_poa_5level_dim import SourceAbsCensus_2021Poa_5levelDim

if __name__ == "__main__":
    source = SourceAbsCensus_2021Poa_5levelDim()
    launch(source, sys.argv[1:])
