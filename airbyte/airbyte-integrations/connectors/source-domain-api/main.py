#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_domain_api import SourceDomainApi

if __name__ == "__main__":
    source = SourceDomainApi()
    launch(source, sys.argv[1:])
