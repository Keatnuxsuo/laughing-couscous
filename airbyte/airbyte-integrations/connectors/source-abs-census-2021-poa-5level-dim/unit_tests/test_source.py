#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#

from unittest.mock import MagicMock

from source_abs_census_2021_poa_5level_dim.source import SourceAbsCensus_2021Poa_5levelDim


def test_check_connection(mocker):
    source = SourceAbsCensus_2021Poa_5levelDim()
    logger_mock, config_mock = MagicMock(), MagicMock()
    assert source.check_connection(logger_mock, config_mock) == (True, None)


def test_streams(mocker):
    source = SourceAbsCensus_2021Poa_5levelDim()
    config_mock = MagicMock()
    streams = source.streams(config_mock)
    # TODO: replace this with your streams number
    expected_streams_number = 2
    assert len(streams) == expected_streams_number
