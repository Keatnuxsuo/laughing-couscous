#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
import datetime as dt

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator, NoAuth


# # Basic incremental stream
# class IncrementalDomainApiStream(DomainApiStream, ABC):
#     """
#     TODO fill in details of this class to implement functionality related to incremental syncs for your connector.
#          if you do not need to implement incremental sync for any streams, remove this class.
#     """

#     # TODO: Fill in to checkpoint stream reads after N records. This prevents re-reading of data if the stream fails for any reason.
#     state_checkpoint_interval = None

#     @property
#     def cursor_field(self) -> str:
#         """
#         TODO
#         Override to return the cursor field used by this stream e.g: an API entity might always use created_at as the cursor field. This is
#         usually id or date based. This field's presence tells the framework this in an incremental stream. Required for incremental.

#         :return str: The name of the cursor field.
#         """
#         return []

#     def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
#         """
#         Override to determine the latest state after reading the latest record. This typically compared the cursor_field from the latest record and
#         the current state and picks the 'most' recent cursor. This is how a stream's state is determined. Required for incremental.
#         """
#         return {}

# Source
class SourceDomainApi(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        """
        TODO: Implement a connection check to validate that the user-provided config can be used to connect to the underlying API

        See https://github.com/airbytehq/airbyte/blob/master/airbyte-integrations/connectors/source-stripe/source_stripe/source.py#L232
        for an example.

        :param config:  the user-input config object conforming to the connector's spec.yaml
        :param logger:  logger object
        :return Tuple[bool, any]: (True, None) if the input config can be used to connect to the API successfully, (False, error) otherwise.
        """
        accepted_cities = {"Sydney", "Melbourne", "Adelaide"}

        input_cities = [x.strip() for x in str(config['cities']).split(",")]

        for next_city in input_cities:
            if next_city not in accepted_cities:
                return False, f"Please enter a valid city ({accepted_cities}) - invalid city was {next_city}, list was {str(config['cities']).split(',')}"
        
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        TODO: Replace the streams below with your own streams.

        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """
        # TODO remove the authenticator if not required.

        # NOTE FROM OFFICE HOURS: Look into looping over a comma-separated config list here?
        # Re-jig all this to work off config

        auth = NoAuth()
        return [DomainSalesListingsStream(authenticator=auth, config=config)]

class DomainSalesListingsStream(HttpStream):
    url_base = 'https://api.domain.com.au/'
    primary_key = 'propertyDetailsUrl'

    def __init__(self, config: Mapping[str, Any], **kwargs):
        super().__init__()
        self.api_key = config['api_key']
        self.cities = config['cities']

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        # No pagination
        return None

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return_value = {
            "api_key": self.api_key
        }

        return return_value

    def _get_cities_list(
        self
        ) -> list:
        return_value = [{"city": next_city} for next_city in str(self.cities).split(',')]

        return return_value


    def stream_slices(
        self, 
        *, 
        sync_mode, 
        cursor_field: List[str] = None, 
        stream_state: Mapping[str, Any] = None
        ) -> Iterable[Optional[Mapping[str, Any]]]:
        return self._get_cities_list()

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        return f'v1/salesResults/{stream_slice["city"]}/listings'

    def parse_response(
        self,
        response: requests.Response,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> Iterable[Mapping]:
        original_response = response.json()

        # result = [dict(item, **{'elem':'value'}) for item in myList]

        # Added columns can be unpacked to a dictionary in this weird/wonderful way:
        # https://stackoverflow.com/questions/14071038/add-an-element-in-each-dictionary-of-a-list-list-comprehension
        added_cols = dict(
            city = stream_slice["city"],
            execution_time = dt.datetime.now()
        )
        
        return_value = [dict(next_dict, **added_cols) for next_dict in original_response]

        return return_value