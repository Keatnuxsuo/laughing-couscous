
def get_abs_census_poa_5level_dim(
        metric_name: str,
        endpoint: str
) -> str:
    import requests
    import json

    headers = {
        'Accept': 'application/vnd.sdmx.data+json'
    }
    params = '?startPeriod=2021&dimensionAtObservation=AllDimensions'
    url = endpoint+params
    r = requests.get(url, headers=headers)
    r = r.json()

    # data = json.loads(r.text)['data']
    #
    # # These are the values to navigate the response dictionary to get required fields
    # resp_config = (1, 3)
    #
    # # The key is the index of each dimension for the associated observation (the values)
    # # Format for 5-level dimensions is '0.0.0.0.0'
    # obs = data['dataSets'][0]['observations'].keys()
    # vals = {idx: i[0] for idx, i in enumerate(data['dataSets'][0]['observations'].values())}
    # pcodes = {idx: i['name'] for idx, i in enumerate(data['structure']['dimensions']['observation'][resp_config[0]]['values'])}
    # states = {idx: i['name'] for idx, i in enumerate(data['structure']['dimensions']['observation'][resp_config[1]]['values'])}
    #
    # out = []
    # for o in obs:
    #     _vals = o.split(':')
    #     _idx = int(_vals[resp_config[0]])
    #     _state = int(_vals[resp_config[1]])
    #     out.append(
    #         {
    #             pcodes[_idx]: {
    #                 metric_name: vals[_idx],
    #                 'state': states[_state]
    #             }
    #         }
    #     )
    return json.dumps(r)


def get_abs_census_poa_6level_dim(
        metric_name: str,
        endpoint: str
) -> str:
    import requests
    import json

    headers = {
        'Accept': 'application/vnd.sdmx.data+json'
    }
    # Hardcoded params to get data for 2021 census period, and include all dimensions available
    params = '?startPeriod=2021&dimensionAtObservation=AllDimensions'
    url = endpoint+params
    r = requests.get(url, headers=headers)

    data = json.loads(r.text)['data']

    # These are the values to navigate the response dictionary to get required fields
    resp_config = (2, 4)

    # The key is the index of each dimension for the associated observation (the values)
    # Format for 6-level dimensions is '0.0.0.0.0.0'
    obs = data['dataSets'][0]['observations'].keys()
    vals = {idx: i[0] for idx, i in enumerate(data['dataSets'][0]['observations'].values())}
    pcodes = {idx: i['name'] for idx, i in enumerate(data['structure']['dimensions']['observation'][resp_config[0]]['values'])}
    states = {idx: i['name'] for idx, i in enumerate(data['structure']['dimensions']['observation'][resp_config[1]]['values'])}

    out = []
    for o in obs:
        _vals = o.split(':')
        _idx = int(_vals[resp_config[0]])
        _state = int(_vals[resp_config[1]])
        out.append(
            {
                pcodes[_idx]: {
                    metric_name: vals[_idx],
                    'state': states[_state]
                }
            }
        )
    return json.dumps(out)


r = get_abs_census_poa_5level_dim(
    'med_age_persons',
    'https://api.data.abs.gov.au/data/ABS,C21_G02_POA,1.0.0/1.0800..'
)
# print(r)
with open('med-age.json', 'w') as f:
    f.write(r)


# r = get_abs_census_poa_5level_dim(
#     'med_ttl_fam_income_weekly',
#     'https://api.data.abs.gov.au/data/ABS,C21_G02_POA,1.0.0/3'
# )
# print(r)


# r = get_abs_census_poa_5level_dim(
#     'med_mortgage_repymt_mthly',
#     'https://api.data.abs.gov.au/data/ABS,C21_G02_POA,1.0.0/5'
# )
# print(r)


# r = get_abs_census_poa_5level_dim(
#     'med_rent_weekly',
#     'https://api.data.abs.gov.au/data/ABS,C21_G02_POA,1.0.0/6'
# )
# print(r)


# r = get_abs_census_poa_6level_dim(
#     'count_households',
#     'https://api.data.abs.gov.au/data/ABS,C21_G35_POA,1.0.0/_T._T'
# )
# print(r)