from HARVEST_TRCA import HARVEST_TRCA
from pymongo import MongoClient
from typing import Union
from dateutil import tz
import datetime
import json
import sys
import os

# ## INFO ######################################################################################

# Objective: get a "trca_series_id" -> "dw_series_id" dictionary

# ## CONS ######################################################################################

MONGODB_CREDENTIALS_FILE_PATH = os.path.join("credentials", "amg", "credentials_mongodb.json")

MAPPING_FILE_PATH = os.path.join("config", "variables_mapping.json")
TRCA_TIMESERIES_DESCRIPTION_FILE_PATH = os.path.join("config", "timeseries_descriptions.json")

COLLECTION_NAME_TIMESERIES = "crud_timeseries"
COLLECTION_NAME_TS_FILTER = "crud_timeseries_filter_set"
FIELD_TS_ID = "timeseries_id"
FIELD_FT_ID = "filter_id"
FILTER_ID_REALTIME = "realtime"  # TODO: change to 'realtime' one day

TZ = "EST"
TIME_TOLERANCE_HOURS = 24

NONE_VALUE = -999

# ## DEFS ######################################################################################

'''
Gets: {}
'''
def get_trca_dict(raw_ts_descs: dict, map_dict: dict) -> dict:
    FIELD_TIME = "CorrectedEndTime"

    ret_dict = {}
    time_dict = {}

    # get fields
    field_loc = map_dict["fields"]["location"]["remote"]
    field_par = map_dict["fields"]["parameter"]["remote"]
    field_ts_id = map_dict["fields"]["timeseries_id"]["remote"]

    print("Processing %d ts descriptions." % len(raw_ts_descs["TimeSeriesDescriptions"]))
    added, no_end_time, update_times, ignore_times = 0, 0, 0, 0
    for cur_i, cur_trca_ts in enumerate(raw_ts_descs["TimeSeriesDescriptions"]):
        cur_trca_par, cur_trca_loc = cur_trca_ts[field_par], cur_trca_ts[field_loc]
        cur_trca_id = cur_trca_ts[field_ts_id]

        # open space in the return dictionary if needed
        ret_dict[cur_trca_loc] = ret_dict[cur_trca_loc] if cur_trca_loc in ret_dict else {}
        time_dict[cur_trca_loc] = time_dict[cur_trca_loc] if cur_trca_loc in time_dict else {}

        # check
        if FIELD_TIME not in cur_trca_ts:
            no_end_time += 1
            continue

        # add, update or ignore new timeseries
        if cur_trca_par not in ret_dict[cur_trca_loc]:
            ret_dict[cur_trca_loc][cur_trca_par] = cur_trca_id
            time_dict[cur_trca_loc][cur_trca_par] = cur_trca_ts[FIELD_TIME]
            added += 1
        else:
            if cur_trca_ts[FIELD_TIME] > time_dict[cur_trca_loc][cur_trca_par]:
                time_dict[cur_trca_loc][cur_trca_par] = cur_trca_ts[FIELD_TIME]
                ret_dict[cur_trca_loc][cur_trca_par] = cur_trca_id
                update_times += 1
            else:
                ignore_times += 1

        del cur_i, cur_trca_ts, cur_trca_par, cur_trca_loc, cur_trca_id

    # debug
    print(" Processed %d TRCA timeseries. Added %d. Skipped %d. Updated %d times. Ignored: %d." % (
        len(raw_ts_descs["TimeSeriesDescriptions"]), added, no_end_time, update_times, ignore_times))

    return ret_dict


def get_drwb_dict(credentials: dict, map_dict: dict) -> dict:
    ret_dict = {}

    # build connection URL
    credentials_url = "%s%s:%s@%s" % (credentials["prtc"], credentials["user"], 
        credentials["pass"], credentials["addr"])

    # get fields
    field_loc = map_dict["fields"]["location"]["dw"]
    field_par = map_dict["fields"]["parameter"]["dw"]
    field_ts_id = map_dict["fields"]["timeseries_id"]["dw"]

    # open connection and find collection
    mongodb_client = MongoClient(credentials_url)[credentials["dbnm"]]

    # get all timeseries in the realtime filter
    mongodb_collection_tsft = mongodb_client[COLLECTION_NAME_TS_FILTER].find()
    tss = set()
    for cur_i, cur_item in enumerate(mongodb_collection_tsft):
        # tss.add(cur_item[FIELD_TS_ID]) if cur_item[FIELD_FT_ID] == FILTER_ID_REALTIME else None
        if cur_item[FIELD_FT_ID].split(".")[0] == FILTER_ID_REALTIME:
            tss.add(cur_item[FIELD_TS_ID])
        del cur_i, cur_item
    
    # iterate element by element in the timeseries
    added = 0
    print("Processing MongoDB records...")
    mongodb_collection_ts = mongodb_client[COLLECTION_NAME_TIMESERIES].find()
    for cur_i, cur_item in enumerate(mongodb_collection_ts):
        cur_id, cur_loc = cur_item[field_ts_id], cur_item[field_loc]
        cur_par = cur_item[field_par]

        # timeseries must be in the realtime filter
        if cur_id not in tss:
            continue
        
        # 
        ret_dict[cur_loc] = ret_dict[cur_loc] if cur_loc in ret_dict else {}
        if cur_par not in ret_dict[cur_loc]:
            ret_dict[cur_loc][cur_par] = cur_id
            added += 1

        del cur_i, cur_item, cur_id, cur_loc, cur_par
     
    print(" Processed MongoDB timeseries. Added %d " % added)

    return ret_dict


def get_latest_timeseries_id(mongodb_connection) -> int:
    COLLECTION = 'crud_timeseries'
    FIELD = 'id'

    all_found = mongodb_connection[COLLECTION].find_one(sort=[(FIELD, -1)])
    return None if all_found is None else all_found[FIELD]


def open_mongodb_connection(credentials: dict) -> tuple:
    """
    Returns MongoDB client and connections with DB
    """

    # build connection URL
    credentials_url = "%s%s:%s@%s" % (credentials["prtc"], credentials["user"], 
        credentials["pass"], credentials["addr"])

    # open connection and find collection
    mongodb_client = MongoClient(credentials_url, w=1)

    return mongodb_client, mongodb_client[credentials["dbnm"]]


"""
Gets a dictionary with {"TRCA_timeseries_id": "dw_timeseries_id"}
"""
def merge_dicts(trca_dict: dict, drwb_dict: dict, vars_map: dict) -> dict:

    ret_dict = {}
    for cur_trca_loc in trca_dict.keys():

        # match locations
        if cur_trca_loc not in vars_map["locations"]:
            del cur_trca_loc
            continue
        cur_drwb_loc = vars_map["locations"][cur_trca_loc]

        # check
        if cur_drwb_loc not in drwb_dict:
            del cur_trca_loc, cur_drwb_loc
            continue

        for cur_trca_par, cur_trca_ts_id in trca_dict[cur_trca_loc].items():
            
            # match parameters
            if cur_trca_par not in vars_map["parameters"]:
                del cur_trca_par, cur_trca_ts_id
                continue
            cur_drwb_par = vars_map["parameters"][cur_trca_par]

            # try to get drwb timeseries ID
            if cur_drwb_par in drwb_dict[cur_drwb_loc]:
                ret_dict[cur_trca_ts_id] = drwb_dict[cur_drwb_loc][cur_drwb_par]

            del cur_trca_par, cur_trca_ts_id, cur_drwb_par
        del cur_trca_loc
    
    return ret_dict



"""
Should be used only once
"""
def create_empty_timeseries(mapping: dict, credentials: dict, filter_id: Union[str, None] = None):
    MODULE_INSTANCE_ID = "trca_realtime"
    COLLECTION_TS = 'crud_timeseries'
    COLLECTION_TS_FT = "crud_timeseries_filter_set"

    mongodb_client, mongodb_client_db = open_mongodb_connection(credentials)

    # get biggest ID
    latest_id = get_latest_timeseries_id(mongodb_client_db) + 1

    UNITS = {
        "Qo": "m^3/s",
        "Ho": "m",
        "Po": "mm"
    }

    # create all empty timeseries
    all_timeseries = []
    for cur_location_id in mapping["locations"].values():
        if cur_location_id.startswith("_"):
            continue

        for cur_parameter_id in mapping["parameters"].values():
            if cur_parameter_id.startswith("_"):
                continue

            all_timeseries.append({
                "id": latest_id,
                "header_units": UNITS[cur_parameter_id],
                "header_missVal": -999,
                "header_type": "instantaneous",
                "header_moduleInstanceId": MODULE_INSTANCE_ID,
                "header_parameterId_id": cur_parameter_id,
                "header_stationName": None,
                "header_location_id": cur_location_id,
                "header_timeStep_unit": "nonequidistant",
                "events": []
            })
            latest_id += 1

            del cur_parameter_id
        del cur_location_id

    insert_result = mongodb_client_db[COLLECTION_TS].insert_many(all_timeseries)
    print("Inserted %d." % len(all_timeseries))

    # if a filter id is given, add the new timeseries to this new Filter Id
    if filter_id is not None:
        all_relationships = []
        for cur_ts in all_timeseries:
            all_relationships.append({
                "timeseries_id": cur_ts["id"],
                "filter_id": filter_id
            })
            del cur_ts
        insert_result = mongodb_client_db[COLLECTION_TS_FT].insert_many(all_relationships)
        del all_relationships


def build_insert_args(timestamp_id, timestamps: list, values: list, map_dict: dict) -> tuple:
    
    field_ts_id = map_dict["fields"]["timeseries_id"]["dw"]
    
    # preprocess timeseries
    all_events = []
    for cur_ts_str, cur_value in zip(timestamps, values):
        cur_ts_str = "%s%s" % (cur_ts_str[0:-3], cur_ts_str[-2:])
        cur_datetime = datetime.datetime.strptime(cur_ts_str, "%Y-%m-%dT%H:%M:%S.%f0%z")
        cur_date = cur_datetime.strftime("%Y-%m-%d")
        cur_time = cur_datetime.strftime("%H:%M:%S")
        all_events.append({
            "date": cur_date,
            "time": cur_time,
            "value": cur_value if cur_value is not None else NONE_VALUE,
            "flag": 0
        })
        del cur_ts_str, cur_value, cur_datetime, cur_date, cur_time
    
    # build and return objects
    ret_query = { field_ts_id: timestamp_id }
    ret_values = { "$set": { "events": all_events } }
    return (ret_query, ret_values)


# ## MAIN ######################################################################################

if __name__ == "__main__":

    # get variables mapping
    with open(MAPPING_FILE_PATH) as _r_file:
        _config_file_mapping = json.load(_r_file)

    # get TRCA timeseries description
    with open(TRCA_TIMESERIES_DESCRIPTION_FILE_PATH) as _r_file:
        _config_file_trca_tss = json.load(_r_file)

    # get MongoDB credentials
    with open(MONGODB_CREDENTIALS_FILE_PATH) as _r_file:
        _mongodb_credentials = json.load(_r_file)

    # define time limits
    _date_now = datetime.datetime.now(tz=tz.gettz(TZ))
    _date_prv = _date_now - datetime.timedelta(hours=TIME_TOLERANCE_HOURS)

    # extract dictionary in the form of {"locationId": {"parameterId": timeseriesId}} - DW
    _ts_dict_drwb = get_drwb_dict(_mongodb_credentials, _config_file_mapping)
    # print(_ts_dict_drwb)

    # extract dictionary in the form of {"locationId": {"parameterId": timeseriesId}} - TRCA
    _ts_dict_trca = get_trca_dict(_config_file_trca_tss, _config_file_mapping)
    
    '''
    # ONE TIME: create a bunch of empty timeseries
    create_empty_timeseries(_config_file_mapping, _mongodb_credentials,
        filter_id="realtime.multi04catchments")
    '''

    ''''''
    # get updated timeseries
    _updated_timeseries_ids = HARVEST_TRCA.list_updated_timeseries(_date_prv)
    if _updated_timeseries_ids is None:
        print("Aborting. Unable to retrieve timeseries descriptions.")
        sys.exit()
    print("Found %d updated timeseries." % len(_updated_timeseries_ids))
    ''''''

    # map from TRCA to DrainWeb
    _trca_to_dw = merge_dicts(_ts_dict_trca, _ts_dict_drwb, _config_file_mapping)
    
    ''''''
    # filter timeseries listing
    _old_count = len(_trca_to_dw)
    _trca_to_dw = dict([(_tid, _did) 
                        for (_tid, _did) in _trca_to_dw.items()
                        if _tid in _updated_timeseries_ids])
    _new_count = len(_trca_to_dw)
    print("Reduced from %d to %d time series considered." % (_old_count, _new_count))

    # 
    _mongodb_client, _mongodb_client_db = open_mongodb_connection(_mongodb_credentials)

    # download timeseries from from TRCA's API
    _cur_last = 10
    _trca_ids = list(_trca_to_dw.keys())
    while _cur_last < len(list(_trca_to_dw.keys())) + 10:
        if _cur_last > len(list(_trca_to_dw.keys())):
            _cur_last = len(list(_trca_to_dw.keys()))
        print("FROM", _cur_last-10, "to", _cur_last)
        _cur_subset = _trca_ids[_cur_last-10:_cur_last]
        _rt_data = HARVEST_TRCA.get_realtime_data(_cur_subset, _date_prv, _date_now)
        _rt_data = HARVEST_TRCA.extract_timeseries(_rt_data)
        if _rt_data is None:
            print(" Ignored. No timeseries among keys:", list(_rt_data.keys()))
            continue

        # update records
        for _cur_trca_timeseries_id, _cur_timeseries_values in _rt_data['timeseries'].items():
            _cur_dw_timeseries_id = _trca_to_dw[_cur_trca_timeseries_id]
            _cur_query, _cur_values = build_insert_args(_cur_dw_timeseries_id,
                _rt_data['timestamps'], _cur_timeseries_values, _config_file_mapping)
            _mongodb_client_db[COLLECTION_NAME_TIMESERIES].update_one(_cur_query, _cur_values)
            print(" Updated by query:", _cur_query)
            del _cur_trca_timeseries_id, _cur_dw_timeseries_id, _cur_timeseries_values
            del _cur_query, _cur_values

        # give next step
        _cur_last += 10
        del _cur_subset, _rt_data
    ''''''

    # print("Downloaded:", _rt_data['ResponseStatus']['Message'])
    # print("Downloaded:", _rt_data['TimeSeries'][0])
    # print("Downloaded:", len(_rt_data['Points']))
    # print("Downloaded:", _rt_data.keys())
    # print(_rt_data['TimeRange'])

    print("Done.")
