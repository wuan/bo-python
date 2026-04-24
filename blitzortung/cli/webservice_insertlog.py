"""

Inserts the generated json reports from the service into a compact line-based file format

"""

import datetime
import glob
import json
import logging
import os
import sys
from datetime import datetime
from optparse import OptionParser
from typing import Any

import geoip2.database
import statsd
from geoip2.database import Reader

from blitzortung.convert import value_to_string

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.INFO)


def main():
    parser = OptionParser()

    parser.add_option("--debug", dest="debug", default=False, action="store_true", help="enable debug output")
    parser.add_option("--metrics", dest="metrics", default=False, action="store_true", help="produce metrics")
    parser.add_option("--base-dir", dest="base_dir", default='/var/log/blitzortung',
                      help="base directory for log files")
    parser.add_option("--geoip-db", dest="geoip_db", default='/var/lib/GeoIP/GeoLite2-City.mmdb',
                      help="GeoIP database path")

    (options, args) = parser.parse_args()

    reader = geoip2.database.Reader(options.geoip_db)
    statsd_client = statsd.StatsClient('localhost', 8125, prefix='org.blitzortung.service')

    if options.debug:
        logger.setLevel(logging.DEBUG)

    latest_time = None
    logger.debug("latest time %s" % latest_time)

    base_dir = options.base_dir

    json_file_names = glob.glob(os.path.join(base_dir, '*.json'))

    json_file_names.sort()

    for json_file_name in json_file_names:

        with open(json_file_name, 'r') as json_file:

            logger.debug(f"opened file {json_file_name}")

            data = json.load(json_file)

            global_timestamp = datetime.datetime.fromtimestamp(data['timestamp'] / 1000000)

            if 'get_strikes_grid' in data:

                results = []
                for entry in data['get_strikes_grid']:
                    remote_address = entry[6]

                    user_agent = entry[7]
                    version = user_agent_version(user_agent)

                    city, country_code = geoip_lookup(reader, remote_address)

                    remote_address = None

                    local_x = None
                    local_y = None
                    data_area = None

                    timestamp_microseconds = entry[0]
                    minute_length = entry[1]
                    grid_baselength = entry[2]
                    minute_offset = entry[3]
                    region = entry[4]
                    count_threshold = entry[5]
                    if len(entry) > 8:
                        local_x = entry[8]
                        local_y = entry[9]
                        data_area = entry[10]

                    results.append([
                        timestamp_microseconds / 1000000,
                        region,
                        grid_baselength,
                        minute_offset,
                        minute_length,
                        count_threshold,
                        remote_address if remote_address is not None else '-',
                        country_code if country_code is not None else '-',
                        city if city is not None else '-',
                        version,
                        local_x if local_x is not None else '-',
                        local_y if local_y is not None else '-',
                        data_area if data_area is not None else '-',
                    ])

                    if options.metrics:
                        tags = {
                            "version": version if version is not None else '-',
                            "region": region,
                            "minutes": minute_length,
                            "offset": minute_offset,
                            "grid": grid_baselength
                        }
                        if local_x and local_y and data_area:
                            tags["data_area"] = f"{local_x}x{local_y}-{data_area}"
                        if country_code:
                            tags["country"] = country_code

                        tag_values = ",".join([f"{key}={value}" for key, value in tags.items()])

                        statsd_client.incr(f'access,{tag_values}')

                write_results(global_timestamp, results, base_dir)

        os.unlink(json_file_name)


def geoip_lookup(reader: Reader, remote_address) -> tuple[Any, Any]:
    country_code = None
    city = None
    try:
        geo_info = reader.city(remote_address)
        city = geo_info.city.name
        country_code = geo_info.country.iso_code
    except (ValueError, geoip2.errors.AddressNotFoundError):
        pass
    return city, country_code


def user_agent_version(user_agent) -> int | None:
    version: int | None = None
    if user_agent:
        user_agent_parts = user_agent.split(' ')[0].rsplit('-', 1)
        version_prefix = user_agent_parts[0]
        if version_prefix == 'bo-android' and len(user_agent_parts) > 1:
            try:
                version = int(user_agent_parts[1])
            except ValueError:
                pass
    return version


def write_results(global_timestamp: datetime, results: list[Any], base_dir):
    with open(os.path.join(base_dir, "servicelog_" + global_timestamp.strftime("%Y-%m-%d")),
              'a+') as output_file:
        for result in results:
            line = "\t".join([value_to_string(value) for value in result])
            output_file.write(line + "\n")


if __name__ == "__main__":
    main()
