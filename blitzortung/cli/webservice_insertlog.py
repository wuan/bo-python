"""

Inserts the generated json reports from the service into a compact line-based file format

"""

from __future__ import print_function

import datetime
import glob
import json
import logging
import os
import sys
from optparse import OptionParser

from blitzortung.convert import value_to_string

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.INFO)

import geoip2.database


def main():
    reader = geoip2.database.Reader('/var/lib/GeoIP/GeoLite2-City.mmdb')

    parser = OptionParser()

    parser.add_option("--debug", dest="debug", default=False, action="store_true", help="enable debug output")

    (options, args) = parser.parse_args()

    if options.debug:
        logger.setLevel(logging.DEBUG)

    latest_time = None
    logger.debug("latest time %s" % latest_time)

    base_dir = '/var/log/blitzortung'

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

                    country_code = None
                    city = None
                    local_x = None
                    local_y = None
                    data_area = None

                    user_agent = entry[7]
                    version = None
                    if user_agent:
                        user_agent_parts = user_agent.split(' ')[0].rsplit('-', 1)
                        version_prefix = user_agent_parts[0]
                        version_string = user_agent_parts[1] if len(user_agent_parts) > 1 else None
                        if version_prefix == 'bo-android':
                            version = int(version_string)

                    try:
                        geo_info = reader.city(remote_address)
                        city = geo_info.city.name
                        country_code = geo_info.country.iso_code
                    except (ValueError, geoip2.errors.AddressNotFoundError):
                        pass

                    remote_address = None

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

                with open(os.path.join(base_dir, "servicelog_" + global_timestamp.strftime("%Y-%m-%d")), 'a+') as output_file:
                    for result in results:
                        line = "\t".join([value_to_string(value) for value in result])
                        output_file.write(line + "\n")

        os.unlink(json_file_name)

if __name__ == "__main__":
    main()
