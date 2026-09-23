"""

Inserts the generated json reports from the service into a compact line-based file format

"""

import glob
import json
import logging
import os
import sys
from datetime import datetime, timezone
from json import JSONDecodeError
from optparse import OptionParser
from typing import Any

import geoip2.database
import geoip2.errors
import statsd
from geoip2.database import Reader

from blitzortung.convert import value_to_string

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.INFO)


def parse_options():
    """Parse the command line options for the insertlog tool."""
    parser = OptionParser()

    parser.add_option("--debug", dest="debug", default=False, action="store_true", help="enable debug output")
    parser.add_option("--metrics", dest="metrics", default=False, action="store_true", help="produce metrics")
    parser.add_option("--base-dir", dest="base_dir", default='/var/log/blitzortung',
                      help="base directory for log files")
    parser.add_option("--geoip-db", dest="geoip_db", default='/var/lib/GeoIP/GeoLite2-City.mmdb',
                      help="GeoIP database path")

    (options, _) = parser.parse_args()
    return options


def extract_entry_fields(entry) -> dict[str, Any]:
    """Split a raw log entry into the fields used for output and metrics."""
    fields: dict[str, Any] = {
        "timestamp_microseconds": entry[0],
        "minute_length": entry[1],
        "grid_baselength": entry[2],
        "minute_offset": entry[3],
        "region": entry[4],
        "count_threshold": entry[5],
        "local_x": None,
        "local_y": None,
        "data_area": None,
    }

    if len(entry) > 8:
        fields["local_x"] = entry[8]
        fields["local_y"] = entry[9]
        fields["data_area"] = entry[10]

    return fields


def build_result_row(fields: dict[str, Any], version, country_code, city) -> list[Any]:
    """Build the compact output row for one request entry."""
    return [
        fields["timestamp_microseconds"] / 1000000,
        fields["region"],
        fields["grid_baselength"],
        fields["minute_offset"],
        fields["minute_length"],
        fields["count_threshold"],
        '-',  # Mask the raw client IP in the published log for privacy.
        country_code if country_code is not None else '-',
        city if city is not None else '-',
        version,
        fields["local_x"] if fields["local_x"] is not None else '-',
        fields["local_y"] if fields["local_y"] is not None else '-',
        fields["data_area"] if fields["data_area"] is not None else '-',
    ]


def emit_metrics(statsd_client, fields: dict[str, Any], version, country_code) -> None:
    """Emit access metrics for a single request entry."""
    tags = {
        "version": version if version is not None else '-',
        "region": fields["region"],
        "minutes": fields["minute_length"],
        "offset": fields["minute_offset"],
        "grid": fields["grid_baselength"],
    }
    if fields["local_x"] and fields["local_y"] and fields["data_area"]:
        tags["data_area"] = f"{fields['local_x']}x{fields['local_y']}-{fields['data_area']}"
    if country_code:
        tags["country"] = country_code

    tag_values = ",".join([f"{key}={value}" for key, value in tags.items()])
    statsd_client.incr(f'access,{tag_values}')


def build_results(entries, reader, statsd_client, metrics_enabled) -> list[Any]:
    """Turn the raw request entries of one report into output rows and metrics."""
    results = []
    for entry in entries:
        remote_address = entry[6]
        user_agent = entry[7]
        version = user_agent_version(user_agent)

        city, country_code = geoip_lookup(reader, remote_address)

        fields = extract_entry_fields(entry)
        results.append(build_result_row(fields, version, country_code, city))

        if metrics_enabled:
            emit_metrics(statsd_client, fields, version, country_code)

    return results


def process_json_file(json_file_name, reader, statsd_client, base_dir, metrics_enabled) -> None:
    """Parse and write a single report file."""
    logger.debug(f"opened file {json_file_name}")

    with open(json_file_name, 'r', encoding='utf-8') as json_file:
        data = json.load(json_file)

    global_timestamp = datetime.fromtimestamp(data['timestamp'] / 1000000, tz=timezone.utc)

    if 'get_strikes_grid' in data:
        results = build_results(data['get_strikes_grid'], reader, statsd_client, metrics_enabled)
        write_results(global_timestamp, results, base_dir)


def process_file(json_file_name, reader, statsd_client, base_dir, metrics_enabled) -> None:
    """Process one report file and remove it once handled (or known corrupt)."""
    processed = False
    try:
        process_json_file(json_file_name, reader, statsd_client, base_dir, metrics_enabled)
        processed = True
    except JSONDecodeError:
        logger.warning(f"Invalid JSON in file {json_file_name}, deleting it")
        processed = True
    finally:
        # Only delete files that were consumed successfully or are known to be
        # corrupt; an unexpected error must not destroy a valid report.
        if processed and os.path.exists(json_file_name):
            os.unlink(json_file_name)


def main():
    options = parse_options()

    reader = geoip2.database.Reader(options.geoip_db)
    statsd_client = statsd.StatsClient('localhost', 8125, prefix='org.blitzortung.service')

    if options.debug:
        logger.setLevel(logging.DEBUG)

    base_dir = options.base_dir
    logger.debug("processing reports in %s", base_dir)

    json_file_names = glob.glob(os.path.join(base_dir, '*.json'))
    json_file_names.sort()

    for json_file_name in json_file_names:
        process_file(json_file_name, reader, statsd_client, base_dir, options.metrics)


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
              'a+', encoding='utf-8') as output_file:
        for result in results:
            line = "\t".join([value_to_string(value) for value in result])
            output_file.write(line + "\n")


if __name__ == "__main__":
    main()
