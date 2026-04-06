#!/usr/bin/env python3
"""
Live Tesla Solar Poller.

Polls the Tesla API at a configurable interval (default: 15 minutes) for
real-time power flow data and publishes to both MQTT and InfluxDB.

This replaces the daily-only data pipeline for near-real-time monitoring
in Home Assistant (via MQTT) and Grafana (via InfluxDB).

Environment variables:
  POLL_INTERVAL_SECONDS - Polling interval (default: 900 = 15 minutes)
  TESLA_EMAIL           - Tesla account email (required)
  TESLA_CACHE_FILE      - Path to OAuth cache file (default: cache.json)
  MQTT_ENABLED          - Enable MQTT publishing (default: true)
  INFLUXDB_ENABLED      - Enable InfluxDB publishing (default: true)

The poller uses the SITE_DATA (live_status) endpoint which returns
current power values for solar, battery, grid, and load.
"""

import logging
import os
import signal
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path

import pytz
import teslapy

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from config import Config
from sun_data import SunData

logger = logging.getLogger(__name__)

POLL_INTERVAL = int(os.environ.get('POLL_INTERVAL_SECONDS', '900'))


def setup_logging():
    """Configure logging."""
    log_level = getattr(logging, Config.LOG_LEVEL, logging.INFO)
    handlers = [logging.StreamHandler()]
    if Config.LOG_FILE:
        log_path = Path(Config.LOG_FILE)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(str(log_path).replace('.log', '-poller.log')))

    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=handlers,
    )


def get_tesla_client():
    """Create and return an authenticated Tesla API client."""
    email = Config.TESLA_EMAIL
    if not email:
        logger.error('TESLA_EMAIL is required')
        sys.exit(1)

    cache_file = os.environ.get('TESLA_CACHE_FILE', 'cache.json')
    tesla = teslapy.Tesla(email, cache_file=cache_file, retry=2, timeout=30)

    if not tesla.authorized:
        logger.error(
            'Tesla API not authorized. Run tesla_solar_download.py '
            'manually first to complete the OAuth flow.'
        )
        sys.exit(1)

    return tesla


def get_site_ids(tesla):
    """Discover energy site IDs from the Tesla API."""
    sites = []
    for product in tesla.api('PRODUCT_LIST')['response']:
        resource_type = product.get('resource_type')
        if resource_type in ('battery', 'solar'):
            sites.append({
                'site_id': product['energy_site_id'],
                'resource_type': resource_type,
            })
    return sites


def poll_live_status(tesla, site_id):
    """
    Poll the SITE_DATA (live_status) endpoint for real-time power data.

    Returns a dict with power values and SOE, or None on failure.
    """
    try:
        response = tesla.api(
            'SITE_DATA',
            path_vars={'site_id': site_id},
        )['response']

        now = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

        result = {
            'timestamp': response.get('timestamp', now),
            'solar_power': response.get('solar_power', 0),
            'battery_power': response.get('battery_power', 0),
            'grid_power': response.get('grid_power', 0),
            'load_power': response.get('load_power', 0),
            'battery_soe': response.get('percentage_charged', 0),
            'grid_status': response.get('grid_status', 'Unknown'),
        }

        # Compute load if not provided (solar + battery + grid = load)
        if result['load_power'] == 0 and result['solar_power'] != 0:
            result['load_power'] = (
                result['solar_power']
                + result['battery_power']
                + result['grid_power']
            )

        return result

    except Exception as e:
        logger.error(f'Failed to poll live status for site {site_id}: {e}')
        traceback.print_exc()
        return None


def poll_energy_today(tesla, site_id):
    """
    Poll today's energy totals via CALENDAR_HISTORY_DATA.

    Returns a dict with cumulative Wh values for today, or None on failure.
    """
    try:
        # Get site timezone
        site_config = tesla.api(
            'SITE_CONFIG',
            path_vars={'site_id': site_id},
        )['response']
        tz_name = site_config.get('installation_time_zone', 'America/Los_Angeles')
        tz = pytz.timezone(tz_name)

        now = datetime.now(tz)
        start_date = now.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
        end_date = now.replace(hour=23, minute=59, second=59, microsecond=0).isoformat()

        response = tesla.api(
            'CALENDAR_HISTORY_DATA',
            path_vars={'site_id': site_id},
            kind='energy',
            period='day',
            start_date=start_date,
            end_date=end_date,
            time_zone=tz_name,
        )['response']

        if not response or 'time_series' not in response:
            logger.warning('No energy time_series in response')
            return None

        # Sum all intervals in today's time series
        totals = {
            'solar_energy_exported': 0,
            'grid_energy_imported': 0,
            'grid_energy_exported_from_solar': 0,
            'battery_energy_exported': 0,
            'battery_energy_imported_from_solar': 0,
            'consumer_energy_imported_from_solar': 0,
        }

        for entry in response['time_series']:
            for key in totals:
                totals[key] += entry.get(key, 0)

        totals['timestamp'] = now.isoformat()
        return totals

    except Exception as e:
        logger.error(f'Failed to poll energy totals for site {site_id}: {e}')
        traceback.print_exc()
        return None


def publish_mqtt(site_id, data, sun_status=None, energy_data=None):
    """Publish live data to MQTT for Home Assistant."""
    if not Config.MQTT_ENABLED:
        return

    from mqtt_publisher import MQTTPublisher

    publisher = MQTTPublisher()
    if not publisher.connect():
        logger.error('Failed to connect to MQTT broker')
        return

    try:
        # Publish HA discovery config (idempotent, retained)
        publisher.publish_ha_discovery(str(site_id))

        # Publish current power values
        publisher.publish_power_data(str(site_id), {
            'timestamp': data['timestamp'],
            'solar_power': data['solar_power'],
            'battery_power': data['battery_power'],
            'grid_power': data['grid_power'],
            'load_power': data['load_power'],
        })

        # Publish SOE
        publisher.publish_soe_data(str(site_id), {
            'timestamp': data['timestamp'],
            'soe': data['battery_soe'],
        })

        # Publish sun data
        if sun_status:
            publisher.publish_sun_data(str(site_id), sun_status)

        # Publish energy totals for HA Energy Dashboard
        if energy_data:
            publisher.publish_energy_data(str(site_id), energy_data)
            logger.info(
                f'MQTT energy: solar={energy_data["solar_energy_exported"]:.0f}Wh '
                f'grid_in={energy_data["grid_energy_imported"]:.0f}Wh '
                f'grid_out={energy_data["grid_energy_exported_from_solar"]:.0f}Wh '
                f'batt_out={energy_data["battery_energy_exported"]:.0f}Wh'
            )

        logger.info(
            f'MQTT: solar={data["solar_power"]:.0f}W '
            f'battery={data["battery_power"]:.0f}W '
            f'grid={data["grid_power"]:.0f}W '
            f'load={data["load_power"]:.0f}W '
            f'soe={data["battery_soe"]:.1f}%'
        )

    except Exception as e:
        logger.error(f'Failed to publish to MQTT: {e}')
        traceback.print_exc()
    finally:
        publisher.disconnect()


def publish_influxdb(site_id, data, sun_status=None, energy_data=None):
    """Publish live data point to InfluxDB."""
    if not Config.INFLUXDB_ENABLED:
        return

    from influxdb_client import InfluxDBClient, Point, WritePrecision
    from influxdb_client.client.write_api import SYNCHRONOUS

    try:
        client = InfluxDBClient(
            url=Config.INFLUXDB_URL,
            token=Config.INFLUXDB_TOKEN,
            org=Config.INFLUXDB_ORG,
        )
        write_api = client.write_api(write_options=SYNCHRONOUS)

        ts = data['timestamp']

        # Power data point
        power_point = (
            Point('tesla_solar_power')
            .tag('site_id', str(site_id))
            .field('solar_power', float(data['solar_power']))
            .field('battery_power', float(data['battery_power']))
            .field('grid_power', float(data['grid_power']))
            .field('load_power', float(data['load_power']))
            .time(ts)
        )
        write_api.write(bucket=Config.INFLUXDB_BUCKET, record=power_point)

        # SOE data point
        soe_point = (
            Point('tesla_solar_soe')
            .tag('site_id', str(site_id))
            .field('soe', float(data['battery_soe']))
            .time(ts)
        )
        write_api.write(bucket=Config.INFLUXDB_BUCKET, record=soe_point)

        # Sun data point
        if sun_status:
            sun_point = (
                Point('sun_position')
                .tag('site_id', str(site_id))
                .field('altitude', float(sun_status.get('altitude', 0)))
                .field('azimuth', float(sun_status.get('azimuth', 0)))
                .field('is_daytime', sun_status.get('is_daytime', False))
                .field('time_to_sunset_hours', float(sun_status.get('time_to_sunset_hours', 0)))
                .field('production_factor', float(sun_status.get('production_factor', 0)))
                .time(ts)
            )
            write_api.write(bucket=Config.INFLUXDB_BUCKET, record=sun_point)

        # Energy totals point
        if energy_data:
            energy_point = (
                Point('tesla_solar_energy')
                .tag('site_id', str(site_id))
                .field('solar_energy_exported', float(energy_data.get('solar_energy_exported', 0)))
                .field('grid_energy_imported', float(energy_data.get('grid_energy_imported', 0)))
                .field('grid_energy_exported_from_solar', float(energy_data.get('grid_energy_exported_from_solar', 0)))
                .field('battery_energy_exported', float(energy_data.get('battery_energy_exported', 0)))
                .field('battery_energy_imported_from_solar', float(energy_data.get('battery_energy_imported_from_solar', 0)))
                .field('consumer_energy_imported_from_solar', float(energy_data.get('consumer_energy_imported_from_solar', 0)))
                .time(ts)
            )
            write_api.write(bucket=Config.INFLUXDB_BUCKET, record=energy_point)

        logger.info(f'InfluxDB: wrote power + soe + sun + energy data at {ts}')

        client.close()

    except Exception as e:
        logger.error(f'Failed to publish to InfluxDB: {e}')
        traceback.print_exc()


def run_poll_cycle(tesla, sites, sun_tracker):
    """Run one poll cycle for all sites."""
    # Get sun data once per cycle (same for all sites at this location)
    sun_status = sun_tracker.get_sun_status()
    production_factor = sun_tracker.get_production_factor()
    if sun_status:
        sun_status['production_factor'] = round(production_factor * 100, 1)  # as percentage
        logger.info(
            f'Sun: alt={sun_status.get("altitude", 0):.1f}° '
            f'az={sun_status.get("azimuth", 0):.1f}° '
            f'daytime={sun_status.get("is_daytime", False)} '
            f'factor={production_factor:.0%}'
        )

    for site in sites:
        site_id = site['site_id']
        obfuscated = f"***{str(site_id)[-4:]}"

        logger.debug(f'Polling site {obfuscated}...')
        data = poll_live_status(tesla, site_id)

        if data is None:
            logger.warning(f'No data returned for site {obfuscated}')
            continue

        # Poll energy totals (today's cumulative Wh)
        energy_data = poll_energy_today(tesla, site_id)

        publish_mqtt(site_id, data, sun_status, energy_data)
        publish_influxdb(site_id, data, sun_status, energy_data)


def main():
    setup_logging()

    logger.info(
        f'Tesla Solar Live Poller starting '
        f'(interval={POLL_INTERVAL}s / {POLL_INTERVAL/60:.0f}min)'
    )

    # Handle graceful shutdown
    running = True

    def signal_handler(sig, frame):
        nonlocal running
        logger.info('Shutdown signal received, stopping...')
        running = False

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    tesla = get_tesla_client()
    sites = get_site_ids(tesla)
    sun_tracker = SunData()

    if not sites:
        logger.error('No energy sites found on Tesla account')
        sys.exit(1)

    logger.info(f'Found {len(sites)} energy site(s)')

    # Initial poll immediately
    run_poll_cycle(tesla, sites, sun_tracker)

    while running:
        logger.info(f'Next poll in {POLL_INTERVAL}s ({POLL_INTERVAL/60:.0f}min)')

        # Sleep in small increments to allow signal handling
        sleep_end = time.time() + POLL_INTERVAL
        while running and time.time() < sleep_end:
            time.sleep(min(10, sleep_end - time.time()))

        if running:
            run_poll_cycle(tesla, sites, sun_tracker)

    logger.info('Live poller stopped')


if __name__ == '__main__':
    main()
