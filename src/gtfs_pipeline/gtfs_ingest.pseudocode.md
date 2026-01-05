# GTFS Ingest — Pseudocode

## Purpose
Provide a readable high-level pseudocode summary of the `GTFSIngest` module.

## Class: GTFSIngest
- init(config, db_manager)
  - store config and db_manager
  - setup logger
  - session = None

- async __aenter__()
  - create aiohttp ClientSession with timeout and User-Agent
  - return self

- async __aexit__()
  - if session: close session

- async fetch_gtfs_rt_data(feed_url)
  - log fetching
  - perform HTTP GET using session
  - if status == 200: read bytes and return
  - handle timeouts and errors -> return None

- async fetch_gtfs_static_data(feed_url)
  - log fetching
  - perform HTTP GET for ZIP file
  - if status == 200: read bytes
    - open ZIP in-memory
    - for each expected GTFS file (agency, stops, routes, trips, stop_times, calendar, calendar_dates):
      - if present: read CSV into pandas DataFrame, store in dict
    - return (gtfs_data, raw_zip_bytes)
  - handle errors/timeouts -> return None

- parse_gtfs_rt_data(data, feed_type)
  - try parse protobuf into FeedMessage
  - if feed_type == 'trip_updates': call _parse_trip_updates(feed)
  - elif feed_type == 'vehicle_positions': call _parse_vehicle_positions(feed)
  - else: log error and return {}
  - handle exceptions -> log and return {}

- _parse_trip_updates(feed)
  - initialize empty list trip_updates
  - for each entity in feed.entity:
    - if entity has trip_update:
      - extract trip fields: trip_id, route_id, direction_id, start_time, start_date
      - extract vehicle id if present
      - extract timestamp and delay if present
      - append dictionary to trip_updates
  - return dict containing feed_type, feed.header.timestamp, version, and trip_updates list

- _parse_vehicle_positions(feed)
  - initialize empty list vehicle_positions
  - for each entity in feed.entity:
    - if entity has vehicle:
      - extract vehicle.vehicle.id, trip fields, current_stop_sequence, current_status, timestamp
      - if position present: extract latitude, longitude, bearing, speed
      - append dictionary to vehicle_positions
  - return dict containing feed_type, feed.header.timestamp, version, and vehicle_positions list

- async ingest_feed(feed_url, feed_type, feed_name=None, timestamp_override=None)
  - fetch raw_data via fetch_gtfs_rt_data
  - if no data: return False
  - determine timestamp (override or now)
  - parse parsed_data via parse_gtfs_rt_data
  - add feed_url and optional feed_name to parsed_data
  - store via db_manager.store_gtfs_rt_data(parsed_data, feed_url, raw_bytes, timestamp, feed_name)
  - log success/failure and return boolean

- async ingest_gtfs_static()
  - iterate configured static feeds
  - call _ingest_single_static for each and collect results
  - return results dict mapping feed_url -> bool

- async _ingest_single_static(feed_name, feed_url)
  - fetch gtfs static via fetch_gtfs_static_data
  - if fetch failed: return False
  - timestamp = now
  - call db_manager.store_gtfs_static_data(gtfs_data, feed_url, raw_bytes, timestamp, feed_name)
  - log and return boolean success

- async ingest_realtime_feeds(feed_types=None)
  - build task list for selected feed types from config
  - create asyncio tasks for ingest_feed(feed_url, feed_type, feed_name, timestamp_override)
  - run tasks with asyncio.gather
  - map results to feed_url -> bool (handle exceptions)
  - return results

- async ingest_all_feeds()
  - results = {}
  - ingest GTFS static first and update results
  - ingest realtime feeds and update results
  - return results

- async continuous_realtime_ingestion(interval=60, feed_types=None, include_static_on_first_cycle=False)
  - loop forever:
    - optionally ingest static on first cycle
    - ingest realtime feeds
    - log summary (successful/total)
    - sleep remaining interval
    - handle KeyboardInterrupt and exceptions

- async continuous_ingestion(interval=60)
  - loop forever:
    - ingest_all_feeds
    - log summary and sleep
    - handle KeyboardInterrupt and exceptions

## CLI entry
- async main():
  - config = GTFSConfig()
  - db_manager = DatabaseManager(config.database)
  - async with GTFSIngest(config, db_manager) as ingest:
    - results = await ingest.ingest_all_feeds()
    - print summary

- if __name__ == '__main__': asyncio.run(main())

---

Notes:
- This pseudocode omits low-level error messages and logging details but preserves control flow and responsibilities.
- File saved as a Markdown file for readability.
