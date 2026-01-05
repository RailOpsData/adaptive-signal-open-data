# Pseudocode: ingest_feed (lines 197-230)

- 関数: async ingest_feed(feed_url, feed_type, feed_name=None, timestamp_override=None)
- 目的: 単一のGTFS-RTフィードを取得→解析→DB保存するワークフロー

処理手順:
1. try:
2.   raw_data = await fetch_gtfs_rt_data(feed_url)
3.   if raw_data is falsy:
4.     return False
5.   timestamp = timestamp_override if provided else current time formatted 'YYYYmmdd_HHMMSS'
6.   parsed_data = parse_gtfs_rt_data(raw_data, feed_type)
7.   if parsed_data is falsy:
8.     return False
9.   parsed_data['feed_url'] = feed_url
10.  if feed_name provided: parsed_data['feed_name'] = feed_name
11.  success = await db_manager.store_gtfs_rt_data(
12.    parsed_data,
13.    feed_url,
14.    raw_bytes=raw_data,
15.    timestamp=timestamp,
16.    feed_name=feed_name
17.  )
18.  if success:
19.    log info "Successfully ingested ..."
20.  else:
21.    log error "Failed to store ..."
22.  return success
23. except Exception as e:
24.  log error "Error ingesting ...: e"
25.  return False

# 終了