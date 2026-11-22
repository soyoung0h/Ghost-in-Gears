# Ghost-in-Gears

A mystery case solved with SQL and Snowflake. Each week we got a new piece of the story (crime records, forum posts, phone records, CCTV footage) and had to load it, connect it to what we already had, and narrow down the suspects.

Class project for GEN BUS 746 (Advanced SQL & Data Warehousing), UW–Madison MSBA, Spring 2024. Revisited in Nov 2026.

## Files
Intro SQL – crime records and victim profiles
Advanced SQL – linking anonymous forum accounts to city officials by IP address, Time Travel, roles
Data Modeling – phone directory and call log, victim view
Extract & Load – reloading a pipe-delimited call log, calls in the 14 days before each homicide, CCTV JSON
Data Transformation – decoding office locations, officials who called the victims
Data Governance – resource monitor, timeouts, read-only role for views

The data was in course S3 buckets, so it's not included here.

## Nov 2026 updates

Went back through the scripts and fixed a few things:

- 'homicide' → 'Homicide' (was returning nothing)
- % of answered calls is now calculated instead of a hard-coded number
- officials ranked by posts now actually uses the post count
- forum_posts_by_officials now only includes posts from officials' IPs
- load check counts rows in the table instead of the stage
- removed a MAX() that didn't do anything
- Time Travel restore uses LAST_QUERY_ID() instead of a copied query ID
- typo fixes (tenure, stage name, comment marker)
- split weeks 5 and 6 into their own files
