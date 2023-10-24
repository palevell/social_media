#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# acct_maint_twscrape.py - Thursday, June 22, 2023
""" Follow retweeters and unfollow less-active tweeps """
__version__ = "0.2.6-dev49"

import builtins
import json
import logging.config
import lzma
import os
import sys
from datetime import datetime, timedelta, timezone
from itertools import cycle
from os.path import basename, exists, getmtime, join
from pathlib import Path
from random import shuffle, uniform
from time import sleep

import click
import coloredlogs
import oyaml as yaml
import pandas as pd
import snscrape.base
import snscrape.modules.twitter as sntwitter
from pid.decorator import pidfile
from requests import get
from sqlalchemy import create_engine, inspect, text

basedir = Path(__file__).resolve().parent.parent
if basedir not in sys.path:
	sys.path.insert(0, str(basedir))
from config import Config

__module__ = Path(__file__).resolve().stem

# Set PID Dir
PIDDIR = "/run/user/%d" % os.getegid()
if not exists(PIDDIR):
	PIDDIR = "/tmp"


@pidfile(piddir=PIDDIR)
def main():
	acct_names = Config.ACCTS
	batch_id = get_new_batch_id(_run_dt)
	for acct_name in acct_names:
		acct_dict = process_acct_info(batch_id, acct_name)
		acct_id = acct_dict["id"]
		user_dict = process_user_info(acct_name, acct_id)
		last_tweet = None
		last_tweet_date = None 
		if "lastTweet" in user_dict:
			last_tweet = user_dict["lastTweet"]
		if "lastTweetDate" in user_dict:
			last_tweet_date = user_dict["lastTweetDate"]
		rows = process_following(batch_id, acct_name, acct_id)
			
	return


# @click.command()
# @click.argument("twit", nargs=-1)
# @pidfile(piddir=PIDDIR)
def main_old():
	twit_list = Config.ACCTS
	twit = tuple(twit_list)
	for screen_name in twit:
		screen_name = screen_name.lstrip("@")

		# Load from Twitter data dump
		acct_dict = load_account_info(screen_name)
		acct_id = acct_dict["accountId"]
		asof = acct_dict["asof"]
		following_ids = load_friend_ids(screen_name=screen_name)
		logger.info(f"Following IDs: {len(following_ids):,d}")
		follower_ids = load_follower_ids(screen_name=screen_name)
		logger.info(f"Follower IDs : {len(follower_ids):,d}")

		cached_follower_ids = get_cached_user_ids(follower_ids)
		cached_following_ids = get_cached_user_ids(following_ids)

		# Update friends / following (Cached)
		if cached_following_ids:
			update_relations(acct_id, asof, "following", cached_following_ids)

		# Update followers (Cached)
		if cached_follower_ids:
			update_relations(acct_id, asof, "follower", cached_follower_ids)

		# Find User IDs with issues
		bad_follower_ids = get_bad_user_ids(follower_ids)
		bad_following_ids = get_bad_user_ids(following_ids)

		# Determine which User IDs to fetch (ie. not cached or current)
		fetch_follower_ids = sorted(
			list(set(follower_ids) - set(cached_follower_ids) - set(bad_follower_ids))
		)
		fetch_following_ids = sorted(
			list(
				set(following_ids) - set(cached_following_ids) - set(bad_following_ids)
			)
		)
		# Eliminate duplicates
		fetch_user_ids = sorted(list(set(fetch_following_ids + fetch_follower_ids)))
		logger.info(f"User IDs to fetch: {len(fetch_user_ids):,d}")

		if fetch_user_ids:
			# Testing
			if _test_max_users:
				# Shuffle list of IDs to fetch and pick first XXX values
				for _ in range(10):
					shuffle(fetch_user_ids)
				fetch_user_ids = fetch_user_ids[:_test_max_users]
				# Reduce following/follower IDs to those in the list, above
				following_ids = list(
					set(following_ids).intersection(set(fetch_user_ids))
				)
				follower_ids = list(set(follower_ids).intersection(set(fetch_user_ids)))
				logger.info(f"[TEST] User IDs to fetch: {len(fetch_user_ids)}")
				for i, fetch_user_id in enumerate(fetch_user_ids):
					logger.info(f"    {i + 1:4d} {fetch_user_id}")
			# Fetch profiles for User IDs
			line_number = 0
			all_fetched_ids = []
			batch_size = Config.BATCH_SIZE
			batches = (
				fetch_user_ids[i : i + batch_size]
				for i in range(0, len(fetch_user_ids), batch_size)
			)
			for batch_count, batch in enumerate(batches):
				if batch_count > 0:
					snoozer(_min_batch_delay, _max_batch_delay)
				fetched_ids = fetch_users(batch, line_number)
				if fetched_ids:
					fetched_follower_ids = sorted(
						list(set(follower_ids).intersection(set(fetched_ids)))
					)
					fetched_following_ids = sorted(
						list(set(following_ids).intersection(set(fetched_ids)))
					)

					# Update friends / following (Fetched)
					if fetched_following_ids:
						update_relations(
							acct_id, asof, "following", fetched_following_ids
						)

					# Update followers (Fetched)
					if fetched_follower_ids:
						update_relations(
							acct_id, asof, "follower", fetched_follower_ids
						)

					# ToDo: blocked & muted
					line_number += batch_size
					do_nothing()
	return


def init():
	msgs = [
		f"{__module__} {__version__} Run Start: {_run_dt.replace(microsecond=0)}",
	]
	if DRYRUN:
		msgs.append("(DRY RUN)")
	if not ONLINE_MODE:
		msgs.append("(OFFLINE MODE)")
	logger.info(" ".join(msgs))
	# get_table_names()
	if ONLINE_MODE:
		proxy = os.getenv("ALL_PROXY")
		if proxy:
			print(f"ALL_PROXY = {proxy}")
		# Check IP Address being used
		check_ip_pool = cycle(Config.MYIP_URLS_HTTPS)
		retries = 3
		while retries > 0:
			try:
				url = next(check_ip_pool)
				r = get(url, timeout=(3.05, 30))
				if r.ok:
					logger.info(f"IP Address: {r.text.rstrip()} via {url}")
				retries = -1
			except Exception as e:
				retries -= 1
				if retries > 0:
					logger.warning(e)
				else:
					logger.exception(e)
				do_nothing()
	return


def eoj():
	stop_dt = datetime.now().astimezone().replace(microsecond=0)
	duration = stop_dt.replace(microsecond=0) - _run_dt.replace(microsecond=0)
	logger.info("Run Stop : %s  Duration: %s" % (stop_dt, duration))
	return


def db_create_view(tablename: str, row_type: str) -> str | None:
	sql = None
	row_type = row_type.lower()
	if row_type not in ["following", "followers", "accountinfo", "user", "usertweet"]:
		raise ValueError(f"Unrecognized file_type: '{row_type}'")
	if row_type == "following":
		sql = f"""
			CREATE TEMP VIEW tmp_following AS
			SELECT j->'id_str' id_str, j->'username' username
			FROM {tablename};
		"""
	elif row_type == "usertweet":
		# jsonb_array_elements(j->'descriptionLinks')->'url'
		usertweet_cols = {
			"_type": "_type",
			"blue": "blue",
			"blueType": "blue_type",
			"created": "created_at",
			"descriptionLinks": "description_links",
			"displayname": "display_name",
			"favouritesCount": "favourites_count",
			"followersCount": "followers_count",
			"friendsCount": "friends_count",
			"id": "id",
			"id_str": "id_str",
			"lastTweet": "last_tweet",
			"lastTweetDate": "last_tweet_date",
			"listedCount": "listed_count",
			"location": "location",
			"mediaCount": "media_count",
			"profileBannerUrl": "banner_url",
			"profileImageUrl": "image_url",
			"protected": "protected",
			"rawDescription": "description",
			"statusesCount": "statuses_count",
			"url": "url",
			"username": "username",
		}
		sql = """ CREATE TEMP VIEW loaded_json AS
			SELECT id, batch_id, file_id, asof, posted_at, data_type, acct_name, acct_id, md5_hash,
				j->>'id_str' id_str,
				j->>'username' username,
				j->>'displayname' displayname,
				j->>'created' created,
				j->>'statusesCount' statusesCount,
				j->>'friendsCount' friendsCount,
				j->>'followersCount' followersCount,
				j->>'favouritesCount' favouritesCount,
				j->>'listedCount' listedCount,
				j->>'mediaCount' mediaCount,
				j->>'protected' protected,
				j->>'verified' verified,
				j->>'blue' blue,
				j->>'blueType' blueType,
				j->>'profileImageUrl' profileImageUrl,
				j->>'profileBannerUrl' profileBannerUrl,
				j->>'location' location,
				j->>'rawDescription' rawDescription
			FROM dt_json_loader;
		"""
	return sql.strip()


def db_load_json(batch_id: int, file_type: str, filename: str | Path) -> list:
	loaded_rows = []
	file_type = file_type.lower()
	if file_type not in ["following", "followers", "accountinfo", "user", "usertweet"]:
		raise ValueError(f"Unrecognized file_type: '{file_type}'")
	file_dict = get_new_file_id(batch_id, filename)
	if file_dict:
		file_id = file_dict["file_id"]
		tbl = f"tmp_json_{batch_id}_{file_id}"

		stmts = [
			f"CREATE TEMP TABLE {tbl} (j JSONB) ON COMMIT DROP;",
			f"COPY {tbl} FROM '{filename}' CSV QUOTE e'\x01' DELIMITER e'\x02';",
			db_create_view(tbl, file_type),
			f"SELECT * FROM tmp_{file_type} LIMIT 10;"
		]
		with engine.begin() as trans:
			for stmt in stmts:
				result = trans.execute(text(stmt))
				if result.rowcount > 0:
					loaded_rows = result.fetchall()
					do_nothing()
		if loaded_rows:
			update_file_status(file_id, "J")
	return loaded_rows


def fetch_users(user_ids: list | set, lineno: int = 0) -> list:
	fetched_ids = []
	no_tweet_ids = []
	error_ids = []
	consecutive_error_count = 0

	# Shuffle list of User IDs
	user_id_count = len(user_ids)
	if user_id_count > 50:
		sorted_ids = sorted(user_ids)
		top_half = user_ids[:user_id_count]
		bottom_half = user_ids[user_id_count:]
		for _ in range(10):
			shuffle(top_half)
			shuffle(bottom_half)
		user_ids = bottom_half + top_half
	for _ in range(10):
		shuffle(user_ids)

	# Fetch User IDs
	for count, user_id in enumerate(user_ids):
		if count > 0:
			sleep(uniform(_min_search_delay, _max_search_delay))
		try:
			user = sntwitter.TwitterUserScraper(user_id)
			entity = user.entity

			if entity.link:
				url = entity.link.url
			else:
				url = None
			last_tweeted = None
			i, tweet = -1, None
			for i, tweet in enumerate(user.get_items()):
				last_tweeted = tweet.date
				if i == 1:
					break
			if not last_tweeted:
				# Protected or shadow-banned account?
				no_tweet_ids.append(user_id)
				msg = " ".join(
					[
						f"{lineno+count+1:5d}) {user_id:19d}",
						f"No tweets",
						f"https://twitter.com/intent/user?user_id={user_id}",
					]
				)
				logger.warning(msg)
				insert_issue(user_id, _run_dt, False, True, False, msg)
			logger.debug(f"{lineno+count+1:5d}) {user_id:19d} Adding to database . . .")
			good_id = insert_user(
				user_id=int(entity.id),
				asof=_run_dt,  # AsOf
				username=entity.username,
				displayname=entity.displayname,
				created_at=entity.created,
				followers_count=entity.followersCount,
				friends_count=entity.friendsCount,
				statuses_count=entity.statusesCount,
				listed_count=entity.listedCount,
				media_count=entity.mediaCount,
				last_tweeted=last_tweeted,
				blue=entity.blue,
				protected=entity.protected,
				verified=entity.verified,
				description=entity.rawDescription,
				# label=entity.label.description,
				location=entity.location,
				url=url,
				image_url=entity.profileImageUrl,
				banner_url=entity.profileBannerUrl,
			)
			fetched_ids.append(good_id)
			consecutive_error_count = 0
			do_nothing()
		except snscrape.base.ScraperException as e:
			consecutive_error_count += 1
			error_ids.append(user_id)
			msg = " ".join(
				[
					f"{lineno+count+1:5d}) {user_id:19d} {e}",
					f"https://twitter.com/intent/user?user_id={user_id}",
				]
			)
			logger.warning(msg)
			if "failed, giving up" in e.args:
				insert_issue(user_id, _run_dt, True, False, False, msg)
			elif "Response" in e.args:
				insert_issue(user_id, _run_dt, True, False, False, msg)
			elif "User" in e.args:
				insert_issue(user_id, _run_dt, False, False, True, msg)
			else:
				insert_issue(user_id, _run_dt, False, False, False, msg)
			if consecutive_error_count == 25:
				raise Exception("Too many errors.  Aborting.")
		# snoozer(_min_error_delay, _max_error_delay)
	return fetched_ids


def find_logging_config() -> str | None:
	logging_config = None
	config_dirs = ["/etc/Searches", basedir]  # ToDo: configdir
	for cd in config_dirs:
		config_file = join(cd, "logging.yaml")
		if not exists(config_file):
			continue
		logging_config = config_file
	return logging_config


def get_cached_users(user_ids):
	user_ids = sorted(user_ids)
	tablename = "dt_user"
	columns = [
		"user_id",
		"asof",
		"username",
		"displayname",
		"created_at",
		"followers_count",
		"friends_count",
		"listed_count",
		"statuses_count",
		"last_tweeted",
	]
	where_clause = "asof > :since"
	order_by = "asof DESC"
	params = {"since": (_run_dt.replace(microsecond=0) - timedelta(days=_cache_days))}
	sql = f"SELECT * FROM {tablename} WHERE {where_clause} ORDER BY {order_by};"
	with engine.connect() as conn:
		setschema = f"SET search_path TO {schema},public;"
		conn.execute(text(setschema))

		where_conditions = [
			"user_id IN (%s)" % stringify(user_ids),
			"asof > '%s'" % (_run_dt.replace(microsecond=0) - timedelta(hours=24)),
		]
		where_clause = " AND ".join(where_conditions)
		orderby_clause = "user_id"

		sql = "SELECT DISTINCT * FROM %s WHERE %s ORDER BY %s LIMIT 100000;" % (
			tablename,
			where_clause,
			orderby_clause,
		)
		print(sql)
		rows = conn.execute(text(sql))
		row_count = rows.rowcount
		print("Row Count: {:,d}".format(row_count))
		# df = pd.DataFrame(rows, columns=rows.keys())
	return pd.DataFrame(rows, columns=list(rows.keys()))


# ToDo: Should this be using dt_user_history?
def get_cached_user_ids(user_ids):
	user_ids = sorted(user_ids)
	cached_user_ids = []
	with engine.connect() as conn:
		tablename = "dt_user"
		setschema = f"SET search_path TO {schema},public;"
		conn.execute(text(setschema))
		params = {
			"since": (_run_dt.replace(microsecond=0) - timedelta(days=_cache_days))
		}
		# Columns: ['user_id', 'asof', 'screen_name', 'name', 'created_at', 'default_profile_image', 'protected', 'followers_count', 'friends_count', 'listed_count', 'statuses_count', 'last_tweet']
		where_conditions = [
			"user_id IN (%s)" % stringify(user_ids),
			"asof > :since",
		]
		where_clause = " AND ".join(where_conditions)
		orderby_clause = "user_id"
		sql = f"SELECT DISTINCT user_id FROM {tablename} WHERE {where_clause} ORDER BY {orderby_clause};"
		# logger.info(sql)
		for row in conn.execute(text(sql), params).fetchall():
			cached_user_ids.append(row[0])
		row_count = len(cached_user_ids)
		# df = pd.DataFrame(rows, columns=rows.keys())
	return cached_user_ids


def get_bad_user_ids(user_ids):
	user_ids = sorted(user_ids)
	bad_user_ids = []

	with engine.connect() as conn:
		tablename = "dt_issue"
		setschema = f"SET search_path TO {schema},public;"
		conn.execute(text(setschema))
		params = {
			"since": (_run_dt.replace(microsecond=0) - timedelta(days=_cache_days))
		}
		# Columns: ['user_id', 'asof', 'screen_name', 'name', 'created_at', 'default_profile_image', 'protected', 'followers_count', 'friends_count', 'listed_count', 'statuses_count', 'last_tweet']
		where_conditions = [
			"user_id IN (%s)" % stringify(user_ids),
			"asof > :since",
		]
		where_clause = " AND ".join(where_conditions)
		orderby_clause = "user_id"
		sql = f"SELECT DISTINCT user_id FROM {tablename} WHERE {where_clause} ORDER BY {orderby_clause};"
		# logger.info(sql)
		for row in conn.execute(text(sql), params).fetchall():
			bad_user_ids.append(row[0])
		row_count = len(bad_user_ids)
	return bad_user_ids


def get_follower_ids(user_id=None, screen_name=None):
	return load_ids(id_type="follower", user_id=user_id, screen_name=screen_name)


def get_new_batch_id(batch_date: datetime | None) -> int:
	if not batch_date:
		batch_date = _run_dt
	sql = "INSERT INTO dt_batch_control(batch_date) VALUES (:batch_date) RETURNING id;"
	print(sql)
	params = {"batch_date": batch_date}
	with engine.begin() as conn:
		row = conn.execute(text(sql), params).fetchone()
		batch_id = row[0]
		print(f"New Batch ID: {batch_id}")
	return batch_id


def get_new_file_id(batch_id: int, filename: str | Path) -> dict | None:
	file_dict = None
	filedate = datetime.fromtimestamp(getmtime(filename)).astimezone()
	lines = line_count(filename)

	with engine.begin() as trans:
		params = {
			"batch_id": batch_id,
			"filename": str(filename),
			"filedate": filedate,
			"lines": lines,
		}
		columns = [
			"batch_id",
			"filename",
			"filedate",
			"lines",
		]
		cols = ",".join(columns)
		placeholders = ",".join(f":{x}" for x in columns)
		sql = f"""
			INSERT INTO dt_file_control({cols})
			VALUES({placeholders})
			ON CONFLICT (batch_id, filename) DO NOTHING
			RETURNING id;
		""".strip()
		try:
			result = trans.execute(text(sql), params)
			if result.rowcount > 0:
				file_id = result.fetchone()[0]
				file_dict = {"file_id": file_id, "filedate": filedate, "lines": lines}
		except Exception as e:
			logger.exception(e)
			do_nothing()
	return file_dict


def get_table_names():
	# Get Table Names
	logger.debug("Table Names & Row Counts:")
	for table_name in sorted(inspect(engine).get_table_names()):
		sql = f"SELECT count(*) rows FROM {table_name};"
		with engine.connect() as conn:
			result = conn.execute(text(sql)).fetchone()
		logger.debug(f"- {table_name:20s}\t{result[0]:9,d}")
	return


def load_account_info(screen_name: str) -> dict:
	"""
	- Retrieves account information from Twitter data archive
	- Uses timestamp of the archive to obtain 'asof' date
	"""
	acct_dict = {}
	filename = _twscrape_data_dir / f"{screen_name}" / "data" / f"account.js"
	asof = datetime.fromtimestamp(getmtime(filename)).astimezone()
	with open(filename) as fp:
		data = fp.read().lstrip("window.YTD.account.part0 = ")
		acct_dict = json.loads(data)[0]["account"]
	acct_dict.update({"asof": asof})
	return acct_dict


def load_follower_ids(user_id=None, screen_name=None):
	return load_ids(id_type="follower", user_id=user_id, screen_name=screen_name)


def load_friend_ids(user_id=None, screen_name=None):
	return load_ids(id_type="following", user_id=user_id, screen_name=screen_name)


def load_ids(id_type, user_id=None, screen_name=None):
	filename = None
	if id_type not in [
		"blocked",
		"follower",
		"following",
		"friend",
		"muted",
	]:
		raise ValueError(
			"Invalid id_type.  Valid types: blocked, follower, friend, or muted"
		)
	if id_type == "friend":
		id_type = "following"
	# filename = _data_dir / f"{screen_name}_following.js"
	filename = _twscrape_data_dir / f"{screen_name}" / "data" / f"{id_type}.js"
	friendly_id_type = str(id_type).capitalize()
	# fn_logger = logging.getLogger("%s.get_%s_ids" % (__module__, id_type))
	logger.info(f"Filename: {filename}")
	ids = []
	id_key = "accountId"
	if exists(filename):  # and _run_dt.timestamp() - getmtime(filename) < 12 * 60**2:
		ids = load_ids_file(id_type, id_key, filename)
	if not ids:
		logger.debug("Fetching %s IDs for @%s . . ." % (friendly_id_type, screen_name))
	return ids


def load_ignored_user_ids():
	ids = []
	filename = _twscrape_data_dir / "ignore_user_ids.txt"
	with open(filename) as fp:
		ids = [x.rstrip() for x in fp.readlines()]
	return ids


def idle(last_tweeted):
	"""
	Calculate idle time for a user account
	:param last_tweeted: date of user's last tweet
	:return: date diffence between run time and last tweet (timedelta object)
	"""
	if not last_tweeted.tzinfo:
		last_tweeted.replace(tzinfo=timezone.utc)
	return _run_utc - last_tweeted


def insert_issue(
	user_id: int,
	asof: datetime,
	no_response: bool,
	no_tweets: bool,
	no_user: bool,
	message: str,
) -> int:
	params_dict = {
		"user_id": user_id,
		"asof": asof,
		"no_response": no_response,
		"no_tweets": no_tweets,
		"no_user": no_user,
		"message": message,
	}
	params_list = ",".join([f":{x}" for x in params_dict.keys()])
	sql = f"SELECT * FROM fn_insert_issue({params_list});"
	with engine.begin() as conn:
		setschema = f"SET search_path TO {schema},public;"
		conn.execute(text(setschema))
		seen_id = conn.execute(text(sql), params_dict).fetchone()[0]
		do_nothing()

	return seen_id


def insert_user(
	user_id,
	asof,
	username,
	displayname,
	created_at,
	followers_count=None,
	friends_count=None,
	listed_count=None,
	media_count=None,
	statuses_count=None,
	last_tweeted=None,
	blue=None,
	protected=None,
	verified=None,
	default_profile=None,
	description=None,
	label=None,
	location=None,
	url=None,
	image_url=None,
	banner_url=None,
) -> int | None:
	seen_id = None
	params_dict = {
		"in_user_id": user_id,
		"asof": asof,
		"username": username,
		"displayname": displayname,
		"created_at": created_at,
		"followers_count": followers_count,
		"friends_count": friends_count,
		"listed_count": listed_count,
		"media_count": media_count,
		"statuses_count": statuses_count,
		"last_tweeted": last_tweeted,
		"blue": blue,
		"protected": protected,
		"verified": verified,
		"default_profile": default_profile,
		"description": description,
		"label": label,
		"location": location,
		"url": url,
		"image_url": image_url,
		"banner_url": banner_url,
	}
	params_list = ",".join([f":{x}" for x in params_dict.keys()])
	sql = f"SELECT * FROM fn_user_insert({params_list});"
	with engine.begin() as conn:
		setschema = f"SET search_path TO {schema},public;"
		conn.execute(text(setschema))
		seen_id = conn.execute(text(sql), params_dict).fetchone()[0]
		do_nothing()
	return seen_id


def line_count(filename: str | Path) -> int:
	l = -1
	with open(filename) as fp:
		l = sum(1 for _ in fp)
	return l


def load_ids_file(id_type, id_key, filename):
	ids = []
	with open(filename) as fp:
		# data = json.load(infile)
		data = fp.read().lstrip(f"window.YTD.{id_type}.part0 = ")
		ids_dict = json.loads(data)

	do_nothing()
	for item in ids_dict:
		if id_type in item:
			if id_key in item[id_type]:
				acct_id = item[id_type][id_key]
				try:
					acct_id = int(acct_id)
				except Exception as e:
					logger.exception(e)
				# logger.info(acct_id)
				ids.append(acct_id)
				do_nothing()
	return ids


def process_acct_info(batch_id: int, acct_name: str) -> dict:
	"""
	At present, the AccountInfo.json files don't contain lastTweet or lastTweetDate keys,
	which have been added to the files in the Users directory
	"""
	acct_dict = {}
	acct_dir = _twscrape_data_dir / "Accounts" / acct_name
	if not acct_dir.exists():
		logger.warning(f"Account directory not found: {acct_dir}")
	filename = acct_dir / "AccountInfo.json"
	file_dict = get_new_file_id(batch_id, filename)
	file_id = file_dict["file_id"]
	# asof = datetime.fromtimestamp(getmtime(filename)).astimezone()
	asof = file_dict["filedate"]
	with open(filename) as acctfile:
		acct_dict = json.load(acctfile)
	acct_name2 = acct_dict["username"]
	if acct_name != acct_name2:
		raise ValueError(
			f"Account name mismatch.  Was expecting '{acct_name}', got '{acct_name2}'."
		)
	acct_id = acct_dict["id"]
	created_at = acct_dict["created"]
	logger.info(f"{acct_id:19d} @{acct_name:16s} {asof.replace(microsecond=0)}")
	# cols = ",".join(["id", "user_id", "username", "status", "created_at", "notes"])
	with engine.begin() as trans:
		cols = ",".join(
			[
				"user_id",
				"username",
				"created_at",
				"asof",
			]
		)
		params = {
			"user_id": acct_id,
			"username": acct_name,
			"created_at": created_at,
			"asof": asof,
		}
		sql = f"""
			INSERT INTO dt_auth_user AS dau({cols})
			VALUES(:user_id, :username, :created_at, :asof)
			ON CONFLICT (user_id, username) DO UPDATE
				SET asof = :asof
				WHERE dau.user_id = EXCLUDED.user_id
				  AND dau.username = EXCLUDED.username
				  AND dau.asof < EXCLUDED.asof
			RETURNING id;
		""".strip()
		result = trans.execute(text(sql), params)
		if result.rowcount > 0:
			# ToDo: When we get newer data
			update_file_status(file_id, "P")
			do_nothing()
		if acct_dict:
			update_file_status(file_id, "C")
	return acct_dict


def process_followers(batch_id: int, acct_name: str, acct_id: int):
	process_relationships(batch_id, "Followers", acct_name, acct_id)
	return


def process_following(batch_id: int, acct_name: str, acct_id: int) -> int | None:
	process_relationships(batch_id, "Following", acct_name, acct_id)
	return


def process_relationships(batch_id: int, relation_type: str, acct_name: str, acct_id: int):
	rows = []
	filename = None
	operation = None
	relation_type = relation_type.lower()
	if relation_type == "following":
		operation = "follow"
		filename = _twscrape_data_dir / "Accounts" / acct_name / "Following.json"
	elif relation_type == "follower":
		pass
	else:
		raise ValueError(f"Unrecognized relation_type: '{relation_type}'")
	logger.debug(f"Processing {filename}")
	file_dict = get_new_file_id(batch_id, filename)
	file_id = file_dict["file_id"]
	asof = datetime.fromtimestamp(getmtime(filename)).astimezone()
	update_file_status(file_id, "R")
	loaded_rows = db_load_json(batch_id, relation_type, filename)

	for user_id, username in loaded_rows:
		"""
		# Columns: user_id1, user_id2, asof, follows, blocked, muted
		for user_id in user_ids:
		"""
		params = {
			"operation": operation,
			"in_user_id1": acct_id,
			"in_user_id2": user_id,
			"asof": asof,
		}
		if relation_type == "follower":
			params.update({
				"in_user_id1": user_id,
				"in_user_id2": acct_id,
			})
		with engine.begin() as trans:
			sql = "SELECT * FROM fn_relation(:operation, :in_user_id1, :in_user_id2, :asof);"
			# I think this returns a list of IDs in dt_relation
			rows = trans.execute(text(sql)).fetchall()
			do_nothing()

	return rows


def process_user_info(acct_name: str, acct_id: int) -> dict:
	"""
	At present, the AccountInfo.json files don't contain lastTweet or lastTweetDate keys,
	which have been added to the files in the Users directory
	"""
	user_dict = {}
	# So far, the User files are two days older than the AccountInfo files
	last_tweet = None
	last_tweet_date = None
	user_info = _twscrape_data_dir / "Users" / f"{acct_name}_{acct_id}.json"
	user_asof = datetime.fromtimestamp(getmtime(user_info)).astimezone()
	with open(user_info) as userfile:
		user_dict = json.load(userfile)
	if "lastTweet" in user_dict:
		last_tweet = user_dict["lastTweet"]
	if "lastTweetDate" in user_dict:
		last_tweet_date = datetime.fromisoformat(user_dict["lastTweetDate"]).astimezone()
	logger.info(f"{acct_id:19d} @{acct_name:16s} {user_asof.replace(microsecond=0)} {last_tweet_date}")
	return user_dict


def save_ids(filename, ids):
	# fn_logger = logging.getLogger(__module__ + ".save_ids")
	lines = []
	for id in ids:
		line = str(id)
		if not line.endswith(os.linesep):
			line = line + os.linesep
		lines.append(line)
	# We're getting fancy, here - supporting XZ/LZMA compression
	if filename.endswith(".xz"):
		open = lzma.open
	else:
		open = builtins.open
	logger.debug("Saving IDs to '%s'" % filename)
	with open(filename, "wt") as lzfile:
		lzfile.writelines(lines)
	logger.debug("Saved %d IDs to '%s'" % (len(ids), filename))
	return


def snoozer(min_sleep, max_sleep=None):
	"""
	Sleep for for a specified amount of time; if max_sleep is specified,
	a random number is generated
	:param min_sleep: The minimum amount of time to sleep
	:param max_sleep: The maximum amount of time to sleep, if supplied
	:return: None
	"""
	# fn_logger = logging.getLogger("%s.snoozer" % __module__)
	if type(min_sleep) not in [int, float]:
		raise ValueError("min_sleep should be an int or float")
	if max_sleep:
		if type(max_sleep) not in [int, float]:
			raise ValueError("max_sleep should be an int or float")
		sleep_seconds = abs(uniform(min_sleep, max_sleep))
	else:
		sleep_seconds = abs(min_sleep)
	start_ts = datetime.now().timestamp()
	logger.debug("sleep(%f)" % sleep_seconds)
	sleep(sleep_seconds)
	stop_ts = datetime.now().timestamp()
	actual_sleep = stop_ts - start_ts
	if actual_sleep < sleep_seconds:
		raise Exception("actual sleep less than requested sleep")
	return


def stringify(iterable, separator=","):
	iterable = separator.join([str(x) for x in iterable])
	return iterable


def update_file_status(file_id: int, status: str):
	params = {"file_id": file_id, "status": status[0].upper(), "status_date": _run_dt}
	sql = f"UPDATE dt_file_control SET file_status = :status, status_date = :status_date;"
	with engine.begin() as trans:
		rowcount = trans.execute(text(sql), params).rowcount
	return


# ToDo: Change database function(s) to handle follow/unfollow, block/unblock, and mute/unmute operations
def update_relations(acct_id: int, asof: datetime, relation_type: str, user_ids: list):
	"""
	Updates dt_relation; Both acct_id and user_id must exist in dt_user, beforehand
	"""
	if relation_type not in ["follower", "following", "friend", "blocked", "muted"]:
		raise ValueError(f"Unrecognized relation type: '{relation_type}'")
	if not user_ids:
		raise ValueError("Nothing to process")
	# ToDo: Follower IDs
	operation = None
	if relation_type in ["follower", "following", "friend"]:
		operation = "follow"
	elif relation_type == "blocked":
		operation = "block"
	elif relation_type == "muted":
		operation = "mute"
	else:
		raise ValueError(f"Unrecognized relation type: '{relation_type}'")
	with engine.begin() as conn:
		setschema = f"SET search_path TO {schema},public;"
		conn.execute(text(setschema))
		# Columns: user_id1, user_id2, asof, follows, blocked, muted
		for user_id in user_ids:
			params = {
				"operation": operation,
				"in_user_id1": acct_id,
				"in_user_id2": user_id,
				"asof": asof,
			}
			if relation_type == "follower":
				params.update(
					{
						"in_user_id1": user_id,
						"in_user_id2": acct_id,
					}
				)
			sql = "SELECT * FROM fn_relation(:operation, :in_user_id1, :in_user_id2, :asof);"
			results = conn.execute(text(sql), params)
			do_nothing()

		# ToDo: Process User IDs no longer related

	return


def do_nothing():
	pass


if __name__ == "__main__":
	_run_dt = datetime.now().astimezone().replace(microsecond=0)
	_run_utc = _run_dt.astimezone(timezone.utc)
	_fdate = _run_dt.strftime("%Y%m%d")
	_fdatetime = _run_dt.strftime("%Y%m%d_%H%M%S")

	__appname__ = Config.__appname__
	DEBUG = Config.DEBUG
	DRYRUN = Config.DRYRUN
	ONLINE_MODE = Config.ONLINE_MODE

	# Configure Logging
	with open(find_logging_config(), "r") as cfgfile:
		log_cfg = yaml.safe_load(cfgfile.read())
	logging.config.dictConfig(log_cfg)
	logging.basicConfig(level=logging.DEBUG)
	coloredlogs.install(fmt=log_cfg["formatters"]["simple"]["format"])
	coloredlogs.set_level(logging.DEBUG)
	logger = logging.getLogger("")

	# Configure File System Stuff
	_data_dir = Config.DATA_DIR
	_twscrape_data_dir = Config.TWSCRAPE_DATA_DIR

	# Database Stuff
	engine = create_engine(Config.SQLALCHEMY_DATABASE_URI, echo=DEBUG)
	schema = Config.DB_SCHEMA

	# Tweet-related Configuration
	_cache_days = Config.CACHE_DAYS
	_min_batch_delay, _max_batch_delay = Config.BATCH_DELAY_RANGE
	_min_error_delay, _max_error_delay = Config.ERROR_DELAY_RANGE
	_min_search_delay, _max_search_delay = Config.SEARCH_DELAY_RANGE

	# Testing Stuff
	_test_max_users = Config.TEST_MAX_USERS

	init()
	""" From Click Docs @ https://click.palletsprojects.com/en/latest/api/ :
	standalone_mode – the default behavior is to invoke the script in
	standalone mode. Click will then handle exceptions and convert them
	into error messages and the function will never return but shut down the
	interpreter. If this is set to False they will be propagated to the caller
	and the return value of this function is the return value of invoke().
	"""
	# main(standalone_mode=False)
	main()
	eoj()
