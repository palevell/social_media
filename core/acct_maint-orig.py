#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# acct_maint.py - Saturday, September 4, 2021
""" Follow retweeters and unfollow less-active tweeps """
""" ToDo: Load cached data from database """
__version__ = "0.9.107-dev1"

import builtins
import click
import coloredlogs
import json
import logging.config
import lzma
import os
import oyaml as yaml
import pid
import snscrape.base
import snscrape.modules.twitter as sntwitter
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler
from munch import Munch
from os.path import basename, exists, getmtime, join
from pathlib import Path
from random import shuffle, uniform
from tabulate import tabulate
from time import sleep

import pandas as pd
from PIL import Image, ImageDraw, ImageFont
from pid.decorator import pidfile
from sqlalchemy import create_engine, text

basedir = Path(__file__).resolve().parent.parent
if basedir not in sys.path:
	sys.path.insert(0, str(basedir))
from config import Config

__module__ = Path(__file__).resolve().stem

# Set PID Dir
PIDDIR = "/run/user/%d" % os.getegid()
if not exists(PIDDIR):
	PIDDIR = "/tmp"


@click.command()
@click.argument(
	"screen_name", default=Config.DEFAULT_TWIT
)  # , help='@Twitter handle to check')
@click.option("-c", "--csv/--no-csv", default=False, help="Save to CSV file")
@click.option("-d", "--database/--no-database", default=True, help="Save to database")
@click.option("-f", "--first-run", is_flag=True, help="First run for this user (don't cache)")
@pidfile(piddir=PIDDIR)
def main(screen_name, csv, database, first_run):
	# fn_logger = logging.getLogger("%s.main" % __module__)

	screen_name = screen_name.lstrip("@")

	# Load from Twitter data dump
	following_ids = load_friend_ids(screen_name=screen_name)
	logger.info(f"Following IDs: {len(following_ids):,d}")

	# Find User IDs already in database
	if not first_run:
		cached_user_ids = get_cached_user_ids(following_ids)
		logger.info(f"Cached User IDs: {len(cached_user_ids):,d}")
	else:
		cached_user_ids = []

	# Determine which User IDs to fetch (ie. not cached or current)
	fetch_ids = sorted(list(set(following_ids) - set(cached_user_ids)))

	# Fetch profiles for User IDs
	line_number = 0
	batch_size = Config.BATCH_SIZE
	batches = (fetch_ids[i:i + batch_size] for i in range(0, len(fetch_ids), batch_size))
	for batch_count, batch in enumerate(batches):
		if batch_count > 0:
			snoozer(MIN_BATCH_DELAY, MAX_BATCH_DELAY)
		following_users = get_users(batch, csv, database, line_number)
		line_number += batch_size
		do_nothing()
	return


def init():
	global cache_dir
	msg = "%s %s Run Start: %s" % (
		__module__,
		__version__,
		_run_dt.replace(microsecond=0),
	)
	if DRYRUN:
		msg += " (DRY RUN)"
	logging.info(msg)
	"""if Config.DB_REBUILD:
		tablenames = ['dt_account',
					  'dt_candidate',
					  'dt_pruned',
					  'dt_user',
					  'dt_user_friend',
		]
		for tablename in tablenames:
			if schema:
				tablename = schema + '.' + tablename
			sql = "DROP TABLE IF EXISTS %s;" % tablename
			try:
				engine.execute(sql)
			except Exception as e:
				logging.exception("Exception: %s" % e)"""
	return


def eoj():
	stop_dt = datetime.now().astimezone().replace(microsecond=0)
	duration = stop_dt.replace(microsecond=0) - _run_dt.replace(microsecond=0)
	logging.info("Run Stop : %s  Duration: %s" % (stop_dt, duration))
	return


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
	tablename = 'dt_user_history'
	with engine.connect() as conn:
		if schema:
			setschema = f"SET search_path TO {schema},public;"
			conn.execute(text(setschema))
		# Columns: ['user_id', 'asof', 'screen_name', 'name', 'created_at', 'default_profile_image', 'protected', 'followers_count', 'friends_count', 'listed_count', 'statuses_count', 'last_tweet']
		where_conditions = ["user_id IN (%s)" % stringify(user_ids),
			"asof > '%s'" % (_run_dt.replace(microsecond=0) - timedelta(hours=24)),
		]
		where_clause = ' AND '.join(where_conditions)
		orderby_clause = "user_id"

		sql = "SELECT DISTINCT * FROM %s WHERE %s ORDER BY %s LIMIT 100000;" \
		      % (tablename, where_clause, orderby_clause)
		print(sql)
		rows = conn.execute(text(sql))
		row_count = rows.rowcount
		print("Row Count: {:,d}".format(row_count))
		# df = pd.DataFrame(rows, columns=rows.keys())
	return pd.DataFrame(rows, columns=rows.keys())


# ToDo: Should this be using dt_user_history?
def get_cached_user_ids(user_ids):
	user_ids = sorted(user_ids)
	cached_user_ids = []
	with engine.connect() as conn:
		tablename = "dt_user_history"
		if schema:
			setschema = f"SET search_path TO {schema},public;"
			conn.execute(text(setschema))
		params = {"yesterday": (_run_dt.replace(microsecond=0) - timedelta(days=7))}
		# Columns: ['user_id', 'asof', 'screen_name', 'name', 'created_at', 'default_profile_image', 'protected', 'followers_count', 'friends_count', 'listed_count', 'statuses_count', 'last_tweet']
		where_conditions = [
			"asof > :yesterday",
		]
		where_clause = " AND ".join(where_conditions)
		orderby_clause = "user_id"
		sql = f"SELECT DISTINCT user_id FROM {tablename} WHERE {where_clause} ORDER BY {orderby_clause};"
		logger.info(sql)
		for row in conn.execute(text(sql), params).fetchall():
			cached_user_ids.append(row[0])
		row_count = len(cached_user_ids)
		# df = pd.DataFrame(rows, columns=rows.keys())
	return cached_user_ids


def get_follower_ids(user_id=None, screen_name=None):
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
	if id_type in ["friend", "following"]:
		filename = data_dir / f"{screen_name}_following.js"
	elif id_type == "follower":
		pass
	elif id_type == "blocked":
		pass
	elif id_type == "muted":
		pass
	else:
		# This shouldn't happen
		raise ValueError(
			"Invalid id_type.  Valid types: blocked, follower, friend, or muted"
		)
	friendly_id_type = str(id_type).capitalize()
	# fn_logger = logging.getLogger("%s.get_%s_ids" % (__module__, id_type))
	logger.info(f"Filename: {filename}")
	ids = []
	id_key = "accountId"
	if exists(filename) :  # and _run_dt.timestamp() - getmtime(filename) < 12 * 60**2:
		ids = load_ids_file(id_type, id_key, filename)
	if not ids:
		logger.debug(
			"Fetching %s IDs for @%s . . ." % (friendly_id_type, screen_name)
		)
	return ids


def get_users(user_ids, csv, database, lineno=0):
	user_ids = sorted(user_ids)
	# shuffle(user_ids)
	max_tweets = 1
	rows = []
	tweets = []
	idlers = []
	for count, user_id in enumerate(user_ids):
		if count > 0:
			sleep(uniform(3.3333, 7.7777))
		retries = 3
		while retries > 0:
			flag_list = []
			munched_tweet = None
			i, tweet = -1, None
			try:
				for i, tweet in enumerate(sntwitter.TwitterUserScraper(user_id).get_items()):
					if i == max_tweets:
						break
					munched_tweet = Munch.fromDict(tweet)
					tweets.append(tweet)
				if munched_tweet:
					username = munched_tweet.user.username
					followers_count = munched_tweet.user.followersCount
					if followers_count < 100:
						flag_list.append("100")
					elif followers_count < 1000:
						flag_list.append("1000")
					tweet_date = munched_tweet.date
					if (_run_dt - tweet_date).days > 365:
						flag_list.append("IDLE")
						idlers.append(username)

					flags = ",".join(flag_list)
					msg = " ".join([
						f"{lineno+count+1:5d})",
						f"{user_id:19d}",
						f"{username:16s}",
						f"{followers_count:12,d}",
						f"{tweet_date}",
						f"{flags}",
					])
					# msg = f"{count+1:5d}) {user_id:19d} {username:16s} {followers_count:9,d} {tweet_date}"
					logger.info(msg)
					# print(f"%2d) @%-16s %s" % (count+1, munched_tweet.user.username, '{:9,d}'.format(munched_tweet.user.followersCount)))
					row = (int(munched_tweet.user.id),
						_run_dt,
						munched_tweet.user.username,
						munched_tweet.user.displayname,
						munched_tweet.user.created,
						munched_tweet.user.followersCount,
						munched_tweet.user.friendsCount,
						munched_tweet.user.statusesCount,
						munched_tweet.date,
					)
					rows.append(row)
				else:
					# Protected or shadow-banned account
					msg = " ".join([
						f"{lineno+count+1:5d})",
						f"{user_id:19d}",
						f"No tweets",
						f"https://twitter.com/intent/user?user_id={user_id}",
					])
					logger.warning(msg)
					do_nothing()
				retries = -1
			except snscrape.base.ScraperException as e:
				retries -= 1
				if retries == 0:
					continue
				else:
					logger.warning(f"User ID: {user_id}: {e}  Retries: {retries}")
					snoozer(395,405)
	columns = ['user_id', 'asof', 'username', 'displayname', 'created_at',
	           'followers_count', 'friends_count', 'statuses_count',
	           'last_tweeted']  # , 'listed_count'
	df = pd.DataFrame(sorted(rows), columns=columns)
	df.set_index(['user_id', 'asof',], inplace=True)
	if DEBUG:
		print(df.columns)
		print(tabulate(df))
		do_nothing()
	if database:
		tablename = 'dt_user_history'
		db_rows = df.to_sql(tablename, con=engine, if_exists='append', schema=schema)
	if csv:
		subdir = cache_dir / f"{_run_dt.year}" / f"{_run_dt.month:02d}"
		subdir.mkdir(parents=True, exist_ok=True)
		cfilename = subdir.joinpath("whoami_%s.csv.xz" % _fdatetime)
		df.reset_index()
		df.to_csv(cfilename, header=columns, index=False)
	if idlers:
		logger.info("*** Idlers ***")
		for i, idler in enumerate(idlers):
			logger.info(f"{i+1:3d} {idler}")
	return rows


def idle(last_tweeted):
	"""
	Calculate idle time for a user account
	:param last_tweeted: date of user's last tweet
	:return: date diffence between run time and last tweet (timedelta object)
	"""
	if not last_tweeted.tzinfo:
		last_tweeted.replace(tzinfo=timezone.utc)
	return _run_utc - last_tweeted


def load_ids_file(id_type, id_key, filename):
	ids = []
	with open(filename) as infile:
		data = json.load(infile)
	do_nothing()
	for item in data:
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
	# LOG_LEVEL = Config.LOG_LEVEL

	# Configure Logging
	with open(find_logging_config(), "r") as cfgfile:
		log_cfg = yaml.safe_load(cfgfile.read())
	logging.config.dictConfig(log_cfg)
	coloredlogs.install(fmt=log_cfg["formatters"]["simple"]["format"])
	logger = logging.getLogger("")
	# logger.setLevel(LOG_LEVEL)
	# fn_logger = logging.getLogger(__module__)


	# Configure logging (OLD)
	"""FILENAME_SUFFIX = Config.FILENAME_SUFFIX
	if not FILENAME_SUFFIX:
		fname = "%s.log" % __module__
	else:
		fname = "%s-%s.log" % (__module__, FILENAME_SUFFIX)
	logfilename = join(Config.LOG_DIR, fname)
	file_handler = RotatingFileHandler(
		logfilename, maxBytes=9 * 1024**2, backupCount=9
	)
	logging.basicConfig(
		level=LOG_LEVEL,
		format="%(asctime)s %(name)-30s %(levelname)-8s %(message)s",
		datefmt="%Y-%m-%d %H:%M:%S%z",
		handlers=[
			file_handler,
		],
	)
	# define a Handler which writes INFO messages or higher to the sys.stderr
	console = logging.StreamHandler()
	console.setLevel(logging.DEBUG)
	# set a format which is simpler for console use
	formatter = logging.Formatter("%(name)-28s: %(levelname)-8s %(message)s")
	# tell the handler to use this format
	console.setFormatter(formatter)
	# add the handler to the root logger
	logging.getLogger("").addHandler(console)"""

	# Configure File System Stuff
	cache_dir = Config.CACHE_DIR
	data_dir = Config.DATA_DIR
	report_dir = Config.REPORT_DIR
	acct_cache = Config.ACCT_CACHE
	user_cache = Config.USER_CACHE

	# Database Stuff
	DATABASE_URL = Config.DATABASE_URL
	if DATABASE_URL:
		# Connect to database
		engine = create_engine(Config.DATABASE_URL, echo=False)
		schema = Config.DB_SCHEMA

	# Direct Message Recipient (ie. me)
	DM_RECIPIENT_ID = Config.DM_RECIPIENT_ID

	# Tweet-related Configuration
	MIN_BATCH_DELAY,  MAX_BATCH_DELAY =  Config.BATCH_DELAY_RANGE
	MIN_SEARCH_DELAY, MAX_SEARCH_DELAY = Config.SEARCH_DELAY_RANGE

	NEW_FRIEND_LIMIT = Config.NEW_FRIEND_LIMIT
	MIN_LISTED_COUNT = Config.MIN_LISTED_COUNT
	MIN_STATUS_COUNT = Config.MIN_STATUS_COUNT

	init()
	main(standalone_mode=False)
	eoj()
