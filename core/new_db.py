#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# new_db.py - Wednesday, June 21, 2023
""" Testing new database with SQitch """
__version__ = "0.1.1-dev2"

import os
import sqlalchemy as sa
import sys
import time
from datetime import datetime, timedelta
from glob import glob
from os.path import exists, getmtime, join, lexists, realpath
from pathlib import Path

# from psycopg.extras import DictCursor, Json, execute_values

basedir = Path(__file__).resolve().parent.parent
if basedir not in sys.path:
	sys.path.insert(0, str(basedir))
from config import Config

__module__ = Path(__file__).resolve().stem


def main():

	return


def init():
	started = time.strftime(_iso_datefmt, _run_localtime)
	print(f"Run Start: {__module__} v{__version__} {started}")
	return


def eoj():
	stop_ts = time.time()
	stop_localtime = time.localtime(stop_ts)
	stop_gmtime = time.gmtime(stop_ts)
	duration = timedelta(seconds=(stop_ts - _run_ts))
	print(
		f"Run Stop : {time.strftime(_iso_datefmt, stop_localtime)}  Duration: {duration}"
	)
	return


def do_nothing():
	pass


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
):
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
		conn.execute(sa.text(setschema))
		result = conn.execute(sa.text(sql), params_dict)
		# print(f"Row count: {result.rowcount}")
		# for row in result.fetchall():
		# 	print(row)
		do_nothing()

	return


def migrate_ppc_retweets():
	max_rows = 10000
	with engine.begin() as to_conn, old_engine.begin() as from_conn:
		setschema = f"SET search_path TO {schema},public;"
		for conn in [from_conn, to_conn]:
			conn.execute(sa.text(setschema))
		"""sql = "TRUNCATE TABLE dt_user_history CASCADE;"
		to_conn.execute(sa.text(sql))
		sql = "TRUNCATE TABLE dt_user CASCADE;"
		to_conn.execute(sa.text(sql))"""
		# Get column names
		# sql = "SELECT * FROM dt_user_history ORDER BY id LIMIT 0;"
		# columns = from_conn.execute(sa.text(sql)).keys()
		from_cols_list = [
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
		from_cols = ",".join(from_cols_list)
		params_list = ",".join([f":{x}" for x in from_cols_list])
		print(f"user_id  asof  username  created_at  followers_count  last_tweeted")
		from_sql = f"SELECT * FROM dt_user_history ORDER BY id LIMIT {max_rows};"
		media_count = None
		for (
			hist_id,
			user_id,
			asof,
			username,
			displayname,
			created_at,
			followers_count,
			friends_count,
			listed_count,
			statuses_count,
			last_tweeted,
		) in from_conn.execute(sa.text(from_sql)).fetchall():
			print(
				f"{hist_id:19d} {asof} @{username:16s} {created_at} {followers_count:12,d} {last_tweeted}"
			)
			# Note: Parameter names match column names (above)
			params_dict = {
				"in_user_id": user_id,
				"asof": asof,
				"username": username,
				"displayname": displayname,
				"created_at": created_at,
				"followers_count": followers_count,
				"friends_count": friends_count,
				"listed_count": listed_count,
				"statuses_count": statuses_count,
				"last_tweeted": last_tweeted,
			}
			# params_list = ",".join([f":{x}" for x in params_dict.keys()])
			to_sql = f"INSERT INTO dt_user_history ({from_cols}) VALUES ({params_list});"
			# result = to_conn.execute(sa.text(to_sql), params_dict)
			insert_user(
				user_id,
				asof,
				username,
				displayname,
				created_at,
				followers_count,
				friends_count,
				listed_count,
				media_count,
				statuses_count,
				last_tweeted,
			)
			do_nothing()
		# to_conn.commit()
		# sql = "ALTER TABLE dt_user ENABLE TRIGGER trb_user_asof;"
		# to_conn.execute(sa.text(sql))

	return


if __name__ == "__main__":
	_run_ts = time.time()
	_run_dt = datetime.fromtimestamp(_run_ts).astimezone()
	_run_localtime = time.localtime(_run_ts)
	_run_gmtime = time.gmtime(_run_ts)
	_run_ymd = time.strftime("%Y%m%d", _run_localtime)
	_run_hms = time.strftime("%H%M%S", _run_localtime)
	_run_ymdhms = f"{_run_ymd}_{_run_hms}"
	_iso_datefmt = "%Y-%m-%d %H:%M:%S%z"

	# Database
	engine = sa.create_engine(Config.SQLALCHEMY_DATABASE_URI, echo=False)
	schema = Config.DB_SCHEMA
	old_engine = sa.create_engine(Config.OLD_SQLALCHEMY_DATABASE_URI, echo=False)
	old_schema = Config.OLD_DB_SCHEMA

	init()
	# main()
	migrate_ppc_retweets()
	eoj()
