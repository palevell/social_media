#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# get_whoami.py - Wednesday, October 13, 2021

""" Get User Profile Information """
__version__ = '0.5.142-dev1'

import click, sys, traceback
import pandas as pd
import snscrape.base
import snscrape.modules.twitter as sntwitter
from datetime import datetime, timedelta, timezone
from itertools import islice
from munch import Munch
from pathlib import Path
from random import shuffle, uniform
from sqlalchemy import create_engine
from tabulate import tabulate
from time import sleep

parentdir = Path(__file__).resolve().parent.parent
if parentdir not in sys.path:
	sys.path.insert(0, str(parentdir))
from config import Config


@click.command()
@click.option("-c", "--csv/--no-csv", default=False, help="Save to CSV file")
@click.option("-d", "--database/--no-database", default=True, help="Save to database")
def main(csv, database):
	"""
	Scrapes user profile(s) from Twitter
	:param csv:
	:param database:
	:return:
	"""
	daily_whoamis = Config.DAILY_WHOAMIS
	weekly_whoamis = []
	whoami_weekday = Config.WHOAMI_WEEKDAY
	# MON, TUE, WED, THU, FRI, SAT, SUN = range(7)
	if _run_dt.weekday() == whoami_weekday:
		weekly_whoamis = Config.WEEKLY_WHOAMIS
		daily_whoamis = list(set(daily_whoamis).difference(set(weekly_whoamis)))
	whoamis = list(set(daily_whoamis + weekly_whoamis))
	shuffle(whoamis)
	max_tweets = 1
	rows = []
	tweets = []
	for count, twit in enumerate(whoamis):
		if count > 0:
			sleep(uniform(3.3333, 7.7777))
		retries = 3
		while retries > 0:
			try:
				munched_tweet = None
				# result = sntwitter.TwitterUserScraper(twit)
				i, tweet = -1, None
				for i, tweet in enumerate(sntwitter.TwitterUserScraper(twit).get_items()):
					if i == max_tweets:
						break
					munched_tweet = Munch.fromDict(tweet)
					tweets.append(tweet)
				if munched_tweet:
					print(f"%2d) @%-16s %s" % (count+1, twit, '{:9,d}'.format(munched_tweet.user.followersCount)))
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
					# Shadow-banned account?
					print(f"%2d) @%-16s No tweets" % (count+1, twit))
					do_nothing()
				retries = -1
			except snscrape.base.ScraperException as e:
				if retries == 0:
					raise
				retries -= 1
				# print(e, file=sys.stderr)
				print(traceback.print_exc(), file=sys.stderr)
				sleep(400)
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
		rows = df.to_sql(tablename, con=engine, if_exists='append', schema=schema)
	if csv:
		y = _run_dt.year
		m = _run_dt.month
		subdir = CACHE_DIR.joinpath("%d" % y, "%02d" % m)
		subdir.mkdir(parents=True, exist_ok=True)
		cfilename = subdir.joinpath("whoami_%s.csv.xz" % _fdatetime)
		df.reset_index()
		df.to_csv(cfilename, header=columns, index=False)
	return


def init():
	print("Run Start: %s" % _run_dt)
	# load_processed_files()
	return


def eoj():
	stop_dt = datetime.now().astimezone().replace(microsecond=0)
	duration = stop_dt.replace(microsecond=0) - _run_dt.replace(microsecond=0)
	print("Run Stop : %s  Duration: %s" % (stop_dt, duration))
	return


def get_last_asof():
	# What is the most recent AsOf date?
	tablename = 'dt_user_history'
	schema_tablename = "%s.%s" % (schema, tablename)
	sql = "SELECT asof FROM %s ORDER BY asof DESC LIMIT 1;" % schema_tablename
	last_asof = datetime.fromtimestamp(-1).astimezone()
	try:
		last_asof = engine.execute(sql).fetchone()[0]
	except TypeError:
		pass
	except Exception as e:
		# print("Exception: %s" % e, file=sys.stderr)
		print(traceback.print_exc(), file=sys.stderr)
		sleep(3)
	finally:
		if not last_asof:
			last_asof = datetime.fromtimestamp(-1).astimezone()
	return last_asof


def load_processed_files():
	"""
	Load consolidated data (already processed)
	:return: most recent AsOf date:
	"""
	last_asof = None
	engine = create_engine(Config.SQLALCHEMY_DATABASE_URI)
	schema = Config.DB_SCHEMA
	columns = ['user_id', 'asof', 'username', 'displayname', 'created_at',
	           'followers_count', 'friends_count', 'statuses_count',
	           'last_tweeted']  # , 'listed_count'
	tablename = 'dt_user_history'
	schema_tablename = "%s.%s" % (schema, tablename)
	filenames = []
	try:
		# What is the most recent AsOf date?
		sql = "SELECT asof FROM %s ORDER BY asof DESC LIMIT 1;" % schema_tablename
		last_asof = engine.execute(sql).fetchone()[0]
		last_mtime = last_asof.timestamp()
		# ToDo: This might work with 'db_df' as 'df', and skipping/delaying the 'set_index' step
		db_df = pd.read_sql_table(tablename, con=engine, schema=schema, columns=columns)
		# db_df.set_index(['user_id', 'asof',], inplace=True)
		filenames = [x for x in CACHE_DIR.glob('**/whoami*.csv*') if Path(x).stat().st_mtime > last_mtime]
		# latest_filename = max(filenames)
		# latest_mtime = Path(latest_filename).stat().st_mtime
		df = pd.concat((pd.read_csv(f,
			parse_dates=['created', 'created_at'],
			date_parser=lambda col: pd.to_datetime(col, utc=True),
		).assign(asof=(datetime.fromtimestamp(Path(f).stat().st_mtime).replace(microsecond=0))) for f in filenames))
		df['asof'] = pd.to_datetime(df['asof'], utc=True)
		if 'last_tweeted' in df.columns:
			df['last_tweeted'] = pd.to_datetime(df['last_tweeted'], utc=True)
		else:
			df['last_tweeted'] = None
		# Rename columns from snscrape to PostgreSQL
		df.rename(columns={
			'id':               'user_id',
			'created':          'created_at',
			'followersCount':   'followers_count',
			'friendsCount':     'friends_count',
			'listedCount':      'listed_count',
			'statusesCount':    'statuses_count',
		}, inplace=True)
		# Compare Data Frames
		if not db_df.equals(df):
			# Write to database
			print("Dataframes are different, updating database")
			diff_df = pd.merge(df, db_df, on=['user_id', 'asof',], how='outer', indicator=True)
			diff_df = diff_df.loc[diff_df['Exist'] == 'left_only']
			if not diff_df.empty:
				# Keep the left columns (x)
				diff_df.rename(columns={
					'username_x':        'username',
					'displayname_x':     'displayname',
					'created_at_x':      'created_at',
					'followers_count_x': 'followers_count',
					'friends_count_x':   'friends_count',
					'statuses_count_x':  'statuses_count',
					'last_tweeted_x':    'last_tweeted',
				}, inplace=True)
				keep_columns = ['user_id', 'asof', 'username', 'displayname', 'created_at',
				                'followers_count', 'friends_count', 'statuses_count', 'last_tweeted',]
				diff_df = diff_df[['user_id', 'asof', 'username', 'displayname', 'created_at',
				                   'followers_count', 'friends_count', 'statuses_count', 'last_tweeted',]]
				diff_df = diff_df[keep_columns]
				diff_df.set_index(['user_id', 'asof',], inplace=True)
				rows = diff_df.to_sql(tablename, con=engine, if_exists='append', schema=schema)
				print("Rows %d" % rows)
			else:
				print("Nothing to add to the database")
		else:
			print("Dataframes are the same")
	except Exception as e:
		if len(filenames) > 0:
			print("Exception: %s" % e, file=sys.stderr)
	return last_asof


def do_nothing():
	pass


if __name__ == '__main__':
	_run_dt = datetime.now().astimezone().replace(microsecond=0)
	_run_utc = _run_dt.astimezone(timezone.utc).replace(tzinfo=None)
	_fdate = _run_dt.strftime("%Y-%m-%d")
	_fdatetime = _run_dt.strftime("%Y%m%d_%H%M%S")

	DEBUG = Config.DEBUG
	DRYRUN = Config.DEBUG

	# File system stuff
	CACHE_DIR = Path(Config.CACHE_DIR)

	# Dabase stuff
	engine = create_engine(Config.SQLALCHEMY_DATABASE_URI)
	schema = Config.DB_SCHEMA

	try:
		init()
		main()
		eoj()
	except UserWarning as e:
		print(e)


"""
	all_df = pd.DataFrame()
	try:
		consolidated_mtime = -1
		consolidated_filename = max(natsorted(CACHE_DIR.glob('**/consolidated_whoami.csv*')))
		if Path(consolidated_filename).exists():
			consolidated_mtime = Path(consolidated_filename).stat().st_mtime
			consolidated_dt = datetime.fromtimestamp(consolidated_mtime)
			# all_df = pd.read_csv(consolidated_filename, index_col='asof', parse_dates=['asof'])
			all_df = pd.read_csv(consolidated_filename, index_col='asof', parse_dates=['asof',], date_parser=lambda col: pd.to_datetime(col, utc=True))
			# all_df.index.tz_localize(None)
	except Exception as e:
		print("Exception: %s" % e, file=sys.stderr)
	# all_df['asof'] = pd.to_datetime(all_df['asof'], utc=True)
	# all_df['asof'].dt.tz_localize(None)
	# all_df['asof'].dt.tz_convert(None)
"""


"""
(
{'user_id': 8835382, 'asof': datetime.datetime(2022, 2, 6, 8, 16, 49, tzinfo=datetime.timezone(datetime.timedelta(days=-1, seconds=68400), 'EST')), 'username': 'palevell2', 'displayname': 'Patrick of Truckistan🚛', 'created_at': datetime.datetime(2007, 9, 12, 15, 32, 19, tzinfo=datetime.timezone.utc), 'followers_count': 2972, 'friends_count': 2841, 'statuses_count': 114386, 'last_tweeted': datetime.datetime(2022, 2, 6, 13, 11, 57, tzinfo=datetime.timezone.utc)}, 
{'user_id': 23540244, 'asof': datetime.datetime(2022, 2, 6, 8, 16, 49, tzinfo=datetime.timezone(datetime.timedelta(days=-1, seconds=68400), 'EST')), 'username': 'randyhillier', 'displayname': 'Randy Hillier', 'created_at': datetime.datetime(2009, 3, 10, 1, 51, 11, tzinfo=datetime.timezone.utc), 'followers_count': 53167, 'friends_count': 2002, 'statuses_count': 22143, 'last_tweeted': datetime.datetime(2022, 2, 5, 23, 2, 20, tzinfo=datetime.timezone.utc)}, 
{'user_id': 242827267, 'asof': datetime.datetime(2022, 2, 6, 8, 16, 49, tzinfo=datetime.timezone(datetime.timedelta(days=-1, seconds=68400), 'EST')), 'username': 'PierrePoilievre', 'displayname': 'pierrepoilievre', 'created_at': datetime.datetime(2011, 1, 25, 17, 55, 39, tzinfo=datetime.timezone.utc), 'followers_count': 261011, 'friends_count': 1206, 'statuses_count': 10435, 'last_tweeted': datetime.datetime(2022, 2, 6, 0, 9, 13, tzinfo=datetime.timezone.utc)}, 
{'user_id': 242827267, 'asof': datetime.datetime(2022, 2, 6, 8, 16, 49, tzinfo=datetime.timezone(datetime.timedelta(days=-1, seconds=68400), 'EST')), 'username': 'PierrePoilievre', 'displayname': 'pierrepoilievre', 'created_at': datetime.datetime(2011, 1, 25, 17, 55, 39, tzinfo=datetime.timezone.utc), 'followers_count': 261011, 'friends_count': 1206, 'statuses_count': 10435, 'last_tweeted': datetime.datetime(2022, 2, 6, 0, 9, 13, tzinfo=datetime.timezone.utc)}, 
{'user_id': 269105370, 'asof': datetime.datetime(2022, 2, 6, 8, 16, 49, tzinfo=datetime.timezone(datetime.timedelta(days=-1, seconds=68400), 'EST')), 'username': 'jimkarahalios', 'displayname': 'Jim Karahalios', 'created_at': datetime.datetime(2011, 3, 20, 3, 24, 27, tzinfo=datetime.timezone.utc), 'followers_count': 10445, 'friends_count': 259, 'statuses_count': 4724, 'last_tweeted': datetime.datetime(2022, 2, 6, 2, 23, 7, tzinfo=datetime.timezone.utc)}, 
{'user_id': 833988769, 'asof': datetime.datetime(2022, 2, 6, 8, 16, 49, tzinfo=datetime.timezone(datetime.timedelta(days=-1, seconds=68400), 'EST')), 'username': 'CandiceBergenMP', 'displayname': 'Candice Bergen', 'created_at': datetime.datetime(2012, 9, 19, 19, 30, 3, tzinfo=datetime.timezone.utc), 'followers_count': 89691, 'friends_count': 2399, 'statuses_count': 8276, 'last_tweeted': datetime.datetime(2022, 2, 4, 20, 42, tzinfo=datetime.timezone.utc)}, 
{'user_id': 2791988124, 'asof': datetime.datetime(2022, 2, 6, 8, 16, 49, tzinfo=datetime.timezone(datetime.timedelta(days=-1, seconds=68400), 'EST')), 'username': 'MaximeBernier', 'displayname': 'Maxime Bernier', 'created_at': datetime.datetime(2014, 9, 5, 14, 6, 45, tzinfo=datetime.timezone.utc), 'followers_count': 176689, 'friends_count': 6462, 'statuses_count': 27549, 'last_tweeted': datetime.datetime(2022, 2, 5, 19, 52, 27, tzinfo=datetime.timezone.utc)}, 
{'user_id': 1045411062932107265, 'asof': datetime.datetime(2022, 2, 6, 8, 16, 49, tzinfo=datetime.timezone(datetime.timedelta(days=-1, seconds=68400), 'EST')), 'username': 'SupportersOfPPC', 'displayname': 'Supporters of PPC', 'created_at': datetime.datetime(2018, 9, 27, 20, 33, 15, tzinfo=datetime.timezone.utc), 'followers_count': 6120, 'friends_count': 1823, 'statuses_count': 20822, 'last_tweeted': datetime.datetime(2022, 2, 3, 11, 57, 45, tzinfo=datetime.timezone.utc)}  ... displaying 10 of 13 total bound parameter sets ...  
{'user_id': 1457517228073623556, 'asof': datetime.datetime(2022, 2, 6, 8, 16, 49, tzinfo=datetime.timezone(datetime.timedelta(days=-1, seconds=68400), 'EST')), 'username': 'OFP_Official', 'displayname': 'Ontario First Party', 'created_at': datetime.datetime(2021, 11, 8, 1, 16, 14, tzinfo=datetime.timezone.utc), 'followers_count': 1944, 'friends_count': 102, 'statuses_count': 140, 'last_tweeted': datetime.datetime(2021, 12, 21, 12, 49, 38, tzinfo=datetime.timezone.utc)}, 
{'user_id': 1475939467378757641, 'asof': datetime.datetime(2022, 2, 6, 8, 16, 49, tzinfo=datetime.timezone(datetime.timedelta(days=-1, seconds=68400), 'EST')), 'username': 'londonontppc', 'displayname': 'Meme Team', 'created_at': datetime.datetime(2021, 12, 28, 21, 19, 32, tzinfo=datetime.timezone.utc), 'followers_count': 69, 'friends_count': 100, 'statuses_count': 176, 'last_tweeted': datetime.datetime(2022, 2, 4, 18, 37, 27, tzinfo=datetime.timezone.utc)}
)
"""


"""
(
{'user_id': 8835382, 'asof': datetime.datetime(2022, 2, 7, 2, 9, tzinfo=datetime.timezone(datetime.timedelta(days=-1, seconds=68400), 'EST')), 'username': 'palevell2', 'displayname': 'Patrick of Truckistan🚛', 'created_at': datetime.datetime(2007, 9, 12, 15, 32, 19, tzinfo=datetime.timezone.utc), 'followers_count': 2975, 'friends_count': 2840, 'statuses_count': 114390, 'last_tweeted': datetime.datetime(2022, 2, 6, 14, 47, 5, tzinfo=datetime.timezone.utc)}, 
{'user_id': 14260960, 'asof': datetime.datetime(2022, 2, 7, 2, 9, tzinfo=datetime.timezone(datetime.timedelta(days=-1, seconds=68400), 'EST')), 'username': 'JustinTrudeau', 'displayname': 'Justin Trudeau', 'created_at': datetime.datetime(2008, 3, 30, 21, 4, 14, tzinfo=datetime.timezone.utc), 'followers_count': 5899541, 'friends_count': 940, 'statuses_count': 38395, 'last_tweeted': datetime.datetime(2022, 2, 6, 14, 46, 58, tzinfo=datetime.timezone.utc)}, 
{'user_id': 15346695, 'asof': datetime.datetime(2022, 2, 7, 2, 9, tzinfo=datetime.timezone(datetime.timedelta(days=-1, seconds=68400), 'EST')), 'username': 'OntarioNDP', 'displayname': 'Ontario NDP', 'created_at': datetime.datetime(2008, 7, 7, 21, 25, 34, tzinfo=datetime.timezone.utc), 'followers_count': 61731, 'friends_count': 3277, 'statuses_count': 14813, 'last_tweeted': datetime.datetime(2022, 2, 6, 20, 2, 2, tzinfo=datetime.timezone.utc)}, 
{'user_id': 17073913, 'asof': datetime.datetime(2022, 2, 7, 2, 9, tzinfo=datetime.timezone(datetime.timedelta(days=-1, seconds=68400), 'EST')), 'username': 'AndreaHorwath', 'displayname': 'Andrea Horwath', 'created_at': datetime.datetime(2008, 10, 30, 20, 33, 32, tzinfo=datetime.timezone.utc), 'followers_count': 152579, 'friends_count': 1967, 'statuses_count': 8754, 'last_tweeted': datetime.datetime(2022, 2, 6, 22, 0, 30, tzinfo=datetime.timezone.utc)}, 
{'user_id': 23540244, 'asof': datetime.datetime(2022, 2, 7, 2, 9, tzinfo=datetime.timezone(datetime.timedelta(days=-1, seconds=68400), 'EST')), 'username': 'randyhillier', 'displayname': 'Randy Hillier', 'created_at': datetime.datetime(2009, 3, 10, 1, 51, 11, tzinfo=datetime.timezone.utc), 'followers_count': 53593, 'friends_count': 2000, 'statuses_count': 22165, 'last_tweeted': datetime.datetime(2022, 2, 7, 1, 38, 20, tzinfo=datetime.timezone.utc)}, 
{'user_id': 58791677, 'asof': datetime.datetime(2022, 2, 7, 2, 9, tzinfo=datetime.timezone(datetime.timedelta(days=-1, seconds=68400), 'EST')), 'username': 'OntarioPCParty', 'displayname': 'Ontario PC Party', 'created_at': datetime.datetime(2009, 7, 21, 13, 16, 42, tzinfo=datetime.timezone.utc), 'followers_count': 57958, 'friends_count': 264, 'statuses_count': 10946, 'last_tweeted': datetime.datetime(2022, 2, 1, 15, 41, 32, tzinfo=datetime.timezone.utc)}, 
{'user_id': 92759522, 'asof': datetime.datetime(2022, 2, 7, 2, 9, tzinfo=datetime.timezone(datetime.timedelta(days=-1, seconds=68400), 'EST')), 'username': 'StevenDelDuca', 'displayname': 'Steven Del Duca', 'created_at': datetime.datetime(2009, 11, 26, 14, 1, 45, tzinfo=datetime.timezone.utc), 'followers_count': 48973, 'friends_count': 15405, 'statuses_count': 11129, 'last_tweeted': datetime.datetime(2022, 2, 7, 0, 17, 10, tzinfo=datetime.timezone.utc)}, 
{'user_id': 103317643, 'asof': datetime.datetime(2022, 2, 7, 2, 9, tzinfo=datetime.timezone(datetime.timedelta(days=-1, seconds=68400), 'EST')), 'username': 'JuliusCaesar69', 'displayname': 'Patrick 🍁', 'created_at': datetime.datetime(2010, 1, 9, 17, 1, 55, tzinfo=datetime.timezone.utc), 'followers_count': 687, 'friends_count': 1279, 'statuses_count': 20755, 'last_tweeted': datetime.datetime(2021, 10, 25, 15, 11, 52, tzinfo=datetime.timezone.utc)}  ... displaying 10 of 33 total bound parameter sets ...  
{'user_id': 1457517228073623556, 'asof': datetime.datetime(2022, 2, 7, 2, 9, tzinfo=datetime.timezone(datetime.timedelta(days=-1, seconds=68400), 'EST')), 'username': 'OFP_Official', 'displayname': 'Ontario First Party', 'created_at': datetime.datetime(2021, 11, 8, 1, 16, 14, tzinfo=datetime.timezone.utc), 'followers_count': 1945, 'friends_count': 102, 'statuses_count': 140, 'last_tweeted': datetime.datetime(2021, 12, 21, 12, 49, 38, tzinfo=datetime.timezone.utc)}, 
{'user_id': 1475939467378757641, 'asof': datetime.datetime(2022, 2, 7, 2, 9, tzinfo=datetime.timezone(datetime.timedelta(days=-1, seconds=68400), 'EST')), 'username': 'londonontppc', 'displayname': 'Meme Team', 'created_at': datetime.datetime(2021, 12, 28, 21, 19, 32, tzinfo=datetime.timezone.utc), 'followers_count': 71, 'friends_count': 100, 'statuses_count': 176, 'last_tweeted': datetime.datetime(2022, 2, 4, 18, 37, 27, tzinfo=datetime.timezone.utc)})
"""
# 2809959174

