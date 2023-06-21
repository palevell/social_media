#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# acct_maint.py - Saturday, September 4, 2021
""" Follow retweeters and unfollow less-active tweeps """
__version__ = '0.8.89'

import builtins, click, logging, os, lzma, pid, sys, tweepy
import pandas as pd
from collections import Counter
from datetime import datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler, SMTPHandler
from os.path import basename, exists, expanduser, getmtime, join, splitext
from pid.decorator import pidfile
from PIL import Image, ImageDraw, ImageFont
from random import shuffle, uniform
from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import Session
from time import sleep

from config import Config
from models import RcFile, User

__MODULE__ = os.path.splitext(os.path.basename(__file__))[0]

# Set PID Dir
PIDDIR = '/run/user/%d' % os.getegid()
if not exists(PIDDIR):
	PIDDIR = '/tmp'


@click.command()
@click.argument('screen_name', default=Config.DEFAULT_TWIT)  # , help='@Twitter handle to check')
@click.option('-c', '--common-friends', is_flag=True, help='process friends in common')
@click.option('-f', '--follow-retweeters', is_flag=True, help='follow retweeters')
@click.option('-u', '--unfollow-idlers', is_flag=True, help='unfollow inactive/idle accounts')
@click.option('-i', '--unfollow-no-img', is_flag=True, help='unfollow accounts with default profile image')
@pidfile(piddir=PIDDIR)
def main(screen_name, common_friends, follow_retweeters, unfollow_idlers, unfollow_no_img):
	"""
	Finds retweeters to follow and purges inactive users
	:param screen_name: Twitter user profile to process
	:param follow_retweeters: follow qualified retweeters
	:param unfollow_idlers: prune idle/inactive friends
	:param unfollow_no_img: prune idle/inactive friends
	:return: None
	"""
	global acct_cache, api, report_dir, tacct, tacctid, user_cache

	fn_logger = logging.getLogger("%s.main" % __MODULE__)

	screen_name = screen_name.lstrip('@')
	if DRYRUN:
		follow_retweeters = False
		unfollow_idlers = False
		unfollow_no_img = False
	fn_logger.debug('get_api(%s)' % screen_name)
	api = get_api(screen_name)

	fn_logger.debug("Fetching API settings . . .")
	api_settings = api.get_settings()
	tacct = api_settings['screen_name']
	fn_logger.debug("Fetching User object for @%s" % tacct)
	me = api.get_user(screen_name=tacct)
	tacctid = me.id
	if tacct == screen_name:
		fn_logger.info("*** TACCT: @%s  (ID: %d) ***" % (tacct, tacctid))
		acct_cache = join(acct_cache, tacct)
		os.makedirs(acct_cache, exist_ok=True)
		report_dir = join(report_dir, "Accounts", tacct)
		os.makedirs(report_dir, exist_ok=True)
		last_tweet = me.status.created_at
		last_tweet_hrs = Config.LAST_TWEET_HRS
		last_tweet_seconds = last_tweet_hrs * 60 * 60
		if (_run_utc - last_tweet).seconds > last_tweet_seconds:
			raise UserWarning("@%s hasn't tweeted since %s (more than %d hours ago)" % (tacct, last_tweet, last_tweet_hrs))
	else:
		fn_logger.debug("Fetching User object for @%s" % screen_name)
		user_cache = join(user_cache, screen_name)
		os.makedirs(user_cache, exist_ok=True)
		report_dir = join(report_dir, "Users", screen_name)
		os.makedirs(report_dir, exist_ok=True)
		query_user = api.get_user(screen_name=screen_name)
		if hasattr(query_user, 'status'):
			last_tweet = query_user.status.created_at
		else:
			fn_logger.warning("Do we want to process this user profile?")
		follow_retweeters = False
		unfollow_idlers = False
		unfollow_no_img = False
		fn_logger.info("*** @%s *** TACCT: @%s  (ID: %d) ***" % (screen_name, tacct, tacctid))
	fn_logger.info("FIND_COMMON: %s  FIND_NEW: %s  IDLERS: %s  NOIMG: %s" % (common_friends, follow_retweeters, unfollow_idlers, unfollow_no_img))
	friend_ids = get_friend_ids(screen_name=screen_name)
	# follower_ids = get_follower_ids(screen_name=screen_name)
	# new_friends = find_new_friends(screen_name, friend_ids, follow)
	if tacct == screen_name:
		if (_run_utc - last_tweet).days < 7:
			new_friends = find_new_friends(screen_name, friend_ids, follow_retweeters)
			report_title = "@%s New Friends: (%d)" % (screen_name, len(new_friends))
			if not follow_retweeters:
				report_title += " (DRY RUN)"
			reporter(report_title, screen_name, new_friends, columnar=True)
		else:
			fn_logger.info("Skipping search for new friends since last tweet was more than a week ago")
	pruned_users = prune_idlers(screen_name, friend_ids, common_friends, unfollow_idlers, unfollow_no_img)
	report_title = "@%s Pruned (%d)" % (screen_name, len(pruned_users))
	if not unfollow_idlers:
		report_title += " (DRY RUN)"
	reporter(report_title, screen_name, pruned_users, columnar=True)
	return


def init():
	global cache_dir
	msg = "%s %s Run Start: %s" % (__MODULE__, __version__, _run_dt.replace(microsecond=0))
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


def find_new_friends(screen_name, exisisting_friend_ids, make_changes):
	"""
	follow qualified retweeters
	:param existing_follower_ids: existing find_new_friends IDs
	:param make_changes: actually follow new friends
	:return: new_friends: qualified retweeters
	"""
	# Create function logger
	fn_logger = logging.getLogger(__MODULE__ + '.find_new_friends')
	# Load configuration
	min_acct_age = Config.MIN_ACCT_AGE
	min_followers = Config.MIN_FOLLOWERS
	# Declare function-level variables
	columns = [
		"user_id",
		"screen_name",
		"account_age",
		"follower_count",
		"idle_days",
		"listed_count",
		"no_avatar",
		"protected",
		"status_count",
	]
	# flagged_retweeters = []
	flagged_rows = []
	# json_rows = []
	# new_friends = []
	qualified_retweeters = []
	new_retweeter_ids = []

	last_week_dt = _run_utc - timedelta(days=7)
	# Collect recent tweets for this account that have been retweeted
	fn_logger.debug("Fetching retweets of me (@%s) . . ." % tacct)
	# Max count value: 100
	# Rate limit: 75 calls / 15-minutes
	retweets_of_me = api.get_retweets_of_me(count=100, trim_user=True)
	if retweets_of_me:
		# Assign earliest tweet ID to max_id, for the next API call
		max_id = min([x.id for x in retweets_of_me])
		# Find earliest tweet date
		earliest_tweet_date = min([x.created_at for x in retweets_of_me])
		# Find more retweets of me if we have less than a week's worth of tweets
		while earliest_tweet_date > last_week_dt:
			snoozer(MIN_BATCH_DELAY, MAX_BATCH_DELAY)
			fn_logger.debug('Fetching more retweets of @%s: %d so far (max_id=%d) . . .' % (tacct, len(retweets_of_me), max_id))
			page = api.get_retweets_of_me(count=100, max_id=max_id, trim_user=True)
			if page:
				retweets_of_me.extend(page)
				max_id = min([x.id for x in page])
				earliest_tweet_date = min([x.created_at for x in page])
	fn_logger.debug('Fetched %d retweets of @%s since %s' % (len(retweets_of_me), tacct, last_week_dt))
	# Get Retweeter IDs for collected tweets
	rt_count = 0
	for rt in retweets_of_me:
		rt_count += 1
		retries = 3
		while retries > 0:
			next_cursor = -1
			try:
				while True:
					snoozer(MIN_SEARCH_DELAY, MAX_SEARCH_DELAY)
					# The next_cursor functionality doesn't seem to be working, at the Twitter API level
					fn_logger.debug("%3d) Fetching Retweeter IDs for tweet %d (cursor=%d) . . ." % (rt_count, rt.id, next_cursor))
					ids, cursors = api.get_retweeter_ids(rt.id, cursor=next_cursor)
					# Extract retweeters that I don't follow
					new_retweeter_ids.extend([x for x in ids if x != tacctid and x not in exisisting_friend_ids])
					next_cursor = cursors[0]
					"""msg = "%3d) %d  %s  IDs: %3d" % (rt_count, rt.id, rt.created_at.strftime("%Y-%m-%d"), len(ids))
					if len(ids) != rt.retweet_count:
						msg = "%s  Retweets: %4d" % (msg, rt.retweet_count)
					if cursors[0] + cursors[1] > 0:
						msg = "%s  %s" % (msg, cursors)
						do_nothing()
					fn_logger.info(msg)"""
					if next_cursor == 0:
						break
				retries = -1
			except Exception as e:
				retries -= 1
				fn_logger.exception("Exception: %s" % e)
				sleep(30)
	new_friends = []
	new_friend_users = []
	if new_retweeter_ids:
		fn_logger.info("@%s New Retweeter IDs: %d" % (tacct, len(new_retweeter_ids)))
		candidate_ids = []
		# Find Retweeters who have retweeted more than once (These are candidates for following)
		for item, count in Counter(new_retweeter_ids).most_common():
			if count == 1:
				# Only follow tweeps who have RT'd more than once
				break
			candidate_ids.append(item)
		fn_logger.info("@%s Candidates: %d" % (tacct, len(candidate_ids)))
		
		#  Process list of candidates for following
		loop_count = 0
		users = get_users(candidate_ids, 'candidate')
		for user in users:
			loop_count += 1
			if loop_count > 1:
				snoozer(MIN_SEARCH_DELAY, MAX_SEARCH_DELAY)
			flags = []
			if user.followers_count < min_followers:
				flags.append('FLWRS')
			if (_run_utc - user.created_at).days < min_acct_age:
				flags.append('NEWBI')
			if user.default_profile_image:
				flags.append('NOIMG')
			if user.listed_count < MIN_LISTED_COUNT:
				flags.append('LISTS')
			if user.protected:
				flags.append('PROTECTED')
			if user.statuses_count < MIN_STATUS_COUNT:
				flags.append('TWEETS')
			if flags:
				flagged_row = [ user.id, _run_dt, user.screen_name, ','.join(sorted(flags)), ]
				# newbie = [ user.id, user.screen_name, ','.join(sorted(flags)), ]
				# json_row = {'screen_name': user.screen_name, 'user_id': user.id, 'flags': ','.join(flags)}
				# flagged_retweeters.append(newbie)
				flagged_rows.append(flagged_row)
				# json_rows.append(json_row)
				fn_logger.debug("%3d) @%-16s %20d %s" % (loop_count, user.screen_name, user.id, ','.join(flags)))
			else:
				qualified = [ user.id, user.screen_name, ]
				qualified_retweeters.append(qualified)
				fn_logger.debug("%3d) @%-16s %20d QUALIFD" % (loop_count, user.screen_name, user.id))
		if flagged_rows:
			fn_logger.info("@%s Flagged Retweeters: %d" % (tacct, len(flagged_rows)))
			columns = ['user_id', 'asof', 'screen_name', 'flags']
			df = pd.DataFrame(sorted(flagged_rows), columns=columns)
			# Save as CSV
			filename = join(acct_cache, "%s_flagged_%s.csv.xz" % (tacct, _fdatetime))
			fn_logger.debug("Saving flagged users to '%s'" % filename)
			df.to_csv(filename, header=columns, index=False)
			# Save to Database
			tablename = 'dt_candidate'
			# fn_logger.debug("Adding flagged users to '%s'" % tablename)
			# df.to_sql(tablename, con=engine, if_exists='append', index=False, schema=schema)
			# ToDo: Merge flagged_rows & flagged_retweeters
			lines = []
			line_count = 0
			for tid, asof, name, flags in flagged_rows:
				line_count += 1
				line = "%3d) @%-16s %s" % (line_count, name, flags)
				lines.append(line)
			report_title = "@%s Flagged: (%d)" % (screen_name, len(flagged_rows))
			reporter(report_title, screen_name, lines)
		# Follow Qualified Retweeters
		if qualified_retweeters:
			fn_logger.info("@%s Qualified Retweeters: %d" % (tacct, len(qualified_retweeters)))
			# Save as CSV
			cfilename = join(acct_cache, "%s_qualified_%s.csv" % (tacct, _fdatetime))
			fn_logger.debug("Saving to '%s'" % cfilename)
			with open(cfilename, 'w') as cfile:
				for user_id, screen_name in qualified_retweeters:
					cfile.write("%s,%d\n" % (screen_name, user_id))
			for user_id, screen_name in qualified_retweeters:
				retries = 3
				while retries > 0:
					try:
						if not DRYRUN and make_changes:
							new_count = len(new_friends) + 1
							if len(new_friends) > 1:
								snoozer(MIN_FOLLOW_DELAY, MAX_FOLLOW_DELAY)
							fn_logger.debug("%3d) Now Following @%-16s (%d)" % (new_count, screen_name, user_id))  # ToDo: Add count
							new_friend = api.create_friendship(user_id=user_id)
						new_friends.append(screen_name)
						if len(new_friends) == NEW_FRIEND_LIMIT:
							fn_logger.info("New friend limit reached.  Exiting loop")
							break
						retries = -1
					except tweepy.errors.HTTPException as e:
						retries -= 1
						fn_logger.exception("HTTPException: @%s %s Retries: %d" % (screen_name, e, retries))
						if 326 in e.api_codes:
							break
						sleep(30)
	if new_friends:
		fn_logger.info("@%s New Friends: %d" % (tacct, len(new_friends)))
		# Save as text
		tfilename = join(acct_cache, "%s_new_friends_%s.txt" % (tacct, _fdatetime))
		fn_logger.debug("Saving to '%s'" % tfilename)
		with open(tfilename, 'w') as tfile:
			tfile.writelines(x+'\n' for x in new_friends)
		for i in range(0, len(new_friends), 3):
			msg = ''
			for j in range(3):
				msg += "%3d) @%-18s" % (i+j+1, new_friends[i+j])
				if i+j+1 == len(new_friends):
					break
			fn_logger.info(msg)
	return new_friends


def get_api(screen_name):
	api = credentials = None
	if Config.TRCFILE:
		# credentials = get_trcentry(Config.TRCFILE, screen_name)
		trc = RcFile(Config.TRCFILE)
		if screen_name in trc.profiles:
			credentials = trc.profile(screen_name)
		else:
			credentials = trc.profile(Config.DEFAULT_TWIT)
	elif Config.TWURLRCFILE:
		pass
	elif Config.TWITTER_ENVS:
		pass
	if credentials:
		auth = tweepy.OAuthHandler(credentials.consumer_key, credentials.consumer_secret)
		auth.set_access_token(credentials.access_token, credentials.access_secret)
		api = tweepy.API(auth, wait_on_rate_limit=True,)
	return api


def get_cached_users(user_ids):
	user_ids = sorted(user_ids)
	with engine.connect() as connection:
		tablename = 'dt_user'
		if schema:
			tablename = schema + '.' + tablename
		# Columns: ['user_id', 'asof', 'screen_name', 'name', 'created_at', 'default_profile_image', 'protected', 'followers_count', 'friends_count', 'listed_count', 'statuses_count', 'last_tweet']
		where_conditions = ["user_id IN (%s)" % stringify(user_ids),
			"asof > '%s'" % (_run_dt.replace(microsecond=0) - timedelta(hours=24)),
		]
		where_clause = ' AND '.join(where_conditions)
		orderby_clause = "user_id"

		sql = "SELECT DISTINCT * FROM %s WHERE %s ORDER BY %s LIMIT 100000;" \
		      % (tablename, where_clause, orderby_clause)
		print(sql)
		rows = connection.execute(text(sql))
		row_count = rows.rowcount
		print("Row Count: {:,d}".format(row_count))
		# df = pd.DataFrame(rows, columns=rows.keys())
	return pd.DataFrame(rows, columns=rows.keys())


def get_follower_ids(user_id=None, screen_name=None, max_pages=None):
	return get_ids(id_type='follower', user_id=user_id, screen_name=screen_name, max_pages=max_pages)


def get_friend_ids(user_id=None, screen_name=None, max_pages=None):
	return get_ids(id_type='friend', user_id=user_id, screen_name=screen_name, max_pages=max_pages)


def get_ids(id_type, user_id=None, screen_name=None, max_pages=None):
	"""
	Wrapper for Tweepy functions that return IDs and work with tweepy.Cursor
	ie. api.get_%s_ids(); api.get_retweeter_ids doesn't work with tweepy.Cursor
	:param id_type: one of blocked, follower, friend, or muted
	:param id: Twitter User ID to query
	:param screen_name: Twitter screen_name to query
	:return: ids
	"""
	if id_type not in ['blocked', 'follower', 'friend', 'muted',]:
		raise ValueError("Invalid id_type.  Valid types: blocked, follower, friend, or muted")
	if id_type == 'friend':
		api_fn = api.get_friend_ids
	elif id_type == 'follower':
		api_fn = api.get_follower_ids
	elif id_type == 'blocked':
		api_fn = api.get_blocked_ids
	elif id_type == 'muted':
		api_fn = api.get_muted_ids
	else:
		# This shouldn't happen
		raise ValueError("Invalid id_type.  Valid types: blocked, follower, friend, or muted")
	friendly_id_type = str(id_type).capitalize()
	fn_logger = logging.getLogger("%s.get_%s_ids" % (__MODULE__, id_type))
	query_user = None
	# if user_id is provided, get screen_name
	if user_id is None and screen_name is None:
		screen_name = tacct
		user_id = tacctid
	elif user_id is None and screen_name == tacct:
		user_id = tacctid
	elif user_id == tacctid and screen_name is None:
		screen_name = tacct
	elif not screen_name:
		fn_logger.debug("Fetching screen_name for UserID %d" % user_id)
		query_user = api.get_user(user_id=user_id)
		screen_name = query_user.screen_name
	elif not user_id:
		fn_logger.debug("Fetching UserID for @%s" % screen_name)
		query_user = api.get_user(screen_name=screen_name)
		user_id = query_user.id
	else:
		fn_logger.warning("This shouldn't happen")
	if screen_name == tacct:
		columns = ['account_id', 'screen_name', 'asof',]
		df = pd.DataFrame([(user_id, screen_name, _run_dt,),], columns=columns)
		tablename = 'dt_account'
	# fn_logger.debug("Adding account info to '%s'" % tablename)
	# df.to_sql(tablename, con=engine, if_exists='append', index=False, schema=schema)
	# else:
	#	screen_name = tacct
	# ToDo: Authorized accounts are stored in a separate folder
	if screen_name == tacct:
		filename = join(acct_cache, "%s_%s_ids_%s.txt.xz" % (screen_name, id_type, _fdate))
	else:
		filename = join(user_cache, "%s_%s_ids_%s.txt.xz" % (screen_name, id_type, _fdate))
	if not max_pages:
		max_pages = 10000
	if query_user:
		q = query_user
		query_lines = [
			"Query User : @%-16s" % q.screen_name,
			"Verified: %s" % q.verified,
			"Followers: {:,d}".format(q.followers_count),
			"Friends: {:,d}".format(q.friends_count),
			"Listed: {:,d}".format(q.friends_count),
			"Tweeets: {:,d}".format(q.friends_count),
		]
		msg = '  '.join(query_lines)
		# fn_logger.debug("Query User: @%s  Verified: %s" % (query_user.screen_name, query_user.verified))
		fn_logger.debug(msg)
		if query_user.verified:
			do_nothing()
		do_nothing()
	# Done processing parameters

	# Get IDs
	ids = []
	if exists(filename) and _run_dt.timestamp() - getmtime(filename) < 12 * 60**2:
		ids = load_ids(filename)
	if not ids:
		fn_logger.debug("Fetching %s IDs for @%s . . ." % (friendly_id_type, screen_name))
		page_count = 0
		# I just found out that the maximum value for count is 5,000, not 100
		# Also, the rate limit is also 15 calls every 15 minutes
		for page in tweepy.Cursor(api_fn, count=5000, screen_name=screen_name).pages():
			page_count += 1
			ids.extend(page)
			fn_logger.debug("%3d) Fetched %d %s IDs for @%s" % (page_count, len(page), friendly_id_type, screen_name))
			if page_count == max_pages:
				fn_logger.info("Page limit reached, fetching %s IDs for @%s.  Exiting Cursor loop" % (friendly_id_type, screen_name))
				break
			snoozer(44.4444, 77.7777)
		fn_logger.debug("Fetched %d %s IDs for @%s" % (len(ids), friendly_id_type, screen_name))
		save_ids(filename, ids)
	columns = ['user_id', 'friend_id', 'asof',]
	df = pd.DataFrame([(user_id, x, _run_dt) for x in sorted(ids)], columns=columns)
	tablename = 'dt_user_%s' % id_type
	# fn_logger.debug("Adding %d %s IDs to '%s'" % (len(ids), friendly_id_type, tablename))
	# df.to_sql(tablename, con=engine, if_exists='append', index=False, schema=schema)
	return ids


def get_users(user_ids, list_type=None, screen_name=None, return_type=None):
	"""
	Get user objects
	:param user_ids: user IDs to lookup
	:param list_type: friends, followers, etc.
	:param screen_name: Twitter user related to the IDs, if any
	:param return_type: return values as CSV, Pandas DataFrame, List, or Tweepy objects (default)
	:return: list of user objects
	"""
	fn_logger = logging.getLogger(__MODULE__ + '.get_users')
	if not list_type:
		list_type = 'users'
	if not screen_name:
		screen_name = tacct
	if not return_type:
		return_type = 'tweepy'
	else:
		return_type = return_type.lower()
		if return_type not in ['csv', 'dataframe', 'df', 'list',]:
			raise ValueError("Invalid return_type.  Valid return_types are CSV, DataFrame, DF, or Tweepy")
	return_values = []
	batch_size = 100
	batches = (user_ids[i:i+batch_size] for i in range(0, len(user_ids), batch_size))
	tweepy_users = []
	fn_logger.debug("Fetching User objects (list_type=%s screen_name=%s return_type=%s) . . ." % (list_type, screen_name, return_type))
	for batch_count, batch in enumerate(batches):
		if batch_count > 0:
			snoozer(MIN_BATCH_DELAY, MAX_BATCH_DELAY)
		users = api.lookup_users(user_id=batch, tweet_mode='extended')
		tweepy_users.extend(users)
		fn_logger.debug("Fetched %d User objects for batch: %d" % (len(users), batch_count + 1))
	# ToDo: Build This Out
	user_rows = []
	df = pd.DataFrame()
	if tweepy_users:
		no_status_count = 0
		fn_logger.debug("Fetched %d user objects for @%s" % (len(tweepy_users), screen_name))
		for user in tweepy_users:
			user_since = None  # datetime.fromtimestamp(-1)
			if hasattr(user, 'status'):
				user_since = user.status.created_at
			else:
				no_status_count += 1
			user_row = [    user.id, _run_dt, user.screen_name, user.name, user.created_at,
			                user.default_profile_image, user.protected,
							user.followers_count, user.friends_count,
							user.listed_count, user.statuses_count,
							user_since,
			]
			user_rows.append(user_row)
		columns = ['user_id', 'asof', 'screen_name', 'name', 'created_at',
		          'default_profile_image', 'protected',
		          'followers_count', 'friends_count',
		          'listed_count', 'statuses_count',
		          'last_tweet', ]
		try:
			if screen_name == tacct:
				filename = join(acct_cache, "%s_%s_%s.csv.xz" % (screen_name, list_type, _fdate))
			else:
				filename = join(user_cache, "%s_%s_%s.csv.xz" % (screen_name, list_type, _fdate))
			df = pd.DataFrame(sorted(user_rows), columns=columns)
			fn_logger.debug("Saving User objects to '%s'" % filename)
			df.to_csv(filename, index=False)
			# Ensure lowercase tablenames (otherwise, PostgreSQL wants table names to quoted)
			tablename = 'dt_user'
			# fn_logger.debug("Adding User objects to '%s'" % tablename)
			# df.to_sql(tablename, con=engine, if_exists='append', index=False, schema=schema)
		except Exception as e:
			fn_logger.exception("Exception: %s" % e)
		if return_type == 'tweepy':
			return_values = tweepy_users
		elif return_type in ['df', 'dataframe']:
			return_values = df
		elif return_type in ['csv', 'list',]:
			return_values = user_rows
	return return_values


def idle(last_tweeted):
	"""
	Calculate idle time for a user account
	:param last_tweeted: date of user's last tweet
	:return: date diffence between run time and last tweet (timedelta object)
	"""
	if not last_tweeted.tzinfo:
		last_tweeted.replace(tzinfo=timezone.utc)
	return _run_utc - last_tweeted


def load_ids(filename):
	fn_logger = logging.getLogger("%s.load_ids" % __MODULE__)
	ids = []
	# We're getting fancy, here - supporting XZ/LZMA compression
	if filename.endswith('.xz'):
		open = lzma.open
	else:
		open = builtins.open
	fn_logger.debug("Loading IDs from '%s'" % filename)
	with open(filename, 'rt') as lzfile:
		for line in lzfile.readlines():
			try:
				ids.append(int(line))
			except Exception as e:
				fn_logger.exception("Exception at line %d of '%s': %s" % line, filename, e)
	fn_logger.debug("Loaded %d IDs from '%s'" % (len(ids), filename))
	return ids


def prune_idlers(screen_name, friend_ids, common_friends, no_img, make_changes):
	"""
	Prune outgoing friendships
	:param friend_ids: existing friend IDs
	:param make_changes: destroy friendships (otherwise, just list query_user)
	:return: pruned_users (list)
	"""
	fn_logger = logging.getLogger(__MODULE__ + '.prune_idlers')
	max_idle_days = Config.MAX_IDLE_DAYS
	prune_limit = Config.PRUNE_LIMIT
	prune_followers = Config.PRUNE_FOLLOWERS
	prune_excludes = Config.PRUNE_EXCLUDES
	report_lines = []
	pruned_rows = []
	pruned_users = []
	pruned_count = 0
	if not DRYRUN and make_changes:
		msg = "Pruning for @%s . . ." % screen_name
	else:
		msg = "Pruning for @%s (DRY RUN) . . ." % screen_name
	fn_logger.info(msg)
	# ToDo: Add logic to see who follows candidates for pruning
	# follower_ids = get_follower_ids(screen_name=screen_name)
	user_count = 0
	for user in get_users(friend_ids, list_type='friend', screen_name=screen_name):
		user_count += 1
		fn_logger.debug("%7d) Checking @%s" % (user_count, user.screen_name))
		if user.screen_name in prune_excludes:
			continue
		prune = False
		flags = []
		if user.default_profile_image and no_img:
			flags.append('NOIMG')
			prune = True
		if hasattr(user, 'status'):
			idle_days = idle(user.status.created_at).days
			if idle_days > max_idle_days:
				if idle_days > 999:
					flag = "ID999"
				elif idle_days > 499:
					flag = "ID500"
				elif idle_days > 364:
					flag = "ID365"
				else:
					flag = "ID%03d" % max_idle_days
				flags.append(flag)
				prune = True
		if user.followers_count < prune_followers:
			flags.append('FLWRS')
			prune = True
		# If a candidate for pruning is followed by enough friends, then don't prune query_user
		if prune and common_friends:
			user_follower_ids = get_follower_ids(screen_name=user.screen_name, max_pages=10)
			friends_following_ids = sorted(list(set(friend_ids).intersection(set(user_follower_ids))))
			ffu_count = len(friends_following_ids)
			# This is a big bottleneck...
			# friends_following_users = get_users(friends_following_ids, 'friends_following_%s' % user.screen_name, return_type='list')
			# ffu_screen_names = [x[2] for x in friends_following_users]
			fn_logger.debug("@%s is followed by %d of %s's friends" % (user.screen_name, ffu_count, screen_name))
			if ffu_count > 3:
				# If more than three of my friends follow this person, don't prune query_user
				prune = False
		# Do the actual pruning / unfollowing
		if prune:
			pruned_count += 1
			pruned_users.append(user.screen_name)
			# json_row = { 'screen_name': user.screen_name, 'user_id': user.id, 'flags': ','.join(flags)}
			pruned_row = (user.id, _run_dt, user.screen_name, ','.join(flags))
			report_line = "%3d) @%-16s %s" % (pruned_count, user.screen_name, ' '.join(sorted(flags)))
			fn_logger.info(report_line)
			# Aggregate rows
			# json_rows.append(json_row)
			pruned_rows.append(pruned_row)
			report_lines.append(report_line)
			if not DRYRUN and make_changes:
				fn_logger.debug('Unfollowing @%s)' % user.screen_name)
				api.destroy_friendship(screen_name=user.screen_name)
				snoozer(MIN_FOLLOW_DELAY, MAX_FOLLOW_DELAY)
				if pruned_count == prune_limit:
					fn_logger.info("Prune limit reached.  Exiting loop")
					break
	if pruned_rows:
		msg = "@%s Pruned Users: %d" % (screen_name, len(pruned_users))
		if DRYRUN or not make_changes:
			msg = "%s (DRY RUN)" % msg
		fn_logger.info(msg)
		columns = ['user_id', 'asof', 'screen_name', 'flags']
		df = pd.DataFrame(sorted(pruned_rows), columns=columns)
		# Save to CSV
		filename = join(acct_cache, "%s_pruned_detail_%s.csv.xz" % (tacct, _fdatetime))
		fn_logger.debug("Saving to '%s'" % filename)
		df.to_csv(filename, header=columns, index=False)
		# Save to Database
		tablename = 'dt_pruned'
		# fn_logger.debug("Adding pruned users to '%s'" % tablename)
		# df.to_sql(tablename, con=engine, if_exists='append', index=False, schema=schema)
	if report_lines:
		# Create greenbar report
		report_title = "@%s Pruned Detail (%d)" % (screen_name, len(report_lines))
		if not make_changes:
			report_title += " (DRY RUN)"
		reporter(report_title, screen_name, report_lines)
	return pruned_users


def reporter(report_title, screen_name, report_lines, columnar=False, send_direct_message=False):
	"""
	Generate report on greenbar image
	:param report_title:
	:param report_lines:
	:param columnar: Use column formatting (True/False)
	:param send_direct_message: Send output via Twitter DM (True/False)
	:return: image_filename(s)
	"""
	fn_logger = logging.getLogger(__MODULE__ + '.reporter')
	blank_page = Config.GREENBAR_PAPER

	fn_logger.debug("Report Directory: '%s'" % report_dir)
	os.makedirs(report_dir, exist_ok=True)
	report_date = _run_dt.strftime("%a %b %-d %Y")
	report_time = _run_dt.strftime("%H:%M:%S %z")

	if columnar:
		if len(report_lines) <= 15:     # One column
			ncols = 1
		elif len(report_lines) <= 30:   # Two columns
			ncols = 2
		elif len(report_lines) <= 45:   # Three columns
			ncols = 3
		elif len(report_lines) <= 60:   # Four columns
			ncols = 4
		else:
			ncols = 5
		rows = []
		raw_lines = (report_lines[i:i+ncols] for i in range(0, len(report_lines), ncols))
		for row_count, raw_line in enumerate(raw_lines):
			columns = ''
			for i in range(len(raw_line)):
				columns += "@%-16s  " % raw_line[i]
			row = "%3d) %s" % (row_count+1, columns)
			rows.append(row)
			fn_logger.debug(row)
		report_lines = rows

	# Set fonts for report title & report body
	title_font = ImageFont.truetype('Arial_Black.ttf', 64)
	body_font = ImageFont.truetype('LiberationMono-Regular', 25)

	# Enumerate pages of report lines
	lines_per_page = 40
	pages = [report_lines[i:i+lines_per_page] for i in range(0, len(report_lines), lines_per_page)]
	image_filenames = []
	for page_count, page_lines in enumerate(pages):
		page_info = '\n'.join([
			report_date,
			report_time,
			"Page: %d" % (page_count + 1),
			])
		fn_logger.debug("Report Title: %s  Date/Time: %s/%s  Page: %d" % (report_title, report_date, report_time, page_count + 1))
		# Open an Image
		fn_logger.debug("Opening blank page: '%s'" % blank_page)
		imgfile = Image.open(blank_page)
		width, height = imgfile.size

		# Call draw Method to add 2D graphics in an image
		drawing = ImageDraw.Draw(imgfile)

		# Start drawing
		x, y = 125, 75
		drawing.text((x, y), report_title, font=title_font, fill=(0, 0, 0))
		drawing.text((width - 340, y + 10), page_info, font=body_font, spacing=5, fill=(0, 0, 0))
		y = 170
		for line in page_lines:
			fn_logger.debug(line)
			drawing.text((x, y), line, font=body_font, spacing=8, fill=(0, 0, 0))
			y += 29.25

		if DEBUG:
			# Display edited image
			imgfile.show()

		# Generate image filename from report title
		new = []
		for c in report_title:
			if c.isalnum():
				new.append(c)
			elif c.isspace():
				new.append('_')
			elif c.isprintable():
				new.append('_')
		if new[0] == '_':
			new.pop(0)
		image_filename = ''.join(new)
		while '__' in image_filename:
			image_filename = image_filename.replace('__', '_')
		# Only show page numbers if there is more than one page
		if len(pages) > 1:
			pgnum = "_P%02d" % (page_count + 1)
		else:
			pgnum = ""
		image_filename = join(report_dir, "%s_%s%s.png" % (image_filename.rstrip('_'), _fdatetime, pgnum))
		fn_logger.debug("Saving '%s'" % image_filename)
		imgfile.save(image_filename)
		image_filenames.append(image_filename)

		# Send report via direct message on Twitter
		if send_direct_message:
			fn_logger.debug("Uploading '%s' to Twitter . . .)" % basename(image_filename))
			media = api.media_upload(filename=image_filename)
			fn_logger.debug('Sending direct message to %d . . .' % DM_RECIPIENT_ID)
			dm = api.send_direct_message(
				recipient_id=DM_RECIPIENT_ID, text=report_title,
				attachment_type='media', attachment_media_id=media.media_id)
	return image_filenames


def reporter2(report_title, report_lines, columnar=False, send_direct_message=False):
	"""
	Generate report on greenbar image
	:param report_title:
	:param report_lines:
	:param columnar: Use column formatting (True/False)
	:param send_direct_message: Send output via Twitter DM (True/False)
	:return: image_filename(s)
	"""
	fn_logger = logging.getLogger("%s.reporter" % __MODULE__)
	blank_page = Config.GREENBAR_PAPER

	# report_dir = Config.REPORT_DIR
	report_date = _run_dt.strftime("%a %b %-d %Y")
	report_time = _run_dt.strftime("%H:%M:%S %z")

	if columnar:
		if len(report_lines) <= 15:     # One column
			ncols = 1
		elif len(report_lines) <= 30:   # Two columns
			ncols = 2
		elif len(report_lines) <= 45:   # Three columns
			ncols = 3
		elif len(report_lines) <= 60:   # Four columns
			ncols = 4
		else:
			ncols = 5
		rows = []
		raw_lines = (report_lines[i:i+ncols] for i in range(0, len(report_lines), ncols))
		for row_count, raw_line in enumerate(raw_lines):
			columns = ''
			for i in range(len(raw_line)):
				columns += "@%-16s  " % raw_line[i]
			row = "%3d) %s" % (row_count+1, columns)
			rows.append(row)
			fn_logger.debug(row)
		report_lines = rows

	# Set fonts for report title & report body
	title_font = ImageFont.truetype('Arial_Black.ttf', 64)
	body_font = ImageFont.truetype('LiberationMono-Regular', 25)

	# Enumerate pages of report lines
	lines_per_page = 40

	rpt_preprocessor(report_lines, blank_page, body_font, lines_per_page)
	pages = [report_lines[i:i+lines_per_page] for i in range(0, len(report_lines), lines_per_page)]
	image_filenames = []
	for page_count, page_lines in enumerate(pages):
		page_info = '\n'.join([
			report_date,
			report_time,
			"Page: %d" % (page_count + 1),
			])
		fn_logger.debug("Report Title: %s  Date/Time: %s/%s  Page: %d" % (report_title, report_date, report_time, page_count + 1))
		# Open an Image
		fn_logger.debug("Opening blank page: '%s'" % blank_page)
		imgfile = Image.open(blank_page)
		width, height = imgfile.size

		# Call draw Method to add 2D graphics in an image
		drawing = ImageDraw.Draw(imgfile)

		# Start drawing
		x, y = 125, 75
		drawing.text((x, y), report_title, font=title_font, fill=(0, 0, 0))
		drawing.text((width - 340, y + 10), page_info, font=body_font, spacing=5, fill=(0, 0, 0))
		y = 170
		for line in page_lines:
			fn_logger.debug(line)
			drawing.text((x, y), line, font=body_font, spacing=8, fill=(0, 0, 0))
			y += 29.25

		if DEBUG:
			# Display edited image
			imgfile.show()

		# Generate image filename from report title
		new = []
		for c in report_title:
			if c.isalnum():
				new.append(c)
			elif c.isspace():
				new.append('_')
			elif c.isprintable():
				new.append('_')
		if new[0] == '_':
			new.pop(0)
		image_filename = ''.join(new)
		while '__' in image_filename:
			image_filename = image_filename.replace('__', '_')
		# Only show page numbers if there is more than one page
		if len(pages) > 1:
			pgnum = "_P%02d" % (page_count + 1)
		else:
			pgnum = ""
		image_filename = join(report_dir, "%s_%s%s.png" % (image_filename.rstrip('_'), _fdatetime, pgnum))
		fn_logger.debug("Saving '%s'" % image_filename)
		imgfile.save(image_filename)
		image_filenames.append(image_filename)

		# Send report via direct message on Twitter
		if send_direct_message:
			fn_logger.debug("Uploading '%s' to Twitter . . ." % basename(image_filename))
			media = api.media_upload(filename=image_filename)
			fn_logger.debug('Sending direct message to %d' % DM_RECIPIENT_ID)
			dm = api.send_direct_message(
				recipient_id=DM_RECIPIENT_ID, text=report_title,
				attachment_type='media', attachment_media_id=media.media_id)
	return image_filenames


def rpt_preprocessor(reportlines, image, font, lines_per_page):

	return


def save_ids(filename, ids):
	fn_logger = logging.getLogger(__MODULE__ + '.save_ids')
	lines = []
	for id in ids:
		line = str(id)
		if not line.endswith(os.linesep):
			line = line + os.linesep
		lines.append(line)
	# We're getting fancy, here - supporting XZ/LZMA compression
	if filename.endswith('.xz'):
		open = lzma.open
	else:
		open = builtins.open
	fn_logger.debug("Saving IDs to '%s'" % filename)
	with open(filename, 'wt') as lzfile:
		lzfile.writelines(lines)
	fn_logger.debug("Saved %d IDs to '%s'" % (len(ids), filename))
	return


def snoozer(min_sleep, max_sleep=None):
	"""
	Sleep for for a specified amount of time; if max_sleep is specified,
	a random number is generated
	:param min_sleep: The minimum amount of time to sleep
	:param max_sleep: The maximum amount of time to sleep, if supplied
	:return: None
	"""
	fn_logger = logging.getLogger("%s.snoozer" % __MODULE__)
	if type(min_sleep) not in [int, float]:
		raise ValueError("min_sleep should be an int or float")
	if max_sleep:
		if type(max_sleep) not in [int, float]:
			raise ValueError("max_sleep should be an int or float")
		sleep_seconds = abs(uniform(min_sleep, max_sleep))
	else:
		sleep_seconds = abs(min_sleep)
	start_ts = datetime.now().timestamp()
	fn_logger.debug('sleep(%f)' % sleep_seconds)
	sleep(sleep_seconds)
	stop_ts = datetime.now().timestamp()
	actual_sleep = stop_ts - start_ts
	if actual_sleep < sleep_seconds:
		raise Exception("actual sleep less than requested sleep")
	return


def stringify(iterable, separator=','):
	iterable = separator.join([ str(x) for x in iterable ])
	return iterable


def do_nothing():
	pass


if __name__ == '__main__':
	_run_dt = datetime.now().astimezone().replace(microsecond=0)
	_run_utc = _run_dt.astimezone(timezone.utc)
	_fdate = _run_dt.strftime("%Y%m%d")
	_fdatetime = _run_dt.strftime("%Y%m%d_%H%M%S")

	__appname__ = Config.__appname__
	DEBUG = Config.DEBUG
	DRYRUN = Config.DRYRUN
	LOG_LEVEL = Config.LOG_LEVEL

	# Configure logging
	FILENAME_SUFFIX = Config.FILENAME_SUFFIX
	if not FILENAME_SUFFIX:
		fname = "%s.log"  % __MODULE__
	else:
		fname = "%s-%s.log"  % (__MODULE__, FILENAME_SUFFIX)
	logfilename = join(Config.LOG_DIR, fname)
	file_handler = RotatingFileHandler(logfilename, maxBytes=9*1024**2, backupCount=9)
	logging.basicConfig(level=LOG_LEVEL,
	                    format='%(asctime)s %(name)-30s %(levelname)-8s %(message)s',
	                    datefmt='%Y-%m-%d %H:%M:%S%z',
	                    handlers=[file_handler,])
	# define a Handler which writes INFO messages or higher to the sys.stderr
	console = logging.StreamHandler()
	console.setLevel(logging.INFO)
	# set a format which is simpler for console use
	formatter = logging.Formatter('%(name)-28s: %(levelname)-8s %(message)s')
	# tell the handler to use this format
	console.setFormatter(formatter)
	# add the handler to the root logger
	logging.getLogger('').addHandler(console)

	# Configure File System Stuff
	cache_dir = Config.CACHE_DIR
	# data_dir = Config.DATA_DIR
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
	MAX_BATCH_DELAY = Config.MAX_BATCH_DELAY
	MIN_BATCH_DELAY = Config.MIN_BATCH_DELAY

	MAX_FOLLOW_DELAY = Config.MAX_FOLLOW_DELAY
	MIN_FOLLOW_DELAY = Config.MIN_FOLLOW_DELAY

	MAX_GET_ID_DELAY = Config.MAX_GET_ID_DELAY
	MIN_GET_ID_DELAY = Config.MIN_GET_ID_DELAY

	MAX_SEARCH_DELAY = Config.MAX_SEARCH_DELAY
	MIN_SEARCH_DELAY = Config.MIN_SEARCH_DELAY

	NEW_FRIEND_LIMIT = Config.NEW_FRIEND_LIMIT
	MIN_LISTED_COUNT = Config.MIN_LISTED_COUNT
	MIN_STATUS_COUNT = Config.MIN_STATUS_COUNT

	api = tweepy.API()
	tacct = ''
	tacctid = -1

	try:
		init()
		main()
	except BlockingIOError as be:
		logging.exception("BlockingIO %s" % be)
	except pid.PidFileAlreadyLockedError:
		logging.warning("PID File Locked.  Aborting.")
	except KeyboardInterrupt:
		logging.warning("KeyboardInterrupt, Aborting.")
	except Exception as e:
		logging.exception("Exception %s" % e)
	finally:
		eoj()


