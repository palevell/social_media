# models.py - Wednesday, January 19, 2022

import oyaml as yaml
# import sqlalchemy as sa
from sqlalchemy import (
	BigInteger,
	Boolean,
	Column,
	DateTime,
	DDL,
	event,
	func,
	ForeignKey,
	Identity,
	Index,
	Integer,
	Numeric,
	String,
	Table,
)
from sqlalchemy.ext import hybrid
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

from config import Config

Base = declarative_base()
schema = Config.DB_SCHEMA


# Table Definitions
# ToDo: Add User Type (ie. my account or not)
# ToDo: Add derived columns for scoring
class User(Base):
	__table__ = Table(
		'dt_user',
		Base.metadata,
		Column('id', BigInteger, primary_key=True, autoincrement=False),
		Column('asof', DateTime(timezone=True), index=True, nullable=False, server_default=func.now()),
		Column('username', String(16), index=True, nullable=False, unique=True),
		Column('displayname', String(50), nullable=False),
		Column('created_at', DateTime(timezone=True)),
		# Column('default_profile_image', Boolean, nullable=False),
		# Column('protected', Boolean, nullable=False),
		# Column('verified', Boolean, nullable=False),
		Column('followers_count', BigInteger),
		Column('friends_count', BigInteger),
		Column('listed_count', BigInteger),
		Column('statuses_count', BigInteger),
		Column('last_tweeted', DateTime(timezone=True), index=True),
		# relationship("Tweets", "Status"),
		schema=schema,
	)
	Index('idx_asof_desc', __table__.columns.asof.desc()),

	def __repr__(self):
		return "<User: @%s id: %d>" % (self.username, self.id)


class UserHistory(Base):
	__table__ = Table(
		'dt_user_history',
		Base.metadata,
		Column('id', BigInteger, Identity(start=10001, always=True), primary_key=True),
		Column('user_id', BigInteger, nullable=False),
		# Column('user_id', BigInteger, ForeignKey('%s.dt_user.id' % schema)),
		Column('asof', DateTime(timezone=True), index=True, nullable=False),
		Column('username', String(16), index=True, nullable=False),
		Column('displayname', String(50), nullable=False),
		Column('created_at', DateTime(timezone=True)),
		Column('followers_count', BigInteger),
		Column('friends_count', BigInteger),
		Column('listed_count', BigInteger),
		Column('statuses_count', BigInteger),
		Column('last_tweeted', DateTime(timezone=True), index=True),
		schema=schema,
	)
	Index('idx_history_asof_user_id', __table__.columns.asof, __table__.columns.user_id, unique=True),
	Index('idx_history_asof_desc', __table__.columns.asof.desc()),

	def __repr__(self):
		return "<User: @%s id: %d Asof: %s>" % (self.username, self.user_id, self.asof)


"""class Status(Base):
	__table__ = Table(
		'dt_status',
		Base.metadata,
		Column('status_id', BigInteger, primary_key=True),
		# Column('asof', DateTime(timezone=True), index=True, nullable=False, server_default=func.now()),
		Column('user_id', BigInteger, ForeignKey('%s.dt_user.id' % schema)),
		# relationship('user', User, back_populates='username'),
		Column('created_at', DateTime(timezone=True), index=True, nullable=False),
		Column('lang', String(2), index=True),
		Column('reply_count', BigInteger),
		Column('retweet_count', BigInteger),
		Column('like_count', BigInteger),
		Column('quote_count', BigInteger),
		Column('content', String(280)),
		Column('conversation_id', BigInteger, index=True),
		Column('quoted_id', BigInteger, index=True),
		Column('hashtags', String),
		Column('mentions', String),
		schema=schema,

	)

	def __repr__(self):
		return "<Status: id: %d>" % self.id"""


"""
replyCount	retweetCount	likeCount	quoteCount	conversationId	lang
"""

""" Tables:
	dt_account
	dt_candidate
	dt_pruned
	dt_user
	dt_user_friend
"""


"""
class Status(Base):
	__tablename__ = 'dt_status'
	# Here we define columns for the table status.
	# Notice that each column is also a normal Python instance attribute.
	id = Column(Integer, primary_key=True)
	uid = Column(Integer, ForeignKey('dt_user.id'))
	user = relationship(User)
	text = Column(String(280))
	_json = Column(Text, nullable=False)
	row_date = Column(DateTime(timezone=True), server_default=func.now(), index=True)
"""
