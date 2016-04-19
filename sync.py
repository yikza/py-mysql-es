#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import yaml
import logging
import os.path

from datetime import date,datetime
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent
from pyelasticsearch import ElasticSearch, bulk_chunks

class MySync(object):
	log_file = None
	log_pos  = None
	
	def __init__(self):
		print '[INFO] starting ...'
		self.config = yaml.load(open('./etc/config.yaml'))
		self.mark_path = self.config['binlog']['mark']
		self.bulk_size = self.config['es']['bulk_size']
		self.excludes_fields = self.config['slave']['excludes_fields']
		self.es = ElasticSearch('http://{host}:{port}/'.format(host=self.config['es']['host'], port=self.config['es']['port']))
		"""
		resume stream
		"""
		if os.path.isfile(self.mark_path):		
			with open(self.mark_path, 'r') as y:
				mark = yaml.load(y)
				self.log_file = mark.get('log_file')
				self.log_pos  = mark.get('log_pos')
				logging.info("resume stream : file: {file}, pos: {pos}".format(file=self.log_file,pos=self.log_pos))	
	
	def mark_binlog(self):
		if self.log_file and self.log_pos:
			with open(self.mark_path, 'w') as y:
				logging.info("mark binlog: binlog_file: {file}, pos: {pos}".format(file=self.log_file, pos=self.log_pos))
				yaml.safe_dump({"log_file": self.log_file, "log_pos": self.log_pos}, y, default_flow_style=False)
	
	def _format(self, dat):
		for k,v in dat.items():
			if isinstance(v, datetime):
				dat[k] = v.strftime('%Y-%m-%d %H:%M:%S')				
			elif isinstance(v, date):
				dat[k] = v.strftime('%Y-%m-%d')
			if k in self.excludes_fields:
				del dat[k]
		return dat
	
	def proc_binlog(self):
		stream = BinLogStreamReader(
			connection_settings = self.config['mysql'],
			server_id = self.config['slave']['server_id'],
			log_file = self.log_file,
			log_pos = self.log_pos,
			only_schemas = self.config['slave']['schemas'],
			blocking = True,
			resume_stream = bool(self.log_file and self.log_pos),
			only_events=[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent]
		)
		for binlogevent in stream:
			self.log_file = stream.log_file
			self.log_pos  = stream.log_pos
			for row in binlogevent.rows:		
				pk     = binlogevent.primary_key			
				table  = binlogevent.table
				schema = binlogevent.schema

				if isinstance(binlogevent, WriteRowsEvent):
					yield self.es.index_op(self._format(row['values']), doc_type=table, index=schema, id=row['values'][pk])
				elif isinstance(binlogevent, UpdateRowsEvent):
					yield self.es.update_op(self._format(row['after_values']), doc_type=table, index=schema, id=row['after_values'][pk])
				elif isinstance(binlogevent, DeleteRowsEvent):
					yield self.es.delete_op(doc_type=table, index=schema, id=row['values'][pk])
				else:
					logging.warning("unsupport event type")
					continue
	
		stream.close()
		
	def send_email(self, msg):
		import smtplib
		from email.mime.text import MIMEText
		msg = MIMEText(msg, 'plain', 'utf-8')
		msg['From']    = self.config['email']['from']['user']
		msg['To']      = ','.join(self.config['email']['to'])
		msg['Subject'] = 'Binlog Sync Exception:'
		try:
			s = smtplib.SMTP();
			s.connect(self.config['email']['host'], self.config['email']['port'])
			s.ehlo()
			s.starttls()
			s.login(user=self.config['email']['from']['user'], password=self.config['email']['from']['passwd'])
			s.sendmail(msg['From'], self.config['email']['to'], msg.as_string())
			s.quit()
		except Exception:
			import traceback
			logging.error(traceback.format_exc())
	
	def run(self):
		try:
			if self.bulk_size < 2:
				for action in self.proc_binlog():
					self.es.bulk([action])
					self.mark_binlog()
			else:
				for chunk in bulk_chunks(self.proc_binlog(), docs_per_chunk=self.bulk_size):
					self.es.bulk(chunk)
					self.mark_binlog()
		except KeyboardInterrupt:
			pass
		except Exception:
			import traceback
			logging.error(traceback.format_exc())
			self.send_email(msg=traceback.format_exc())
			raise
	
def main():	
	logging.basicConfig(
		level=logging.DEBUG,
		format='%(asctime)s %(levelname)s %(message)s',
		datefmt='%Y-%m-%d %H:%M:%S',
		filename=config['log']['run']
	)
	logging.getLogger('elasticsearch').setLevel(logging.INFO)
	logging.getLogger('elasticsearch.trace').setLevel(logging.INFO)
	logging.getLogger('elasticsearch.trace').addHandler(logging.StreamHandler())
	instance = MySync()
	instance.run()

if __name__ == "__main__":
	main()
