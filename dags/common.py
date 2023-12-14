import re
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.decorators import task


def is_doi(s):
  doi_pattern = re.compile(r'^10\.\d{4,}/\S+$')
  return bool(doi_pattern.match(s))


def is_ojs_article_url(s):
  ojs_pattern = re.compile(r'/(?:[^/]+/)+article/view/\d+(/[^/]+)?$', re.IGNORECASE)
  return bool(ojs_pattern.search(s))


def is_issn(s):
  issn_pattern = re.compile(r'^\d{4}-\d{4}$')
  return bool(issn_pattern.match(s))


@task()
def setup_mongodb(mongo_conn_id, mongo_collection):
  mongo_hook = MongoHook(mongo_conn_id)
  collection = mongo_hook.get_collection(mongo_collection)
  collection.drop()
  collection.create_index('id', unique=True)
        

@task()
def insert_into_mongodb(records_info, mongo_conn_id, mongo_collection):
  mongo_hook = MongoHook(mongo_conn_id)
  mongo_hook.insert_many(mongo_collection, records_info)
        

@task()
def duplicate_collection(collection_from, collection_to, mongo_conn_id):
  mongo_hook = MongoHook(mongo_conn_id)
  mongo_hook.aggregate(collection_from, [{'$out': collection_to}])