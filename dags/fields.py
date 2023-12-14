import re
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.decorators import task


def is_code_valid(field, code):
  mysql_hook = MySqlHook(mysql_conn_id='fiadmin')
  mysql_conn = mysql_hook.get_conn()

  is_valid = False
  with mysql_conn.cursor() as cursor:
    sql = "SELECT id FROM utils_auxcode WHERE field=%s and code=%s"
    cursor.execute(sql, (field, code))
    result = cursor.fetchone()
    if result:
      is_valid = True

  return is_valid


def get_source_sas_id(journal, year, volume, number):
  mysql_hook = MySqlHook(mysql_conn_id='fiadmin')
  mysql_conn = mysql_hook.get_conn()

  sql_volume = ""
  if (volume):
    sql_volume = " a.volume_serial = '%s' AND " % volume

  sql_number = ""
  if (number):
    sql_number = " a.issue_number = '%s' AND " % number

  sql = """
    SELECT
      b.id as code
    FROM 
      biblioref_referencesource AS a, 
      biblioref_reference AS b 
    WHERE 
      a.title_serial = %s AND 
      a.reference_ptr_id = b.id AND """ + sql_volume + sql_number + """
      LEFT(b.publication_date_normalized, 4) = %s
    ORDER BY
      CASE 
        WHEN b.status = -1 THEN -4
        ELSE b.status
      END DESC
    LIMIT 1
  """

  id = None
  with mysql_conn.cursor() as cursor:
    cursor.execute(sql, (journal, year))
    result = cursor.fetchone()
    if result:
      id = result[0]
  
  return id


def get_title_and_issn(journal, issn):
  mysql_hook = MySqlHook(mysql_conn_id='fiadmin')
  mysql_conn = mysql_hook.get_conn()
  sql = """
    SELECT 
      coalesce(NULLIF(a.shortened_title, ''), a.title),
      coalesce(NULLIF(a.issn, ''), c.issn),
      b.initial_date,
      b.initial_volume,
      b.initial_number,
      b.final_date,
      b.final_volume,
      b.final_number,
      b.indexer_cc_code
    FROM 
      title_title AS a,
      title_indexrange AS b,
      title_titlevariance AS c
    WHERE
      (
        ('' = %s OR a.shortened_title = %s OR a.title = %s) OR
        ('' = %s OR a.issn = %s OR c.issn = %s)
      ) AND
      c.title_id = a.id AND
      c.type = '240' AND 
      c.issn IS NOT NULL AND
      b.title_id = a.id AND 
      b.index_code_id = '17'
    LIMIT 1
  """

  journal_issn = {}
  with mysql_conn.cursor() as cursor:
    cursor.execute(sql, (journal, journal, journal, issn, issn, issn))
    result = cursor.fetchone()
    if result:
      journal_issn['journal'] = result[0]
      journal_issn['issn'] = result[1]

  return journal_issn


def is_date_valid(date_string):
    pattern = re.compile(r'^\d{8}$')
    return bool(pattern.match(date_string))


def get_new_lang_code(code):
  lang_code = code.strip().lower()
  new_lang_code = None

  if lang_code in ["por", "pt-br"]:
    new_lang_code = "pt"
  elif lang_code in ["eng", "en-us"]:
    new_lang_code = "en"
  elif lang_code in ["spa", "es-es"]:
    new_lang_code = "es"
  else:
    if not is_code_valid('text_language', lang_code):
      new_lang_code = ''

  return new_lang_code


@task()
def transform_date_field(mongo_conn_id, mongo_collection):
  mongo_hook = MongoHook(mongo_conn_id)

  dates = mongo_hook.aggregate(
    mongo_collection, 
    [
      {
        "$group": {
          "_id": "$date",
          "count": {
            "$sum": 1
          }
        }
      }
    ]
  )
  for date in dates:
    value = date['_id']
    if value:
      new_value = None
      value = value.strip().replace("-", "")

      if date['_id'] != value:
        if 4 <= len(value) <= 7:
          value = value.ljust(8, '0')

        if is_date_valid(value):
          new_value = value

      if new_value is not None:
        mongo_hook.update_many(
          mongo_collection, 
          {'date': date['_id']},
          {'$set': {'date': new_value}}
        )


def transform_lang_subfield(mongo_conn_id, mongo_collection, field_name):
  mongo_hook = MongoHook(mongo_conn_id)

  langs = mongo_hook.aggregate(
    mongo_collection, 
    [
      {
        "$unwind": "$%s" % field_name
      },
      {
        "$group": {
          "_id": "$%s._i" % field_name
        }
      }
    ]
  )
  for lang in langs:
    lang_code = lang['_id']
    if lang_code:
      new_lang_code = get_new_lang_code(lang_code)

      if new_lang_code is not None:
        subfield_path = "%s._i" % field_name
        subfield_occ_path = '%s.$[elem]._i' % field_name

        mongo_hook.update_many(
          mongo_collection, 
          {subfield_path: lang['_id']},
          {'$set': {subfield_occ_path: new_lang_code}},
          array_filters = [{'elem._i': lang['_id']}]
        )


@task()
def transform_title_field(mongo_conn_id, mongo_collection):
  transform_lang_subfield(mongo_conn_id, mongo_collection, 'titles')


@task()
def transform_subject_field(mongo_conn_id, mongo_collection):
  transform_lang_subfield(mongo_conn_id, mongo_collection, 'subjects')


@task()
def transform_abstract_field(mongo_conn_id, mongo_collection):
  transform_lang_subfield(mongo_conn_id, mongo_collection, 'abstract')


@task()
def transform_publisher_field(mongo_conn_id, mongo_collection):
  transform_lang_subfield(mongo_conn_id, mongo_collection, 'publishers')


@task()
def transform_source_field(mongo_conn_id, mongo_collection):
  transform_lang_subfield(mongo_conn_id, mongo_collection, 'sources')


@task()
def transform_language_field(mongo_conn_id, mongo_collection):
  mongo_hook = MongoHook(mongo_conn_id)

  langs = mongo_hook.aggregate(
    mongo_collection, 
    [
      {
        "$group": {
          "_id": "$language",
          "count": {
            "$sum": 1
          }
        }
      }
    ]
  )
  for lang in langs:
    lang_code = lang['_id']
    if lang_code:
      new_lang_code = get_new_lang_code(lang_code)

      if new_lang_code is not None:
        mongo_hook.update_many(
          mongo_collection, 
          {'language': lang['_id']},
          {'$set': {'language': new_lang_code}}
        )