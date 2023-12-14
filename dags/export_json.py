import json
from datetime import datetime
from datetime import timezone
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.hooks.filesystem import FSHook
from airflow.decorators import task
from fields import get_title_and_issn
from fields import get_source_sas_id


def get_doc_template():
  template = [
    {
      "model" : "biblioref.reference",
      "fields": {
        "item_form": "s",
        "literature_type" : "S",
        "record_type" : "a",
        "treatment_level" : "as",
        "BIREME_reviewed" : False,
        "created_by" : 2,
        "status" : -3,
        "software_version" : "OJS2FiAdmin 1.0"
      }
    },
    {
      "model" : "biblioref.referenceanalytic",
      "fields": {}
    },
    {
      "model" : "biblioref.referencesource",
      "fields": {}
    }
  ]
  return template


@task()
def export_json_to_fiadmin(mongo_conn_id, mongo_collection):
  mongo_hook = MongoHook(mongo_conn_id)
  file_hook = FSHook(conn_id='export_fiadmin')

  now = datetime.now(timezone.utc).astimezone().isoformat()

  results = mongo_hook.find(mongo_collection, {})
  for result in results:
    doc = get_doc_template()
    
    doc[0]['fields']['created_time'] = now
    doc[0]['fields']['updated_time'] = now

    if 'abstracts' in result:
      doc[0]['fields']['abstract'] = result['abstracts']

    if 'subjects' in result:
      doc[0]['fields']['author_keyword'] = result['subjects']

    if 'language' in result:
      doc[0]['fields']['text_language'] = json.dumps([result['language']])

    if 'date' in result:
      doc[0]['fields']['publication_date_normalized'] = result['date']

    if 'fulltext_url' in result:
      doc[0]['fields']['electronic_address'] = json.dumps([{'_u': result['fulltext_url']}])

    if 'volume' in result:
      doc[2]['fields']['volume_serial'] = result['volume']

    if 'number' in result:
      doc[2]['fields']['issue_number'] = result['number']

    if 'publishers' in result:
      doc[1]['fields']['publisher'] = result['publishers'][0]["text"]

    if 'titles' in result:
      doc[1]['fields']['title'] = result['titles']

    if 'doi' in result:
      doc[1]['fields']['doi_number'] = result['doi']

    if 'pagination' in result:
      doc[1]['fields']['pages'] = result['pagination']

    if 'creators' in result:
      doc[1]['fields']['individual_author'] = []
      for author in result['creators']:
        doc[1]['fields']['individual_author'].append({'text': author})
      doc[1]['fields']['individual_author'] = json.dumps(doc[1]['fields']['individual_author'])

    if 'journal' in result and 'issn' in result:
      journal_issn = None
      for issn in result['issn']:
        journal_issn = get_title_and_issn(result['journal'], issn)
        if journal_issn:
          doc[2]['fields']['title_serial'] = journal_issn['journal']
          doc[2]['fields']['issn'] = journal_issn['issn']
          break

    if 'title_serial' in doc[2]['fields'] and 'publication_date_normalized' in doc[0]['fields']:
      journal = doc[2]['fields']['title_serial']
      year = doc[0]['fields']['publication_date_normalized'][:4]
      volume = None
      number = None

      if 'volume_serial' in doc[2]['fields']:
        volume = doc[2]['fields']['volume_serial']
      if 'issue_number' in doc[2]['fields']:
        number = doc[2]['fields']['issue_number']
      source_id = get_source_sas_id(journal, year, volume, number)

      if source_id:
        doc[1]['fields']['source'] = source_id
        doc.pop(2)

    filepath = "%s/%s.json" % (file_hook.get_path(), result['_id'])
    with open(filepath, 'w') as json_file:
      json.dump(doc, json_file, indent=2)