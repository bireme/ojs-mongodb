import re
import os
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.decorators import task_group
from common import is_doi
from common import is_ojs_article_url
from common import is_issn
from common import setup_mongodb
from common import insert_into_mongodb
from common import duplicate_collection
from fields import transform_language_field
from fields import transform_date_field
from fields import transform_title_field
from fields import transform_subject_field
from fields import transform_abstract_field
from fields import transform_publisher_field
from fields import transform_source_field
from export_json import export_json_to_fiadmin


@dag(
    'import_ojs_xml',
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2023, 11, 13),
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Parse XML content using Airflow',
    schedule_interval=None,  # You can set a schedule if needed
)
def import_ojs_xml():
	ns = {
		'oai': "http://www.openarchives.org/OAI/2.0/",
		'oai_dc': 'http://www.openarchives.org/OAI/2.0/oai_dc/',
		'dc': 'http://purl.org/dc/elements/1.1/',
		'xsi': 'http://www.w3.org/2001/XMLSchema-instance',
		'xml': 'http://www.w3.org/XML/1998/namespace'
	}

	@task()
	def parse_xml_files(directory_path="/home/ubuntu/bireme-ojs-migration/LZ"):
		records_info = []

		for folder_name in os.listdir(directory_path):
			folder_path = os.path.join(directory_path, folder_name)

			# Check if the item in the directory is a directory and ends with "-oai_dc"
			if os.path.isdir(folder_path) and folder_name.endswith("-oai_dc"):
				for xml_file in os.listdir(folder_path):
					if xml_file.endswith(".xml"):
						xml_file_path = os.path.join(folder_path, xml_file)
						
						# Parse each XML file
						records_info.extend(parse_single_xml(xml_file_path))
		
		return records_info


	def parse_sources(sources):
		record_info = {}

		for source in sources:
			if is_issn(source.text):
				if 'issn' in record_info:
					record_info['issn'].append(source.text)
				else:
					record_info['issn'] = [source.text]
			else:
				source_val = {
					'text': source.text, 
					'_i': source.get(f'{{{ns["xml"]}}}lang')
				}
				if 'source' in record_info:
					record_info['sources'].append(source_val)
				else:
					record_info['sources'] = [source_val]

				source_parts = source_val['text'].split(';')

				if len(source_parts) == 3:
					record_info['pagination'] = source_parts.pop().strip()
				record_info['journal'] = source_parts.pop(0).strip()

				vol_num_year = source_parts.pop(0).strip()
				pattern = r'(?:Vol\.|v\.)\s*(?P<volume>\d+)\s*(?:(?:No\.|N[úu]m\.|n\.)\s*(?P<number>\d+)\s*)?\((?P<year>\d{4})\)'
				match = re.match(pattern, vol_num_year)
				if match:
					record_info['volume'] = match.group('volume').strip()
					record_info['year'] = match.group('year').strip()

					if match.group('number'):
						record_info['number'] = match.group('number').strip()

		return record_info


	def parse_identifiers(identifiers):
		record_info = {}

		for identifier in identifiers:
			if is_doi(identifier.text):
				record_info['doi'] = identifier.text
			elif is_ojs_article_url(identifier.text):
				record_info['fulltext_url'] = identifier.text
			else:
				if 'identifier' in record_info:
					record_info['identifier'].append(identifier.text)
				else:
					record_info['identifier'] = [identifier.text]

		return record_info


	def parse_single_xml(xml_file_path):
		tree = ET.parse(xml_file_path)
		root = tree.getroot()

		records_info = []
		records = root.findall('.//oai:record', namespaces=ns)
		for record in records:
			# Skip if record deleted
			status = record.find(".//oai:header", namespaces=ns).get("status")
			if status and status == "deleted":
				continue

			record_info = {}

			dc = record.find('./oai:metadata/oai_dc:dc', namespaces=ns)

			# Multi occ fields
			record_info['creators'] = [creator.text for creator in dc.findall('./dc:creator', namespaces=ns)]
			record_info['types'] = [type.text for type in dc.findall('./dc:type', namespaces=ns)]
			
			# Single occ fields
			record_info['id'] = record.find('./oai:header/oai:identifier', namespaces=ns).text
			record_info['date'] = dc.find('./dc:date', namespaces=ns).text if dc.find('./dc:date', namespaces=ns) is not None else None
			record_info['language'] = dc.find('./dc:language', namespaces=ns).text if dc.find('./dc:language', namespaces=ns) is not None else None

			# Handle identifier tag
			record_info.update(
				parse_identifiers(dc.findall('./dc:identifier', namespaces=ns))
			)

			# Handle source tag
			record_info.update(
				parse_sources(dc.findall('./dc:source', namespaces=ns))
			)

			# Multi occ fields with lang
			tags_with_lang = [
				{
					"name": 'titles',
					"xpath": './dc:title'
				},
				{
					"name": 'subjects',
					"xpath": './dc:subject'
				},
				{
					"name": 'abstract',
					"xpath": './dc:description'
				},
				{
					"name": 'publishers',
					"xpath": './dc:publisher'
				}
			]
			for tag_with_lang in tags_with_lang:
				# Collect both content and language
				tags = dc.findall(tag_with_lang['xpath'], namespaces=ns)
				if tags:
					record_info[tag_with_lang['name']] = [
						{
							'text': tag.text, 
							'_i': tag.get(f'{{{ns["xml"]}}}lang')
						} for tag in tags
					]

			records_info.append(record_info)

		return records_info


	# Constants
	MONGO_CONN_ID = 'mongo'
	LZ_COLLECTION = 'lz'
	TRANSFORM_COLLECTION = 'transform'

	# Task dependencies
	@task_group()
	def extract():
		records_info_task = parse_xml_files()
		setup_mongodb_task = setup_mongodb(MONGO_CONN_ID, LZ_COLLECTION)
		insert_output_task = insert_into_mongodb(records_info_task, MONGO_CONN_ID, LZ_COLLECTION)
		promote_to_transform = duplicate_collection(LZ_COLLECTION, TRANSFORM_COLLECTION, MONGO_CONN_ID)

		records_info_task >> insert_output_task
		setup_mongodb_task >> insert_output_task
		insert_output_task >> promote_to_transform

	@task_group()
	def transform():
		transform_lang = transform_language_field(MONGO_CONN_ID, TRANSFORM_COLLECTION)
		transform_date = transform_date_field(MONGO_CONN_ID, TRANSFORM_COLLECTION)
		transform_title = transform_title_field(MONGO_CONN_ID, TRANSFORM_COLLECTION)
		transform_subject = transform_subject_field(MONGO_CONN_ID, TRANSFORM_COLLECTION)
		transform_abstract = transform_abstract_field(MONGO_CONN_ID, TRANSFORM_COLLECTION)
		transform_publisher = transform_publisher_field(MONGO_CONN_ID, TRANSFORM_COLLECTION)
		transform_source = transform_source_field(MONGO_CONN_ID, TRANSFORM_COLLECTION)

	load = export_json_to_fiadmin(MONGO_CONN_ID, TRANSFORM_COLLECTION)

	extract() >> transform() >> load
	

# Accessing the generated DAG
xml_dag = import_ojs_xml()