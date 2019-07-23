import sys, os
import argparse
import lxml.etree as ET

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from utils import file_utils
from utils import mysql_utils
from utils import xml_utils

# modify server.xml
def modifyServerXml(tree):
	schemas = []

	for row in rows:
		schemas.append(row['schema'])

	for element in tree.iterfind('./user/property[name="schemas"]'):
		element.text = ','.join(schemas)

# modify schema.xml
def modifySchemaXml(tree):
	replaceElements(tree, "./schema", newSchemaElement)
	replaceElements(tree, "./dataNode", newDataNodeElement)
	replaceElements(tree, "./dataSource", newDataSourceElement)

# replace elements
def replaceElements(tree, xpath, newElement):
	newElements = []

	for row in rows:
		element = ET.XML(newElement(row))
		newElements.append(element)

	xml_utils.replaceElements(tree, xpath, newElements)	

def newSchemaElement(row):
	return ('\n\t<schema name="{}" dataNode="{}" tables="{}">'
			'\n\t</schema>\n').format(row['schema'], 
			row['data_node'], 
			row['tables'])

def newDataNodeElement(row):
	return ('\n\t<dataNode name="{}">'
			'\n\t\t<property name="dataSource">'
			'\n\t\t\t<dataSourceRef>{}</dataSourceRef>'
			'\n\t\t</property>\n\t'
			'</dataNode>\n').format(row['data_node'], 
			row['data_source'])

def newDataSourceElement(row):
	return ('\n\t<dataSource name="{}" type="mysql">'
			'\n\t\t<property name="location">'
			'\n\t\t\t<location>{}:{}/{}</location>'
			'\n\t\t</property>'
			'\n\t\t<property name="user">{}</property>'
			'\n\t\t<property name="password">{}</property>'
			'\n\t\t<property name="sqlMode">{}</property>'
			'\n\t</dataSource>\n').format(row['data_source'], 
			row['db_host'], 
			row['db_port'], 
			row['db_database'], 
			row['db_user'], 
			row['db_password'], 
			row['sql_mode'])

def modifyMapFile():
	lines = []
	index = 0

	for row in rows:
		lines.append('{}={}'.format(row['id'], index))
		index += 1

	file_utils.file_put_contents(os.path.join(args.file, 'mapfile.txt'), '\n'.join(lines))

parser = argparse.ArgumentParser(description='cobar config file generator')
parser.add_argument('-H','--host', help='cobar config db host, default=localhost', type=str, default='localhost', required=False)
parser.add_argument('-P','--port', help='cobar config db port, default=3306', type=int, default=3306, required=False)
parser.add_argument('-u','--user', help='cobar config db user, default=root', type=str, default='root', required=False)
parser.add_argument('-p','--password', help='cobar config db password, default=root', type=str, default='root', required=False)
parser.add_argument('-d','--database', help='cobar config db database name', type=str, required=True)
parser.add_argument('-f','--file', help='cobar config file path', type=str, required=True)
args = parser.parse_args()

rows = mysql_utils.query(args.host, 
	args.port, 
	args.user, 
	args.password, 
	args.database, 
	'''select id,`schema`,data_node,tables,data_source,db_host,db_port,db_database,db_user,db_password,sql_mode from cobar_config;''')

xml_utils.modifyXml(os.path.join(args.file, 'server.xml'), modifyServerXml, '<!DOCTYPE cobar:server SYSTEM "server.dtd">')
xml_utils.modifyXml(os.path.join(args.file, 'schema.xml'), modifySchemaXml, '<!DOCTYPE cobar:schema SYSTEM "schema.dtd">')

modifyMapFile()
