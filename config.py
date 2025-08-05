from pathlib import Path
from os import environ
from enum import Enum

BASE_DIR = Path(__file__).parent
DATABASE_OUTPUT_DIR = BASE_DIR.joinpath(environ.get('DATABASE_IMPORT_DIR', 'database/db/imports'))
DATABASE_TEST_DATA_INPUT_DIR = BASE_DIR.joinpath('tests','data', 'output')

GEOGRAPHIC_DATA_LOCATION = BASE_DIR.joinpath('inputs', 'api', 'country_list.json')

VISUALIZATION_DATA_DIR = BASE_DIR.joinpath('visualization_data')

class NodeType(Enum):
    SFU_U15_institution = 'SFU_U15_institution'
    author = "author"
    funder = 'funder'
    source = 'source'
    work = 'work'
    journal = 'journal'
    publisher = 'publisher'
    relationship = 'relationship'
    topic = 'topic'
    subfield = 'subfield'
    field = 'field'
    domain = 'domain'
    affiliated_institution = 'affiliated_institution'
    geographic = 'geographic'
    last_institution = 'last_institution'
    related_institution = 'related_institution'
    authorship = 'authorship'
    issn = 'issn'
    year = 'year'
    none = 'none'

TableMap = {          
    NodeType.author.value : NodeType.author,
    NodeType.authorship.value : NodeType.authorship,
    NodeType.funder.value : NodeType.funder,
    NodeType.SFU_U15_institution.value : NodeType.SFU_U15_institution,
    NodeType.source.value: NodeType.source,
    NodeType.work.value : NodeType.work,
    NodeType.topic.value: NodeType.topic,
    NodeType.subfield.value: NodeType.subfield,
    NodeType.field.value: NodeType.field,
    NodeType.domain.value: NodeType.domain,
    NodeType.geographic.value: NodeType.geographic,
    NodeType.affiliated_institution.value : NodeType.affiliated_institution,
    NodeType.last_institution.value : NodeType.last_institution,
    NodeType.related_institution.value : NodeType.related_institution,
    NodeType.issn.value : NodeType.issn,
    NodeType.year.value : NodeType.year
}

GRAPH_START_ID = ':START_ID'
GRAPH_END_ID = ':END_ID'
RELATIONSHIP_SEPARATOR = '_'

SFU_RED = '#CC0633'

'''
Abbreviations for display name strings
'''
institution_abbreviations = {
    'University of British Columbia': 'UBC',
    'Simon Fraser University': 'SFU',
    'University of Toronto': 'UofT',
    'McGill University': 'McGill',
    'University of Alberta': 'UAlberta',
    'University of Calgary': 'UCalgary',
    'McMaster University': 'McMaster',
    'Université de Montréal': 'UofM',
    'Western University': 'UWO',
    'University of Ottawa': 'uOttawa',
    'University of Waterloo': 'UWaterloo',
    'Queen\'s University': 'Queen\'s',
    'Université Laval': 'ULaval',
    'Dalhousie University': 'Dalhousie',
    'University of Manitoba': 'UManitoba',
    'University of Saskatchewan': 'USask'

}