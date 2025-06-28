from pathlib import Path
from os import environ
from enum import Enum

BASE_DIR = Path(__file__).parent
DATABASE_OUTPUT_DIR = BASE_DIR.joinpath(environ.get('DATABASE_IMPORT_DIR', 'database/db/imports'))
DATABASE_TEST_DATA_INPUT_DIR = BASE_DIR.joinpath('tests','data', 'output')

GEOGRAPHIC_DATA_LOCATION = BASE_DIR.joinpath('inputs', 'api', 'country_list.json')

class NodeType(Enum):
    institution = 'institution'
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
    none = 'none'

TableMap = {          
    NodeType.author.value : NodeType.author,
    NodeType.funder.value : NodeType.funder,
    NodeType.institution.value : NodeType.institution,
    NodeType.source.value: NodeType.source,
    NodeType.work.value : NodeType.work,
    NodeType.topic.value: NodeType.topic,
    NodeType.subfield.value: NodeType.subfield,
    NodeType.field.value: NodeType.field,
    NodeType.domain.value: NodeType.domain,
    NodeType.geographic.value: NodeType.geographic,
    NodeType.affiliated_institution.value : NodeType.affiliated_institution,
    NodeType.last_institution.value : NodeType.last_institution
}