from enum import Enum
from config import VISUALIZATION_DATA_DIR

class VisualizationDataPaths(Enum):
    '''
    Enum class containing all the paths for compiled CSV outputs
    '''

    summary_node_information = VISUALIZATION_DATA_DIR.joinpath("summary_node_information.csv")
    summary_nodes_by_institution_works = VISUALIZATION_DATA_DIR.joinpath("summary_nodes_institution.csv")
    summary_nodes_by_institution_authors = VISUALIZATION_DATA_DIR.joinpath("summary_nodes_authors.csv")
    summary_counts_by_year = VISUALIZATION_DATA_DIR.joinpath("summary_counts_by_year.csv")
    
    work_analysis = VISUALIZATION_DATA_DIR.joinpath('work_analysis.csv')
    author_analysis = VISUALIZATION_DATA_DIR.joinpath('author_analysis.csv')
    
colors =[
    '#0077BB',  # Blue
    '#EE7733',  # Orange
    '#009988',  # Teal
    '#EE3377',  # Magenta
    '#CCBB44',  # Yellow
    '#33BBEE',  # Cyan
    '#BB4477',  # Rose
    '#AA4499',  # Purple
    '#DDAA33',  # Old Gold
    '#77AADD',  # Light Blue
    '#44AA99',  # Green-Cyan
    '#BB5566',  # Dark Rose
    '#882255',  # Wine
    '#AA4466',  # Strong Rose
    '#66AB9D',  # Dull Teal
    '#99DDFF'   # Pale Cyan
]

GRAPH_WIDTH = 750
GRAPH_HEIGHT = 550
SFU_TARGET_INSTITUTION_ID = 'I18014758' 