from enum import Enum
from config import VISUALIZATION_DATA_DIR

class VisualizationDataPaths(Enum):
    '''
    Enum class containing all the paths for compiled CSV outputs
    '''

    summary_node_information = VISUALIZATION_DATA_DIR.joinpath("summary_node_information.csv")
    summary_nodes_by_institution = VISUALIZATION_DATA_DIR.joinpath("summary_nodes_institution.csv")


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