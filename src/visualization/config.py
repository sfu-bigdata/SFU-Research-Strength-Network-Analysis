from enum import Enum
from config import VISUALIZATION_DATA_DIR

class VisualizationDataPaths(Enum):
    '''
    Enum class containing all the paths for compiled CSV outputs
    '''

    summary_node_information = VISUALIZATION_DATA_DIR.joinpath("summary_node_information.csv")
    summary_nodes_by_institution = VISUALIZATION_DATA_DIR.joinpath("summary_nodes_institution.csv")