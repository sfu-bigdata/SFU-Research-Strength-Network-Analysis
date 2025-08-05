'''
Retrieve and format extracted data for later usage
'''

import math
from .client import Client
from config import NodeType, VISUALIZATION_DATA_DIR, SFU_RED, institution_abbreviations
from .config import VisualizationDataPaths, colors as config_colors
import pandas as pd
from src.graphdb.conf import ObjectNames
from src.graphdb.relationships import Relationships
from enum import Enum
from typing import Iterable
import panel as pn
from panel.pane import ECharts

class VisualizationData():

    def __init__(self):
        self.client = Client()

    def target_sfu(self):
        query = f"""
        MATCH (n: {NodeType.SFU_U15_institution.value})
        UNWIND keys(n) AS colName
        RETURN
            apoc.node.id(n) as nodeId,
            colName,
            n[colName] as colValue
        """
        
        df = self.client(query)
        df = df.pivot(
            index='nodeId',
            columns='colName',
            values='colValue'
        ).reset_index(drop=True)

        return df
    
    def summary_node_information(self):
        '''
        Get the counts of each object type stored in the database.
        '''

        query = """
        CALL apoc.meta.stats()
        YIELD labels, nodeCount, relCount
        RETURN labels, nodeCount, relCount
        """
        res = self.client(query)

        path = VisualizationDataPaths.summary_node_information.value
        print("Writing summary data to directory ", path)
        path.parent.mkdir(parents=True, exist_ok=True)
        
        res.to_csv(path, index=False)

    def summary_nodes_by_institution(self):
        '''
        Get some summary data for comparison between institutions 
        '''
        sfu_15 = ObjectNames[NodeType.SFU_U15_institution]
        afl = ObjectNames[NodeType.affiliated_institution]
        authorship = ObjectNames[NodeType.authorship]

        # lineage_root=TRUE since only looking at top level.
        query = f"""
        MATCH ({sfu_15.prefix}: {sfu_15.name} {{lineage_root: TRUE}})
        OPTIONAL MATCH ({sfu_15.prefix})<-[{afl.prefix}_{sfu_15.prefix}: {Relationships.RelationshipTypeMap[(NodeType.affiliated_institution, NodeType.SFU_U15_institution)]}]->({afl.prefix}:{afl.name})

        WITH {sfu_15.prefix}, {afl.prefix},
            CASE
                WHEN {afl.prefix} IS NULL THEN 0
                ELSE size([({afl.prefix})<-[:{Relationships.RelationshipTypeMap[(NodeType.authorship, NodeType.affiliated_institution)]}]-({authorship.prefix}:{authorship.name}) | 1])
            END AS authorship_count
        
        RETURN {sfu_15.prefix}, authorship_count
        """
        res = self.client(query)

        # Format the results, Node Objects need restructuring.
        # sfu_u15 = ObjectNames[NodeType.SFU_U15_institution]
        res[sfu_15.prefix] = [dict(Node) for Node in res[sfu_15.prefix]]
        flattened = pd.json_normalize(res[sfu_15.prefix])
        
        res = pd.concat([flattened, res.drop(columns=[sfu_15.prefix])], axis=1)
        sorted = res.sort_values(by='works_count', ascending=False)        

        path = VisualizationDataPaths.summary_nodes_by_institution.value
        print("Writing dataframe to directory ", path)
        path.parent.mkdir(parents=True, exist_ok=True)
        
        sorted.to_csv(path, index=False)
        
        return

class VisualizationType(Enum):
    BUBBLE_CHART = 0
    BAR_CHART = 1
    TREEMAP_CHART = 2

class GraphVisualization():

    def _create_bar_chart(
            dataframe: pd.DataFrame,
            metric_column: str):
        
            dataframe = dataframe.sort_values(by=metric_column, ascending=False)
            names = dataframe['display_name'].to_list()
            
            '''
            Add style + change to abbreviated format
            '''
            x_axis_data = [
                {'value': institution_abbreviations[value], 'textStyle': {'fontWeight': 'bold', 'color': SFU_RED}} if value == 'Simon Fraser University' else {'value': institution_abbreviations[value]} 
                for value in names
            ]

            series_data = [
                {
                    'value': row[metric_column],
                    'itemStyle': {
                        'color': SFU_RED
                    } 
                } if row['display_name'] == 'Simon Fraser University' else {
                    'value': row[metric_column]
                }
                for _, row in dataframe.iterrows()
            ]

            # Adjust for scaling based on data
            max_val = dataframe[metric_column].max()
            min_val = dataframe[metric_column].min()

            diff = max_val-min_val
            padding = diff*0.15 if diff else 0

            min_val = max(0, min_val-padding)

            if min_val:
                place = math.floor(math.log10(abs(min_val)))-1
                scaling = 10**place

                scaled_min_val = math.floor(min_val / scaling) * scaling
            else:
                scaled_min_val = 0
            
            echart_config = {
                'tooltip' : {},
                'legend' : {
                    'data': names                
                },
                'xAxis': {
                    'type': 'category',
                    'data': x_axis_data,
                    'axisLabel': {
                        'interval': 0,
                        'rotate': 90,
                    }
                },
                'yAxis': {
                    'type':'value',
                    'min': max(0, scaled_min_val)
                },
                'series': [
                    {
                        'name': metric_column,
                        'type': 'bar',
                        'data': series_data
                    }
                ],
                'grid': {
                    'bottom': '15%'
                }
            
            }

            return echart_config


    def _create_bubble_chart(dataframe : pd.DataFrame, 
                            metric_column : str):
        """
        Generates an interactive Packed Circle Chart using the ECharts graph type
        with a force-directed layout.
        """
        names = dataframe['display_name'].to_list()
        categories = [{'name':name} for name in names]
        category_map = {name: i for i, name in enumerate(names)}

        nodes = []
        
        colors = config_colors
        
        max_val_sqrt = math.sqrt(dataframe[metric_column].max())
        cleaned_title : str = metric_column.replace('_', ' ')

        for _, row in dataframe.iterrows():
            # Scale the size to a reasonable pixel range (e.g., 15 to 120 pixels)
            size = (math.sqrt(row[metric_column]) / max_val_sqrt) * 140 + 15
            
            nodes.append({
                'name': row['display_name'],
                'value': '{:,}'.format(row[metric_column]),
                'symbolSize': size,
                'category': category_map[row['display_name']]
            })

        echart_config = {
            'color': colors,
            'tooltip': {
                'trigger': 'item',
                'formatter': '<strong>{b}</strong>:<br/>'+cleaned_title.capitalize()+': {c}' # {b}=name, {c}=value
            },
            'series': [{
                'type': 'graph',
                'layout': 'force', # The key to the packing algorithm!
                'roam': True,      # Allow user to pan and zoom
                'data': nodes,
                'categories': categories,
                'label': {
                    'show': True,
                    'position': 'inside',
                    'formatter': '{b}',
                    'color': '#FFF',
                    'textShadowBlur': 2,
                    'textShadowColor': 'rgba(0, 0, 0, 1)'
                },
                'force': {
                    'repulsion': 420, # How much nodes push each other away
                    'gravity': 0.12,   # Pulls nodes toward the center
                    'friction': 0.5
                }
            }]
        }
        return echart_config

    def _create_treemap_chart(
            dataframe: pd.DataFrame,
            metric_column: str
    ):

        cleaned_title : str = metric_column.replace('_', ' ')
        '''
        'tooltip': {
            'trigger': 'item',
            'formatter': '<strong>{b}</strong>:<br/>'+cleaned_title.capitalize()+': {c}'
        },
        '''

        colors = config_colors

        treemap_data = [
            {
                'name': row['display_name'],
                'value': row[metric_column]
            } for _, row in dataframe.iterrows()
        ]

        echart_config = {
            'color': colors,
            'tooltip': {
                'trigger': 'item',
                'formatter': '<strong>{b}</strong>:<br/>'+cleaned_title.capitalize()+': {c}'
            },
            'series': [{
                'type': 'treemap',
                'data': treemap_data,
                'visualDimension': 1,
                'leafDepth': 1,
                'levels': [
                    {
                        'itemStyle': {
                            'borderColor': 'transparent',
                            'borderWidth': 0,
                            'gapWidth': 4,
                            'shadowColor': 'rgba(0,0,0,1)',
                            'shadowBlur': 10
                        }
                    },
                ],
                'legend': {},
                'breadcrumb': {
                    'show': False
                }
            }],
            'title': {},
        }

        return echart_config
        
        

    supported_graphs = {
        VisualizationType.BUBBLE_CHART : _create_bubble_chart,
        VisualizationType.BAR_CHART: _create_bar_chart,
        VisualizationType.TREEMAP_CHART: _create_treemap_chart
    }

    def create_graph(
        self,
        graph_type: VisualizationType,
        dataframe: pd.DataFrame,
        all_columns: Iterable[str]
    ) -> pn.Column:
        fx=self.supported_graphs.get(graph_type, None)

        if fx is None:
            raise Exception(f"Unsupported graph type: {graph_type}")

        
        metric_selector = pn.widgets.Select(name='Select Metric', options=all_columns, value=all_columns[0])
        
        # Bind the widget to the reusable function
        interactive_chart = pn.bind(fx, dataframe=dataframe, metric_column=metric_selector)

        chart = pn.Column(
            "# Packed Circle Chart (Correct Graph Method)",
            pn.Row(metric_selector),
            ECharts(interactive_chart, height=800, width=800)
        )

        return chart