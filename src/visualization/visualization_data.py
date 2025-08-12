'''
Retrieve and format extracted data for later usage
'''

from ast import literal_eval
import math
from .client import Client
from config import NodeType, VISUALIZATION_DATA_DIR, SFU_RED, institution_abbreviations
from .config import VisualizationDataPaths, colors as config_colors, GRAPH_HEIGHT, GRAPH_WIDTH, SFU_TARGET_INSTITUTION_ID
import pandas as pd
from src.graphdb.conf import ObjectNames
from src.graphdb.relationships import Relationships
from enum import Enum
from typing import Iterable
import panel as pn
from panel.pane import ECharts
import numpy as np

default_colors = [
    '#5470c6', '#91cc75', '#fac858', '#ee6666', '#73c0de', '#3ba272', 
    '#fc8452', '#9a60b4', '#ea7ccc', '#2c3e50', '#d35400', '#8e44ad',
    '#16a085', '#f1c40f', '#c0392b', '#1abc9c'
]

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
        author = ObjectNames[NodeType.author]
        paper = ObjectNames[NodeType.work]

        # lineage_root=TRUE since only looking at top level.
        # Remember that authors not related to the U15+SFU have no been included into the data set
        # Authorships without that author will simply just point to the institution of note     
        '''
        query = f"""
        MATCH ({sfu_15.prefix}: {sfu_15.name} {{lineage_root: TRUE}})
        OPTIONAL MATCH ({sfu_15.prefix})<-[{afl.prefix}_{sfu_15.prefix}: {Relationships.RelationshipTypeMap[(NodeType.affiliated_institution, NodeType.SFU_U15_institution)]}]-({afl.prefix}:{afl.name})
        WITH {sfu_15.prefix}, {afl.prefix}
        OPTIONAL MATCH ({afl.prefix})<-[:{Relationships.RelationshipTypeMap[(NodeType.author, NodeType.affiliated_institution)]}]-({author.prefix}:{author.name})
        WITH {sfu_15.prefix}, {afl.prefix}, count(DISTINCT {author.prefix}) as author_count
        OPTIONAL MATCH ({afl.prefix})<-[:{Relationships.RelationshipTypeMap[(NodeType.authorship, NodeType.affiliated_institution)]}]-({authorship.prefix}:{authorship.name})
        OPTIONAL MATCH ({authorship.prefix})-[:{Relationships.RelationshipTypeMap[(NodeType.authorship),(NodeType.work)]}]->({paper.prefix}: {paper.name})
        WITH {sfu_15.prefix}, author_count, count(DISTINCT {authorship.prefix}) as authorship_count, {paper.prefix}
        OPTIONAL MATCH ({paper.prefix})<-[:{Relationships.RelationshipTypeMap[(NodeType.authorship), (NodeType.work)]}]-({'t_'+ authorship.prefix}: {authorship.name})
        RETURN {sfu_15.prefix}, author_count, authorship_count, count(DISTINCT {paper.prefix}) as openalex_paper_count, count(DISTINCT {'t_'+authorship.prefix}) as total_author_count;
        """
        '''

        query = f"""
        MATCH ({sfu_15.prefix}:{sfu_15.name} {{lineage_root: TRUE}})

        CALL ({sfu_15.prefix}) {{
            OPTIONAL MATCH ({sfu_15.prefix})<-[:{Relationships.RelationshipTypeMap[(NodeType.affiliated_institution),(NodeType.SFU_U15_institution)]}]-({afl.prefix}:{afl.name})<-[:{Relationships.RelationshipTypeMap[(NodeType.author, NodeType.affiliated_institution)]}]-({author.prefix}:{author.name})
            RETURN count(DISTINCT {author.prefix}) AS author_count
        }}

        CALL({sfu_15.prefix}) {{
            OPTIONAL MATCH ({sfu_15.prefix})<-[:{Relationships.RelationshipTypeMap[(NodeType.affiliated_institution),(NodeType.SFU_U15_institution)]}]-({afl.prefix}:{afl.name})<-[:{Relationships.RelationshipTypeMap[(NodeType.authorship), (NodeType.affiliated_institution)]}]-({authorship.prefix}:{authorship.name})-[:{Relationships.RelationshipTypeMap[(NodeType.authorship),(NodeType.work)]}]->({paper.prefix}:{paper.name})
            RETURN count(DISTINCT {authorship.prefix}) AS authorship_count, count(DISTINCT {paper.prefix}) AS openalex_paper_count, collect(DISTINCT {paper.prefix}) as works
        }}

        // Unwind the collected works to count total authorships
        UNWIND works as work
        OPTIONAL MATCH (work)<-[:{Relationships.RelationshipTypeMap[(NodeType.authorship),(NodeType.work)]}]-({'t_'+authorship.prefix}:{authorship.name})

        RETURN I, author_count, authorship_count, openalex_paper_count, count(DISTINCT {'t_'+authorship.prefix}) as total_author_count
        """

        res = self.client(query)

        # Format the results, Node Objects need restructuring.
        # sfu_u15 = ObjectNames[NodeType.SFU_U15_institution]
        res[sfu_15.prefix] = [dict(Node) for Node in res[sfu_15.prefix]]
        flattened = pd.json_normalize(res[sfu_15.prefix])
        
        res = pd.concat([flattened, res.drop(columns=[sfu_15.prefix])], axis=1)
        # There are only authors for SFU_15 affiliated, so authorships all have an affiliated author
        res['local_authorship_ratio'] = np.floor(res['authorship_count']/res['author_count']*100)/100
        # The total authorships for affiliated works, by getting all authorships related to that work, non-institutional authors
        res['total_authorship_ratio'] = np.floor(res['total_author_count']/res['openalex_paper_count']*100)/100
        sorted = res.sort_values(by='works_count', ascending=False)        

        path = VisualizationDataPaths.summary_nodes_by_institution_works.value
        print("Writing dataframe to directory ", path)
        path.parent.mkdir(parents=True, exist_ok=True)
        
        sorted.to_csv(path, index=False)

        return

    def summary_nodes_by_author(self):
        '''
        Get information pertaining to authors by institution
        No need for extended data, because that will be done more in-depth later
        '''

        author = ObjectNames[NodeType.author]
        sfu_15 = ObjectNames[NodeType.SFU_U15_institution]
        afl = ObjectNames[NodeType.affiliated_institution]

        query = f"""
            MATCH (all_{author.prefix}:{author.name})
            WHERE all_{author.prefix}.works_count > 0 AND all_{author.prefix}.i10_index IS NOT NULL
            WITH avg(toFloat(all_{author.prefix}.i10_index) / all_{author.prefix}.works_count) AS global_mean_efficiency,
                stdev(toFloat(all_{author.prefix}.i10_index) / all_{author.prefix}.works_count) AS global_stdev_efficiency

            MATCH ({sfu_15.prefix}:{sfu_15.name} {{lineage_root: TRUE}})
            OPTIONAL MATCH ({sfu_15.prefix})<-[{afl.prefix}_{sfu_15.prefix}: {Relationships.RelationshipTypeMap[(NodeType.affiliated_institution, NodeType.SFU_U15_institution)]}]-({afl.prefix}:{afl.name})
            MATCH ({afl.prefix})<-[:{Relationships.RelationshipTypeMap[(NodeType.author, NodeType.affiliated_institution)]}]-({author.prefix}:{author.name})
            WITH DISTINCT {sfu_15.prefix}, {author.prefix}, global_mean_efficiency, global_stdev_efficiency
            WHERE {author.prefix}.works_count > 0 AND {author.prefix}.i10_index IS NOT NULL


            WITH {sfu_15.prefix}, {author.prefix},
                CASE
                WHEN global_stdev_efficiency > 0 THEN
                    ( (toFloat({author.prefix}.i10_index) / {author.prefix}.works_count) - global_mean_efficiency) / global_stdev_efficiency
                ELSE 0 // Avoid division by zero if all authors globally have the same efficiency
                END AS adjusted_i10_score

            RETURN
                {sfu_15.prefix}.id AS id,
                {sfu_15.prefix}.display_name AS display_name,

                // The new adjusted score. This will NOT be zero anymore.
                avg(adjusted_i10_score) AS mean_adjusted_i10_score,

                // Your original metrics, calculated only for the authors at this institution
                avg({author.prefix}.cited_by_count) AS mean_cited_by_count,
                avg({author.prefix}.h_index) AS mean_h_index,
                avg({author.prefix}.i10_index) AS mean_i10_index,
                avg({author.prefix}.works_count) AS mean_works_count,
                avg({author.prefix}.`2yr_mean_citedness`) AS mean_2yr_mean_citedness,
                apoc.agg.median({author.prefix}.cited_by_count) AS median_cited_by_count,
                apoc.agg.median({author.prefix}.h_index) AS median_h_index,
                apoc.agg.median({author.prefix}.i10_index) AS median_i10_index,
                apoc.agg.median({author.prefix}.works_count) AS median_works_count,
                apoc.agg.median({author.prefix}.`2yr_mean_citedness`) AS median_2yr_mean_citedness        
        """
        res = self.client(query=query)

        # Citations per work using both median and mean values
        res['median_citations_per_work'] = (res['median_cited_by_count']/res['median_works_count'])
        res['mean_citations_per_work'] = res['mean_cited_by_count']/res['mean_works_count']

        res['mean_i10_to_works_ratio'] = res['mean_i10_index']/res['mean_works_count']
        res['median_i10_to_works_ratio'] = res['median_i10_index']/res['median_works_count']

        res['i10_harmonic_mean'] = (2*res['mean_i10_index']*res['mean_works_count'])/(res['mean_i10_index']+res['mean_works_count'])

        path = VisualizationDataPaths.summary_nodes_by_institution_authors.value
        print("Writing dataframe to directory ", path)
        path.parent.mkdir(parents=True, exist_ok=True)
        
        res.to_csv(path, index=False)

        return

    def summary_counts_by_year(self):
        '''
        Get the number of citations and works by year per institution
        '''

        sfu15 = ObjectNames[NodeType.SFU_U15_institution]
        year = ObjectNames[NodeType.year]
        relName = sfu15.prefix+'_'+year.prefix
        
        query = f"""
            MATCH ({sfu15.prefix}:{sfu15.name} {{lineage_root: TRUE}})-[{relName}:{Relationships.RelationshipTypeMap[(NodeType.SFU_U15_institution),(NodeType.year)]}]->({year.prefix}:{year.name})
            RETURN {sfu15.prefix}.id as id, {sfu15.prefix}.display_name as display_name,  {year.prefix}.id as year, {relName}.cited_by_count as cited_by_count, {relName}.works_count as works_count;
        """

        res = self.client(query)

        # Do not include the current year as it is in progress.
        from datetime import datetime
        res = res[res['year'] < datetime.now().year]

        path = VisualizationDataPaths.summary_counts_by_year.value
        print("Writing summary data to directory ", path)
        path.parent.mkdir(parents=True, exist_ok=True)
        
        res.sort_values(by=['id','year'], inplace=True)
        res.to_csv(path, index=False)
        
        return

    def summary_topics_counts(self):
        pass

    def works_analysis(self):
        '''
        Get more in depth information relating to works by institution
        '''

        sfu_15 = ObjectNames[NodeType.SFU_U15_institution]
        afl = ObjectNames[NodeType.affiliated_institution]
        authorship = ObjectNames[NodeType.authorship]
        author = ObjectNames[NodeType.author]
        paper = ObjectNames[NodeType.work]

        query = f"""
            MATCH ({sfu_15.prefix}:{sfu_15.name} {{lineage_root: TRUE}})
            OPTIONAL MATCH ({sfu_15.prefix})<-[:{Relationships.RelationshipTypeMap[(NodeType.affiliated_institution),(NodeType.SFU_U15_institution)]}]-({afl.prefix}:{afl.name})<-[:{Relationships.RelationshipTypeMap[(NodeType.author, NodeType.affiliated_institution)]}]-({author.prefix}:{author.name})
            OPTIONAL MATCH ({sfu_15.prefix})<-[:{Relationships.RelationshipTypeMap[(NodeType.affiliated_institution),(NodeType.SFU_U15_institution)]}]-({afl.prefix}:{afl.name})<-[:{Relationships.RelationshipTypeMap[(NodeType.authorship), (NodeType.affiliated_institution)]}]-({authorship.prefix}:{authorship.name})-[:{Relationships.RelationshipTypeMap[(NodeType.authorship),(NodeType.work)]}]->({paper.prefix}:{paper.name})

            WITH DISTINCT {sfu_15.prefix}, {paper.prefix}
            
            RETURN
                {sfu_15.prefix}.id as id,
                {sfu_15.prefix}.display_name as display_name,
                avg({paper.prefix}.countries_distinct_count) as avg_distinct_countries,
                avg({paper.prefix}.citation_normalized_percentile) as avg_citation_normalized_percentile,
                avg({paper.prefix}.fwci) as avg_fwci,
                avg({paper.prefix}.institutions_distinct_count) as avg_distinct_institutions,
                avg({paper.prefix}.apc_paid) as avg_apc_paid,
                
                apoc.coll.frequenciesAsMap(collect({paper.prefix}.is_oa)) as is_open_access,
                apoc.coll.frequenciesAsMap(collect({paper.prefix}.type)) as type,
                apoc.coll.frequenciesAsMap(collect({paper.prefix}.publication_year)) as total_by_publication_year,
                apoc.coll.frequenciesAsMap(
                    collect(
                        CASE
                            WHEN {paper.prefix}.is_oa = true THEN {paper.prefix}.oa_status
                            ELSE null
                        END
                    )
                ) as open_access_status

        """

        res = self.client(query)
        
        path = VisualizationDataPaths.work_analysis.value
        print("Writing dataframe to directory ", path)
        path.parent.mkdir(parents=True, exist_ok=True)

        res.to_csv(path, index=False)

        return

    def authors_analysis(self):
        '''
        Affiliated authors by year
        Cited by per year
        Works Count per year
        '''

        sfu_15 = ObjectNames[NodeType.SFU_U15_institution]
        afl = ObjectNames[NodeType.affiliated_institution]
        author = ObjectNames[NodeType.author]
        year = ObjectNames[NodeType.year]
        author_afl_rel = author.prefix+'_'+afl.prefix
        author_year_rel = author.prefix+'_'+year.prefix

        query = f"""
        MATCH ({sfu_15.prefix}: {sfu_15.name} {{lineage_root: TRUE}})
        OPTIONAL MATCH ({sfu_15.prefix})<-[:{Relationships.RelationshipTypeMap[(NodeType.affiliated_institution),(NodeType.SFU_U15_institution)]}]-({afl.prefix}:{afl.name})<-[{author_afl_rel}:{Relationships.RelationshipTypeMap[(NodeType.author, NodeType.affiliated_institution)]}]-({author.prefix}:{author.name})-[{author_year_rel}:{Relationships.RelationshipTypeMap[(NodeType.author), (NodeType.year)]}]->({year.prefix}:{year.name})

        WITH {sfu_15.prefix},
            {year.prefix}.id as year,
            sum({author_year_rel}.works_count) as total_works,
            sum({author_year_rel}.cited_by_count) as total_citations

        WITH {sfu_15.prefix}, collect(
            CASE
                WHEN year IS NOT NULL THEN {{
                    year: year,
                    works_count: total_works,
                    cited_by_count: total_citations
                }}
            END
        ) as author_counts_by_year
       
        OPTIONAL MATCH ({sfu_15.prefix})<-[:{Relationships.RelationshipTypeMap[(NodeType.affiliated_institution),(NodeType.SFU_U15_institution)]}]-(:{afl.name})<-[{author_afl_rel}:{Relationships.RelationshipTypeMap[(NodeType.author, NodeType.affiliated_institution)]}]-({author.prefix}_2:{author.name})
        
        UNWIND {author_afl_rel}.years AS affiliation_year

        WITH {sfu_15.prefix}, author_counts_by_year, affiliation_year, count({author_afl_rel}) as count_per_year

        WITH {sfu_15.prefix}, author_counts_by_year, apoc.map.fromPairs(collect([affiliation_year, count_per_year])) as affiliated_per_year

        RETURN 
            {sfu_15.prefix}.id as id,
            {sfu_15.prefix}.display_name as display_name,
            author_counts_by_year,
            affiliated_per_year
        """

        res = self.client(query)
        
        path = VisualizationDataPaths.author_analysis.value
        print("Writing dataframe to directory ", path)
        path.parent.mkdir(parents=True, exist_ok=True)

        res.to_csv(path, index=False)
        
        return



class VisualizationType(Enum):
    BUBBLE_CHART = 'Bubble Chart'
    BAR_CHART = 'Bar Chart'
    TREEMAP_CHART = 'Treemap Chart'
    THEME_RIVER_CHART = 'Theme River Chart'
    BAR_RACE_CHART = 'Bar Race Chart'
    STACKED_AREA_CHART = 'Stacked Area Chart'
    SMOOTHED_LINE_CHART = 'Smoothed Line Chart'
    PIE_CHART = 'Pie Chart'

class GraphVisualization():

    def _create_pie_chart(
        dataframe: pd.DataFrame, 
        metric_column: str,
        **additional_filters
    ):

        for k, v in additional_filters.items():
            dataframe = dataframe[dataframe[k]==v]

        
        if dataframe[metric_column].dtype == 'object':
            dictionary = dataframe.head(1).iloc[0][metric_column]
            sorted_items = dict(sorted(dictionary.items()))
            series_data = [
                 {'name': name, 'value': value}
                 for name, value in sorted_items.items()
            ]
            legend_data = [key for key in sorted_items.keys()]

        else:
            name_col = 'display_name'
            # ECharts expects the data for a pie chart to be a list of dictionaries,
            # with each dictionary having 'name' and 'value' keys.
            series_data = [
                {'name': row[name_col], 'value': row[metric_column]}
                for _, row in dataframe.iterrows()
            ]
            # The legend requires a simple list of the names for each category.
            legend_data = dataframe[name_col].to_list()
        

        echart_config = {
            # The tooltip is configured to show data when a user hovers over a slice ('item').
            # The formatter creates a clean label showing the series name, slice name, value, and percentage.
            'tooltip': {
                'trigger': 'item',
                'formatter': '{a} <br/>{b}: {c} ({d}%)'
            },
            
            # The legend displays the names of the different categories.
            # It's oriented vertically on the left to leave ample space for the chart itself.
            'legend': {
                'orient': 'vertical',
                'left': 'left',
                'data': legend_data
            },
            
            # The 'series' is the primary object that defines the chart itself.
            'series': [
                {
                    'name': '',
                    'color': default_colors,
                    'type': 'pie',
                    # Using an array for the radius creates a doughnut chart. 
                    # The first value is the inner radius, the second is the outer radius.
                    'radius': ['40%', '70%'],
                    'avoidLabelOverlap': False,
                    # This adds a visual effect when a user hovers over a slice,
                    # making the chart more interactive.
                    'emphasis': {
                        'label': {
                            'show': True,
                            'fontSize': '20',
                            'fontWeight': 'bold'
                        }
                    },
                    # Disables the default labels on slices to keep the chart clean;
                    # information is available via the tooltip and legend.
                    'label': {
                        'show': False,
                        'position': 'center'
                    },
                    'labelLine': {
                        'show': False
                    },
                    # Assign the processed data to the chart.
                    'data': series_data
                }
            ]
        }
        
        return echart_config

    def _create_smoothed_line_chart(dataframe: pd.DataFrame, 
                                metric_column: str,
                                 **additional_filters
                                ):
        """
        Generates an ECharts configuration for a smoothed, stacked line chart
        with a tooltip sorted by value.
        """
        years = sorted(dataframe['year'].unique())
        universities = sorted(dataframe['display_name'].unique().tolist())
        
        colors = [
            '#5470c6', '#91cc75', '#fac858', '#ee6666', '#73c0de', '#3ba272', 
            '#fc8452', '#9a60b4', '#ea7ccc', '#2c3e50', '#d35400', '#8e44ad',
            '#16a085', '#f1c40f', '#c0392b', '#1abc9c'
        ]

        series_list = []
        for i, uni in enumerate(universities):
            df_uni = dataframe[dataframe['display_name'] == uni].sort_values(by='year')
            metric_by_year = pd.Series(df_uni[metric_column].values, index=df_uni.year).to_dict()
            data_points = [metric_by_year.get(year, 0) for year in years]

            series_list.append({
                'name': uni,
                'type': 'line',
                'smooth': True,  # *** THIS IS THE ONLY CHANGE NEEDED ***
                'emphasis': {
                    'focus': 'series'
                },
                'data': data_points
            })


        echart_config = {
            'title': {},
            'color': colors,
            'tooltip': {
                'trigger': 'axis',
                'axisPointer': {
                    'type': 'cross',
                    'label': { 'backgroundColor': '#6a7985' }
                },
                'order': 'valueDesc'
            },
            'legend': {
                'data': universities,
                'top': 'bottom',
                'type': 'scroll'
            },
            'grid': {
                'left': '3%',
                'right': '4%',
                'bottom': '10%',
                'containLabel': True
            },
            'xAxis': [
                {
                    'type': 'category',
                    'boundaryGap': False,
                    'data': [str(y) for y in years]
                }
            ],
            'yAxis': [ { 'type': 'value' } ],
            'series': series_list
        }
        
        return echart_config

    def _create_stacked_area_chart(dataframe: pd.DataFrame, 
                               metric_column: str,
                                **additional_filters
                               ):
        """
        Generates an ECharts configuration for a stacked area chart.
        """
        # Get a sorted list of unique years for the x-axis
        years = sorted(dataframe['year'].unique())
        
        # Get a sorted list of unique universities for the series and legend
        universities = sorted(dataframe['display_name'].unique().tolist())
        
        # Define a color palette
        colors = [
            '#5470c6', '#91cc75', '#fac858', '#ee6666', '#73c0de', '#3ba272', 
            '#fc8452', '#9a60b4', '#ea7ccc', '#2c3e50', '#d35400', '#8e44ad',
            '#16a085', '#f1c40f', '#c0392b', '#1abc9c'
        ]

        # -- Create the list of series, one for each university --
        series_list = []
        for i, uni in enumerate(universities):
            # Filter data for the current university
            df_uni = dataframe[dataframe['display_name'] == uni].sort_values(by=['year'])
            
            # Create a dictionary mapping year to value for quick lookup
            metric_by_year = pd.Series(df_uni[metric_column].values, index=df_uni.year).to_dict()

            # Build the data list, ensuring a value for every year in the master list
            # Use 0 or None if a university has no data for a specific year
            data_points = [metric_by_year.get(year, 0) for year in years]

            series_list.append({
                'name': uni,
                'type': 'line',
                'stack': 'Total',  # This key makes the areas stack on top of each other
                'areaStyle': {},   # Enables the color fill under the line
                'emphasis': {
                    'focus': 'series'
                },
                'data': data_points
            })

        # -- Assemble the final ECharts configuration --
        echart_config = {
            'title': {},
            'color': colors,
            'tooltip': {
                'trigger': 'axis', # Shows a tooltip for the vertical line at each x-axis point
                'axisPointer': {
                    'type': 'cross',
                    'label': {
                        'backgroundColor': '#6a7985'
                    }
                },
                'order':'valueDesc'
            },
            'legend': {
                'data': universities,
                'top': 'bottom',
                'type': 'scroll'
            },
            'grid': {
                'left': '3%',
                'right': '4%',
                'bottom': '10%', # Increase bottom margin for the scrollable legend
                'containLabel': True
            },
            'xAxis': [
                {
                    'type': 'category',
                    'boundaryGap': False, # No padding at the start/end of the axis
                    'data': [str(y) for y in years]
                }
            ],
            'yAxis': [
                {
                    'type': 'value'
                }
            ],
            'series': series_list
        }
        
        return echart_config

    def _create_bar_race_chart(dataframe: pd.DataFrame, 
                           metric_column: str,
                            **additional_filters
                           ):
        """
        Generates an ECharts configuration for a bar race chart.
        """
        # Get a sorted list of unique years for the timeline
        years = sorted(dataframe['year'].unique())
        
        # Find the global maximum value for a consistent x-axis scale
        global_max_value = dataframe[metric_column].max()
        
        # Get a stable list of all universities to assign consistent colors
        all_universities = sorted(dataframe['display_name'].unique().tolist())
        
        # Define a color palette
        colors = [
            '#5470c6', '#91cc75', '#fac858', '#ee6666', '#73c0de', '#3ba272', 
            '#fc8452', '#9a60b4', '#ea7ccc', '#2c3e50', '#d35400', '#8e44ad',
            '#16a085', '#f1c40f', '#c0392b', '#1abc9c'
        ]
        
        # Create a mapping from university name to a color
        color_map = {uni: colors[i % len(colors)] for i, uni in enumerate(all_universities)}

        # -- Create the list of 'options', one for each year --
        timeline_options = []
        for year in years:
            # Filter data for the current year and sort by rank (descending)
            df_year = dataframe[dataframe['year'] == year].sort_values(
                by=metric_column, ascending=True
            ).reset_index(drop=True)
            
            # Build the data for the bar series for this specific year
            series_data = []
            for index, row in df_year.iterrows():
                series_data.append({
                    'value': row[metric_column],
                    'itemStyle': {
                        'color': color_map.get(row['display_name'])
                    }
                })
            
            # Create the full option dictionary for this year's frame
            option = {
                'series': {
                    'data': series_data,
                },
                'yAxis': {
                    'data': df_year['display_name'].tolist() # The y-axis labels MUST be sorted for this year
                }
            }
            timeline_options.append(option)
            
        # -- Create the baseOption with settings common to all frames --
        base_option = {
            'timeline': {
                'axisType': 'category',
                'autoPlay': False,
                'playInterval': 1000, # ms
                'data': [str(y) for y in years], # The labels on the timeline slider
                'label': { 'formatter': '{value}' },
                'orient': 'vertical',
                'inverse': True,
                'right': 0,
                'top': 'center',
                'bottom': 20,
                'width': 55,
                'checkpointStyle': { 'color': '#e43c59' },
                'controlStyle': { 'showPlayBtn': True },
                'tooltip': {
                    'formatter': '{b}'
                }
            },
            'baseOption': { # Nested baseOption for timeline
                'title': {},
                'tooltip': {
                    'trigger': 'item',
                    'formatter': '{b}: {c}' # b: name, c: value
                },
                'grid': {
                    'containLabel': True,
                    'left': 30,
                    'right': 150 # Make space for labels and timeline
                },
                'xAxis': {
                    'type': 'value',
                    'max': global_max_value,
                    'min': 0,
                    'axisLabel': {'show': True},
                    'splitLine': {'show': False}
                },
                'yAxis': {
                    'type': 'category',
                    'inverse': True, # Ranks from top to bottom
                    'axisLabel': {'show': True},
                    'animationDuration': 300,
                    'animationDurationUpdate': 300,
                },
                'series': [{
                    'type': 'bar',
                    'label': {
                        'show': True,
                        'position': 'right',
                        'formatter': '{c}',
                        'valueAnimation': True
                    },
                    'encode': {
                        'x': metric_column,
                        'y': 'display_name'
                    }
                }],
                'animationDuration': 0,
                'animationDurationUpdate': 1000, # Speed of bar transitions
                'animationEasing': 'linear',
                'animationEasingUpdate': 'linear'
            },
            'options': timeline_options
        }
        
        return base_option

    def _create_theme_river_chart(
            dataframe: pd.DataFrame,
            metric_column: str,
             **additional_filters
    ):
        names = dataframe['display_name'].to_list()
        dataframe = dataframe.sort_values(by=metric_column, ascending=False, inplace=False)

        themedata = []

        for _, row in dataframe.iterrows():
            date_str = f"{row['year']}"

            themedata.append([
                date_str,
                row[metric_column],
                row['display_name']
            ])

        cleaned_title: str = metric_column.replace('_', ' ')

        echart_config = {
            'tooltip': {
                'trigger': 'axis',
                'axisPointer': {
                    'type': 'line',
                    'lineStyle': {
                        'color': 'rgba(0,0,0,0.1)',
                        'width': 1,
                        'type': 'solid'
                    }
                }
            },
            'legend': {
                'data': names,
                'bottom': 15,
                'type': 'scroll'

            },
            'singleAxis': {
                'top': 80,
                'bottom': 80,
                'axisTick': {},
                'axisLabel': {},
                'type': 'time',
                'axisPointer': {
                    'animation': True,
                    'label': {
                        'show': True
                    }
                }
            },
            'series': [
                {
                    'type': 'themeRiver',
                    'emphasis': {
                        'disabled': True
                    },
                    'label': {
                        'show': False
                    },
                    'data': themedata
                }
            ]
        }

        return echart_config

    def _create_bar_chart(
            dataframe: pd.DataFrame,
            metric_column: str,
             **additional_filters
             ):
        
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

            if min_val > 0:
                min_val = max(0, min_val-padding)

            if min_val != 0:
                place = math.floor(math.log10(abs(min_val)))-1
                scaling = 10**place
                min_val = round(math.floor(min_val // scaling) * scaling, 2)
            
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
                    'min': min_val
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
                            metric_column : str,
                             **additional_filters
                             ):
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
            size = (math.sqrt(abs(row[metric_column])) / max_val_sqrt) * 140 + 15
            
            nodes.append({
                'name': row['display_name'],
                'value': '{:,}'.format(row[metric_column]),
                'symbolSize': size,
                'category': category_map[row['display_name']],
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
            metric_column: str,
             **additional_filters
    ):
        
        colors = config_colors
        for k, v in additional_filters.items():
            dataframe = dataframe[dataframe[k]==v]

        cleaned_title : str = metric_column.replace('_', ' ')
        
        '''
        'tooltip': {
            'trigger': 'item',
            'formatter': '<strong>{b}</strong>:<br/>'+cleaned_title.capitalize()+': {c}'
        },
        '''

        if dataframe[metric_column].dtype == 'object':
            dictionary = dataframe.head(1).iloc[0][metric_column]
            sorted_items = dict(sorted(dictionary.items()))
            treemap_data = [
                 {'name': name, 'value': value}
                 for name, value in sorted_items.items()
            ]
        else:
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
        VisualizationType.TREEMAP_CHART: _create_treemap_chart,
        VisualizationType.THEME_RIVER_CHART: _create_theme_river_chart,
        VisualizationType.BAR_RACE_CHART: _create_bar_race_chart,
        VisualizationType.STACKED_AREA_CHART: _create_stacked_area_chart,
        VisualizationType.SMOOTHED_LINE_CHART: _create_smoothed_line_chart,
        VisualizationType.PIE_CHART : _create_pie_chart
    }
    
    @pn.cache
    def create_graph(
        self,
        graph_type: VisualizationType,
        dataframe: pd.DataFrame,
        all_columns: Iterable[str],
        height: int = GRAPH_HEIGHT,
        width: int = GRAPH_WIDTH,
        additional_filters: Iterable[str] = []
        ) -> pn.Column:
        fx=self.supported_graphs.get(graph_type, None)

        if fx is None:
            raise Exception(f"Unsupported graph type: {graph_type}")


        '''
        Change the metrics into something more readable
        '''
        def clean_options(all_columns):
            options = {}

            # Have to explicitly loop through because of difficulties with Panel
            for column in all_columns:
                split = column.split('_')
                res = ''

                for word in split:
                    res+=' '+word.capitalize() if res else word.capitalize()

                options[res] = column
            return options
        
        metric_selector = pn.widgets.Select(name='Select Metric', options=clean_options(all_columns), value=all_columns[0])
        
        additional_selectors = {}
        if additional_filters:
            for filter in additional_filters:

                # If using id, just show the display name.
                if filter == 'id':
                    values = dict(zip(dataframe['display_name'], dataframe['id']))
                else:
                    values = clean_options(dataframe[filter].unique())

                filter_selector = pn.widgets.Select(name=f'''Filter: {filter.capitalize() if filter != 'id' else 'Name'}''',
                                                    options=values,
                                                    value=next(iter(values.values())))
                additional_selectors[filter] = filter_selector

        # Bind the widget to the reusable function
        interactive_chart = pn.bind(fx, 
                                    dataframe=dataframe, 
                                    metric_column=metric_selector,
                                    **additional_selectors
                                    )

        if len(all_columns) > 1:
            chart = pn.Row(
                ECharts(interactive_chart, height=height, width=width),
                pn.Column(metric_selector,
                          *additional_selectors.values())
            )
        else:
            chart = ECharts(interactive_chart, height=height, width=width)

        return chart
    
    def create_choice_graph(
        self,
        graph_types: Iterable[VisualizationType],
        dataframe: pd.DataFrame,
        data_columns: Iterable[str],
        title: str = '',
        additional_filters = []
    ) -> pn.Column:

        options = { type.value : type  for type in graph_types }
        
        graph_selector = pn.widgets.Select(
            name='Select Chart Type',
            options=options,
            value=graph_types[0]
        )

        return pn.Column(
            pn.Row(
                pn.pane.Markdown(f"""
                                 <a id="{title}"></a> 
                                 #### {title}"""
                                ),
                                graph_selector
            ),
            pn.bind(self.create_graph, graph_type=graph_selector, 
                dataframe=dataframe, 
                all_columns=data_columns,
                width=GRAPH_WIDTH,
                height=GRAPH_HEIGHT,
                additional_filters=additional_filters
            ),
        )

        