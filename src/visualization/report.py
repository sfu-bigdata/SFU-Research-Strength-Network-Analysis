'''
Utility class to generate panel report
'''
from dataclasses import dataclass
import panel as pn
from .visualization_data import VisualizationData, GraphVisualization, VisualizationType
import pandas as pd
from config import NodeType
from .config import VisualizationDataPaths, SFU_TARGET_INSTITUTION_ID
import holoviews as hv
from . import css
from ast import literal_eval
from math import floor
from bokeh.models.widgets.tables import NumberFormatter

def float_floor(value: float, places: int):
    shift = 10*places
    return floor((shift*value))//shift

def custom_number_format(number):
    if number % 1 == 0:
        return f"{int(number):,}"
    elif (number*10)%1==0:
        return f"{number:,.1f}"
    else:
        return f"{number:,.2f}"
    
graph_viz = GraphVisualization()

class Report():

    def _introduction_section(self):
        '''
        Opening section that provides basic details.
        '''
        summary_node_information = pd.read_csv(VisualizationDataPaths.summary_node_information.value)

        labels : dict = literal_eval(summary_node_information['labels'].iloc[0])
        node_count : int = summary_node_information['nodeCount'].iloc[0]
        relationship_count: int = summary_node_information['relCount'].iloc[0] 
        
        return pn.Column(
            pn.pane.Markdown(
                f"""
                ## Introduction
                Using the [OpenAlex API](https://openalex.org), data was collected across all data
                pertaining to Simon Fraser University and the U15 Group of Canadian Research Universities.
                Extracted data includes information on the institutions themselves, works produced, and the authors of works related to the select institutions. 

                The extracted data has been transformed into **{node_count:,}** Neo4j database objects including:
                - **{labels[NodeType.SFU_U15_institution.value]:,}** institutions in the direct lineage of SFU and the U15.
                - **{(labels[NodeType.affiliated_institution.value]+labels[NodeType.SFU_U15_institution.value]):,}** institutions in total.
                - **{labels[NodeType.work.value]:,}** works produced by the target institutions.
                - **{labels[NodeType.author.value]:,}** authors that have authorships with the target institutions.
                - **{labels[NodeType.source.value]:,}** selected journals.

                Between these objects a total of **{relationship_count:,}** relationships have been defined between graph nodes.
                
                This report seeks to assess all objects related to Simon Fraser University and the U15 in order to determine institutional outputs, capabilities and relationships. 
                The following content will cover information relating to Simon Fraser University, how the university compares to other research institutions and networks between researchers.
                
                """
            )
        )
    
    def introduction_page(self):
        return pn.Column(
            self._introduction_section(),
    )

    def _institution_comparison_section(self):
        '''
        This section will provide a general overview data pertaining to OpenAlex works and authors, a general comparison between insitutions will be provided.
        '''

        def work_comparison():
            # For each SFU + U15, numbers for each role
            information = pd.read_csv(VisualizationDataPaths.summary_nodes_by_institution_works.value)
            
            U15_information = information[information['id'] != SFU_TARGET_INSTITUTION_ID]
            SFU_information = information[information['id'] == SFU_TARGET_INSTITUTION_ID]

            target_columns = [
                                'works_count',
                                'openalex_paper_count', 
                                'author_count', 
                                'total_author_count',
                                'authorship_count', 
                                'local_authorship_ratio',
                                'total_authorship_ratio'
                            ]
            ranks = information[['id']+(target_columns)]\
                                    .set_index('id', inplace=False).rank(method='first', ascending=False)

            return pn.Column(
                    graph_viz.create_choice_graph(
                        graph_types=[VisualizationType.BAR_CHART, VisualizationType.BUBBLE_CHART, VisualizationType.TREEMAP_CHART],
                        dataframe=information,
                        data_columns=target_columns,
                        title='OpenAlex Works'
                    ),
                    pn.widgets.Tabulator(
                        information.drop(columns=['id', 'lineage_root']),
                        theme='default',
                        pagination='remote',
                        page_size=25,       # Use a cleaner visual theme
                        layout='fit_data', # Make columns fit the available width
                        formatters={
                            # Format large numbers with commas
                            'works_count': NumberFormatter(format='0,0'),
                            'openalex_paper_count': NumberFormatter(format='0,0'),
                            'author_count': NumberFormatter(format='0,0'),
                            'total_author_count': NumberFormatter(format='0,0'),
                            'authorship_count': NumberFormatter(format='0,0'),
                            'local_authorship_ratio': NumberFormatter(format='0.00'), 
                            'total_authorship_ratio': NumberFormatter(format='0.00')
                        },
                        show_index=False
                    ),
                    pn.pane.Markdown(
                    f"""
                        **How does SFU compare?**

                        - **Total number of works:** SFU has a count of {SFU_information['works_count'].head(1).iloc[0]:,} works, as opposed to the U15 mean of {float_floor(U15_information['works_count'].mean(), 2):,} works per institution. 
                        The median U15 score is {float_floor(U15_information['works_count'].median(),2):,} works per institution. 
                        Overall SFU ranks {int(ranks.loc[SFU_TARGET_INSTITUTION_ID]['works_count'])} out of 16. 
                        
                        - **Total number of authors:** SFU has a count of {SFU_information['author_count'].head(1).iloc[0]:,} authors, as opposed to the U15 mean of {float_floor(U15_information['author_count'].mean(), 2):,} authors per institution. 
                        The median U15 score is {float_floor(U15_information['author_count'].median(),2):,} authors. 
                        Overall SFU ranks {int(ranks.loc[SFU_TARGET_INSTITUTION_ID]['author_count'])} out of 16. 
                        
                        - **Total number of authorships:** SFU has a count of {SFU_information['authorship_count'].head(1).iloc[0]:,} authorships, as opposed to the U15 mean of {float_floor(U15_information['authorship_count'].mean(), 2):,} authorships per institution. 
                        The median U15 score is {float_floor(U15_information['authorship_count'].median(),2):,} authorships. 
                        Overall SFU ranks {int(ranks.loc[SFU_TARGET_INSTITUTION_ID]['authorship_count'])} out of 16. 
                        
                        - **Average authorships per author:** SFU has a count of {SFU_information['local_authorship_ratio'].head(1).iloc[0]:,} authorships per affiliated author, as opposed to the U15 mean of {float_floor(U15_information['local_authorship_ratio'].mean(), 2):,} authorships per affiliated author. 
                        The median U15 score is {custom_number_format(float_floor(U15_information['local_authorship_ratio'].median(),2))} authorships per affilaited author. 
                        Overall SFU ranks {int(ranks.loc[SFU_TARGET_INSTITUTION_ID]['local_authorship_ratio'])} out of 16. 
                        
                        - **Total number of authors involved across affiliated works:** SFU has a count of {custom_number_format(SFU_information['total_author_count'].head(1).iloc[0])} authors that have been involved with a work an SFU author has been involved with, as opposed to the U15 mean of {float_floor(U15_information['total_author_count'].mean(), 2):,}. 
                        The median U15 score is {custom_number_format(float_floor(U15_information['total_author_count'].median(),2))} total authors. 
                        Overall SFU ranks {int(ranks.loc[SFU_TARGET_INSTITUTION_ID]['total_author_count'])} out of 16. 
                        
                        - **Average number of authorships per affiliated work:** SFU has a count of {SFU_information['total_authorship_ratio'].head(1).iloc[0]:,} authorships per affiliated work, as opposed to the U15 mean of {float_floor(U15_information['total_authorship_ratio'].mean(), 2):,}. 
                        The median U15 score is {float_floor(U15_information['total_authorship_ratio'].median(),2):,} authorships per affiliated work. 
                        Overall SFU ranks {int(ranks.loc[SFU_TARGET_INSTITUTION_ID]['total_authorship_ratio'])} out of 16. 
                        
                    """
                    )
            )
            

        def author_comparison():
            information = pd.read_csv(VisualizationDataPaths.summary_nodes_by_institution_authors.value)
            U15_information = information[information['id'] != SFU_TARGET_INSTITUTION_ID]
            SFU_information = information[information['id'] == SFU_TARGET_INSTITUTION_ID]

            target_columns = [
                'mean_works_count',
                'median_works_count',
                'mean_cited_by_count',
                'median_cited_by_count',
                'mean_h_index',
                'median_h_index',
                'mean_i10_index',
                'median_i10_index',
                'mean_2yr_mean_citedness',
                'mean_citations_per_work',
                'median_citations_per_work',
                'mean_i10_to_works_ratio',
                'median_i10_to_works_ratio',
                'i10_harmonic_mean',
                'mean_adjusted_i10_score'
            ]

            ranks = information[['id']+(target_columns)]\
                                    .set_index('id', inplace=False).rank(method='first', ascending=False)

        
            return pn.Column(
                graph_viz.create_choice_graph(
                    graph_types=[VisualizationType.BAR_CHART, VisualizationType.BUBBLE_CHART, VisualizationType.TREEMAP_CHART],
                    dataframe=information,
                    data_columns=target_columns,
                    title='OpenAlex Authors'
                ),
                pn.widgets.Tabulator(
                    information.drop(columns=['id']),
                    theme='default',
                    pagination='remote',
                    page_size=25,       # Use a cleaner visual theme
                    layout='fit_data', # Make columns fit the available width
                    formatters={
                        'mean_works_count': NumberFormatter(format='0.00'),
                        'median_works_count': NumberFormatter(format='0.00'),
                        'mean_cited_by_count': NumberFormatter(format='0.00'),
                        'median_cited_by_count': NumberFormatter(format='0.00'),
                        'mean_h_index': NumberFormatter(format='0.00'),
                        'median_h_index': NumberFormatter(format='0.00'),
                        'mean_i10_index': NumberFormatter(format='0.00'),
                        'median_i10_index': NumberFormatter(format='0.00'),
                        'mean_2yr_mean_citedness': NumberFormatter(format='0.00'),
                        'mean_citations_per_work': NumberFormatter(format='0.00'),
                        'median_citations_per_work': NumberFormatter(format='0.00'),
                        'mean_i10_to_works_ratio': NumberFormatter(format='0.00'),
                        'median_i10_to_works_ratio': NumberFormatter(format='0.00'),
                        'i10_harmonic_mean': NumberFormatter(format='0.00'),
                        'mean_adjusted_i10_score': NumberFormatter(format='0.00')
                    },
                    show_index=False
                ),   
                pn.pane.Markdown(
                f"""
                    **How does SFU compare?**

                    - **Works Count:** On average SFU authors complete a mean of {custom_number_format(SFU_information['mean_works_count'].head(1).iloc[0])} works and a median of {custom_number_format(SFU_information['median_works_count'].head(1).iloc[0])} works.
                    A U15 author completes a mean value of {custom_number_format(U15_information['mean_works_count'].head(1).iloc[0])} citations and a median value of {custom_number_format(U15_information['median_works_count'].head(1).iloc[0])} citations per author.
                    Relatively, SFU ranks {int(ranks.loc[SFU_TARGET_INSTITUTION_ID]['mean_works_count'])} out of 16 for works per author and {int(ranks.loc[SFU_TARGET_INSTITUTION_ID]['median_works_count'])} out of 16 for median works per author.

                    - **Cited By Count:** On average SFU authors receive a mean of {custom_number_format(SFU_information['mean_cited_by_count'].head(1).iloc[0])} citations and a median of {custom_number_format(SFU_information['median_cited_by_count'].head(1).iloc[0])} citations.
                    A U15 author recieves a mean value of {custom_number_format(U15_information['mean_cited_by_count'].head(1).iloc[0])} citations and a median value of {custom_number_format(U15_information['median_cited_by_count'].head(1).iloc[0])} citations per author.
                    Relatively, SFU ranks {int(ranks.loc[SFU_TARGET_INSTITUTION_ID]['mean_cited_by_count'])} out of 16 for mean citations per author and {int(ranks.loc[SFU_TARGET_INSTITUTION_ID]['median_cited_by_count'])} out of 16 for median citations per author.
                
                    - **H-Index:** The mean h-index of SFU authors is {custom_number_format(SFU_information['mean_h_index'].head(1).iloc[0])} and a median h-index of {custom_number_format(SFU_information['median_h_index'].head(1).iloc[0])}.
                    U15 authors have a mean h-index of {custom_number_format(U15_information['mean_h_index'].head(1).iloc[0])} a median h-index of {custom_number_format(U15_information['median_h_index'].head(1).iloc[0])}.
                    Relatively, SFU authors rank {int(ranks.loc[SFU_TARGET_INSTITUTION_ID]['mean_h_index'])} out of 16 for mean h-index and {int(ranks.loc[SFU_TARGET_INSTITUTION_ID]['median_h_index'])} out of 16 for median h-index.

                    - **I10-Index:** The mean I10-index of SFU authors is {custom_number_format(SFU_information['mean_i10_index'].head(1).iloc[0])} and a median I10-index of {custom_number_format(SFU_information['median_i10_index'].head(1).iloc[0])}.
                    U15 authors have a mean I10-index of {custom_number_format(U15_information['mean_i10_index'].head(1).iloc[0])} a median I10-index of {custom_number_format(U15_information['median_i10_index'].head(1).iloc[0])}.
                    Relatively, SFU authors rank {int(ranks.loc[SFU_TARGET_INSTITUTION_ID]['mean_i10_index'])} out of 16 for mean I10-index and {int(ranks.loc[SFU_TARGET_INSTITUTION_ID]['median_i10_index'])} out of 16 for median I10-index.

                    - **2 Year Mean Citedness:** The 2 year mean citedness of SFU authors is {custom_number_format(SFU_information['mean_2yr_mean_citedness'].head(1).iloc[0])}.
                    U15 authors have a 2 year mean citedness of {custom_number_format(U15_information['mean_2yr_mean_citedness'].head(1).iloc[0])}.
                    Relatively, SFU authors rank {int(ranks.loc[SFU_TARGET_INSTITUTION_ID]['mean_2yr_mean_citedness'])} out of 16 for 2 year mean citedness.

                     - **Citations Per Work:** The mean citations per work of SFU authors is {custom_number_format(SFU_information['mean_citations_per_work'].head(1).iloc[0])} and a median of {custom_number_format(SFU_information['median_citations_per_work'].head(1).iloc[0])} citations per work.
                    U15 authors have a mean of {custom_number_format(U15_information['mean_citations_per_work'].head(1).iloc[0])} and a median of {custom_number_format(U15_information['median_citations_per_work'].head(1).iloc[0])} citations per work.
                    Relatively, SFU authors rank {int(ranks.loc[SFU_TARGET_INSTITUTION_ID]['mean_citations_per_work'])} out of 16 for mean citations per work and {int(ranks.loc[SFU_TARGET_INSTITUTION_ID]['median_citations_per_work'])} out of 16 for median citations per work.
                
                    - **Further Investigation on the I10-Index Score:** 
                        
                        - **I10-Index to Works Ratio:** Adjusting for the number of works produced authors, 
                        SFU has a mean value of {custom_number_format(SFU_information['mean_i10_to_works_ratio'].head(1).iloc[0])} and a median value of {custom_number_format(SFU_information['median_i10_to_works_ratio'].head(1).iloc[0])}.
                        The U15 has a mean value of {custom_number_format(U15_information['mean_i10_to_works_ratio'].head(1).iloc[0])} and a median value of {custom_number_format(U15_information['median_i10_to_works_ratio'].head(1).iloc[0])}.
                        Relatively, SFU ranks {int(ranks.loc[SFU_TARGET_INSTITUTION_ID]['mean_i10_to_works_ratio'])} out of 16 for its mean value {int(ranks.loc[SFU_TARGET_INSTITUTION_ID]['median_i10_to_works_ratio'])} out of 16 for median value.
                
                        - **Harmonic mean:** SFU's harmonic mean is {custom_number_format(SFU_information['i10_harmonic_mean'].head(1).iloc[0])}.
                        U15 has a harmonic mean of {custom_number_format(U15_information['i10_harmonic_mean'].head(1).iloc[0])}.
                        Relatively, SFU ranks {int(ranks.loc[SFU_TARGET_INSTITUTION_ID]['i10_harmonic_mean'])} out of 16.

                        - **Mean Adjusted I10 Score:** This value is the normalized I10 score for authors across all institutions or the z-score. The range of values is not too large and this information is only tentatively added.
                            SFU's mean adjusted I10 Score is {custom_number_format(SFU_information['i10_harmonic_mean'].head(1).iloc[0])}.
                            U15 has a mean adjusted I10 Score of {custom_number_format(U15_information['i10_harmonic_mean'].head(1).iloc[0])}.
                            Relatively, SFU ranks {int(ranks.loc[SFU_TARGET_INSTITUTION_ID]['i10_harmonic_mean'])} out of 16.
                """
                )
            )
        
        def general_output_over_time():

            df = pd.read_csv(VisualizationDataPaths.summary_counts_by_year.value)

            target_columns = ['works_count', 'cited_by_count']

            graph = graph_viz.create_choice_graph(
                graph_types=[VisualizationType.THEME_RIVER_CHART, VisualizationType.BAR_RACE_CHART, VisualizationType.STACKED_AREA_CHART, VisualizationType.SMOOTHED_LINE_CHART],
                dataframe=df.copy(deep=True),
                data_columns=target_columns,
                title='Data Over Time'
            )

            min_year = df['year'].min()
            max_year = df['year'].max()

            sfu = df[df['id'] == SFU_TARGET_INSTITUTION_ID].set_index('year')

            u15 = df[df['id'] != SFU_TARGET_INSTITUTION_ID][['year']+target_columns]
            u15 = u15.groupby('year').mean()

            df = df.set_index(['id', 'display_name'])
            df = pd.concat([df['year'], df[['year']+target_columns].groupby('year').rank(method='first', ascending=False)], axis=1).reset_index()
            sfu_ranked = df[df['id'] == SFU_TARGET_INSTITUTION_ID].set_index('year')

            return pn.Column(
                graph,
                pn.widgets.Tabulator(
                    df.drop(columns=['id']),
                    theme='default',
                    pagination='remote',
                    page_size=25,       # Use a cleaner visual theme
                    layout='fit_data', # Make columns fit the available width
                    formatters={
                        'mean_works_count': NumberFormatter(format='0.00'),
                        'median_works_count': NumberFormatter(format='0.00'),
                        'mean_cited_by_count': NumberFormatter(format='0.00'),
                        'median_cited_by_count': NumberFormatter(format='0.00'),
                        'mean_h_index': NumberFormatter(format='0.00'),
                        'median_h_index': NumberFormatter(format='0.00'),
                        'mean_i10_index': NumberFormatter(format='0.00'),
                        'median_i10_index': NumberFormatter(format='0.00'),
                        'mean_2yr_mean_citedness': NumberFormatter(format='0.00'),
                        'mean_citations_per_work': NumberFormatter(format='0.00'),
                        'median_citations_per_work': NumberFormatter(format='0.00'),
                        'mean_i10_to_works_ratio': NumberFormatter(format='0.00'),
                        'median_i10_to_works_ratio': NumberFormatter(format='0.00'),
                        'i10_harmonic_mean': NumberFormatter(format='0.00'),
                        'mean_adjusted_i10_score': NumberFormatter(format='0.00')
                    },
                    show_index=False
                ),
                pn.pane.Markdown(
                    f"""
                        **How does SFU compare?**

                        - **Works Count:** 

                            - **Relative Rank:** SFU begins with a works count rank of {int(sfu_ranked.loc[min_year]['works_count']):,} in {min_year} and ended with a rank of {int(sfu_ranked.loc[max_year]['works_count']):,} in {max_year}.
                        
                            - **Relative Growth** SFU started with a value of {sfu.loc[min_year]['works_count']} in {min_year} and ended with a value of {sfu.loc[max_year]['works_count']} in {max_year},
                            this represents a relative change of {custom_number_format((sfu.loc[max_year]['works_count']-sfu.loc[min_year]['works_count'])/sfu.loc[min_year]['works_count']*100)}%. Comparatively, the U15 average is {custom_number_format((u15.loc[max_year]['works_count']-u15.loc[min_year]['works_count'])/u15.loc[min_year]['works_count']*100)}%.
                    
                        - **Cited By Count:** 

                            - **Relative Rank:** SFU begins with a cited by count rank of {int(sfu_ranked.loc[min_year]['cited_by_count']):,} in {min_year} and ended with a rank of {int(sfu_ranked.loc[max_year]['cited_by_count']):,} in {max_year}.
                        
                            - **Relative Growth** SFU started with a value of {sfu.loc[min_year]['cited_by_count']} in {min_year} and ended with a value of {sfu.loc[max_year]['cited_by_count']} in {max_year},
                            this represents a relative change of {custom_number_format((sfu.loc[max_year]['cited_by_count']-sfu.loc[min_year]['cited_by_count'])/sfu.loc[min_year]['cited_by_count']*100)}%. Comparatively, the U15 average is {custom_number_format((u15.loc[max_year]['cited_by_count']-u15.loc[min_year]['cited_by_count'])/u15.loc[min_year]['cited_by_count']*100)}%.
                    """
                )
            )

        def conclusion():
            return pn.pane.Markdown(
                f"""
                **Summary:** When looking at a general overview of the data, there are some significant differences in terms of output by institution.
                Simon Fraser University has relatively low works output and a relatively low number of affiliated authors compared to U15 institutions.
                However, SFU does relatively well when regarding the quality of the works relating to the institution, with above average values for author productivity, citation scores, I10-index, and the number of citations per author.  
                """
            )
        
        return pn.Column(
            pn.pane.Markdown(
                f"""
                    ## A General Comparison

                    The following section will contain a visualization of general OpenAlex data relating to the target institutions. pertaining to OpenAlex works and authors, a general comparison between insitutions will be found here.

                """
            ),
            work_comparison(),
            pn.HSpacer(),
            author_comparison(),
            pn.HSpacer(),
            general_output_over_time(),
            pn.HSpacer(),
            conclusion()
        )

    def general_comparison_page(self):
        return pn.Column(
            self._institution_comparison_section()
    )

    def _analysis_section(self):
        '''
        This section will provide a more in depth look at the Open Alex Data.
        '''

        def works_analysis():
            '''
            Detail the works by institution
            
            Included Data:

                First within the institutions:
                    Categorization:
                        OA Status
                        Type

                    Numerical Catagorization:
                        Countries Distinct Count
                        Institutions Distinct Count
                        FWCI

                    Over time:
                        Publication year (bar)
                
                Then for comparison:
                    OA Status -> percentage (bar)
                    Type -> percentage (bar)
                    FWCI -> bar (mean)
                    Countries -> bar
                    Institutions -> bar
            '''

            df = pd.read_csv(VisualizationDataPaths.work_analysis.value)


            '''
            Categorical Dataframe
            '''

            categorical_columns = ['type', 'is_open_access', 'open_access_status']
            for category in categorical_columns:
                df[category] = df[category].apply(literal_eval).fillna(0)

            categorical = pn.Column(
                graph_viz.create_choice_graph(
                    graph_types=[VisualizationType.PIE_CHART, VisualizationType.TREEMAP_CHART],
                    dataframe=df[['id', 'display_name']+categorical_columns],
                    data_columns=categorical_columns,
                    additional_filters=['id'],
                    title="Categorical Breakdown"
                )
            )

            '''
            Numerical Categorization
            '''
            numerical_columns = ['avg_distinct_countries', 'avg_distinct_institutions', 'avg_citation_normalized_percentile', 'avg_fwci', 'avg_apc_paid']

            numerical = pn.Column(
                graph_viz.create_choice_graph(
                    graph_types=[VisualizationType.BAR_CHART, VisualizationType.SMOOTHED_LINE_CHART, VisualizationType.BUBBLE_CHART],
                    dataframe=df[['id', 'display_name']+numerical_columns],
                    data_columns=numerical_columns,
                    title="Work Features"
                )
            )
            '''
            Over Time
            '''
            temporal_columns = ['total_by_publication_year']
            local_df = df.copy(deep=True)
            local_df = local_df[['id', 'display_name', 'total_by_publication_year']]
            publication_data = local_df['total_by_publication_year'].apply(literal_eval)
            local_df.loc[:,'year'] = [list(d.keys()) for d in publication_data]
            local_df.loc[:,'total'] = [list(d.values()) for d in publication_data]
            local_df = local_df.drop(columns=temporal_columns)

            publication_data = local_df.explode(['year', 'total'])
            publication_data['year'] = publication_data['year'].apply(literal_eval)

            # Filter out all years before 2000
            publication_data = publication_data[(publication_data['year'] >=2000)&(publication_data['year']<2025)]
            '''
            Modify the dataframe for usage
            '''
            
            temporal = graph_viz.create_choice_graph(
                graph_types=[VisualizationType.THEME_RIVER_CHART, VisualizationType.BAR_RACE_CHART, VisualizationType.STACKED_AREA_CHART, VisualizationType.SMOOTHED_LINE_CHART],
                dataframe=publication_data,
                data_columns=['total'],
                title="Change Over Time"
            )
            
            def works_composition_categorical_markdown():
                type_df =  pd.json_normalize(df['type']).fillna(0).astype(int)\
                    .join(df[['id', 'display_name']])

                type_df = type_df\
                    .melt(
                            id_vars=['id', 'display_name'],
                            var_name='type',
                            value_name='count'
                    )

                type_df_grouped = type_df.groupby(['id', 'display_name'])['count'].transform('sum')

                type_df['proportion'] = type_df['count']/type_df_grouped

                sfu_type = type_df[type_df['id']==SFU_TARGET_INSTITUTION_ID].set_index('type')
                u15_type = type_df[type_df['id']!=SFU_TARGET_INSTITUTION_ID]

                u15_type_counts = u15_type.groupby('type')['count'].sum()
                baseline_proportions = pd.DataFrame((u15_type_counts/u15_type_counts.sum())).rename(columns={
                    'count': 'baseline_proportion'
                })
                sfu_type = sfu_type.join(baseline_proportions)
                sfu_type['specialization_ratio'] = sfu_type['proportion']/sfu_type['baseline_proportion']

                top_3_types = sfu_type.nlargest(3, 'count')
                top_3_most_likely = sfu_type.nlargest(3, 'specialization_ratio')
                has_none = sfu_type[sfu_type['count']==0].index.to_list()
                for i in range(len(has_none)):
                    if has_none[i][-1] != 's':
                        has_none[i] = has_none[i]+'s'

                top_3_values = []
                for index, type in top_3_types.iterrows():
                    article = str(index)
                    if article[-1] != 's':
                            article+='s'
                    
                    substr = article.capitalize() + ' with a count of ' + f"{type['count']:,}" + ' items.'
                    top_3_values.append(substr)
                
                
                top_3_most_likely_str = []
                for index, type in top_3_most_likely.iterrows():
                        article = str(index)
                        if article[-1] != 's':
                                article+='s'

                        substr = article.capitalize() + ' with a specialization ratio of ' + f"{type['specialization_ratio']:.2f}"+'.'
                        top_3_most_likely_str.append(substr)

                open_access = df['is_open_access']
                open_access = pd.json_normalize(open_access).join(df['id'])
                
                open_access['total'] = open_access['true'] + open_access['false']
                open_access['true_rate'] = open_access['true']/open_access['total']

                sfu_open_access = open_access[open_access['id'] == SFU_TARGET_INSTITUTION_ID]
                u15_open_access = open_access[open_access['id'] != SFU_TARGET_INSTITUTION_ID]

                baseline_rate = u15_open_access['true'].sum() / u15_open_access['total'].sum()
                difference = (sfu_open_access['true_rate']-baseline_rate).head(1).iloc[0]

                open_access_status_df = pd.json_normalize(df['open_access_status']).join(df['id'])
                sfu_open_access_status = open_access_status_df[open_access_status_df['id']==SFU_TARGET_INSTITUTION_ID]

                return pn.Column(
                    pn.pane.Markdown("**Works Composition:**"),
                    pn.widgets.Tabulator(
                        sfu_type.drop(columns=['id', 'display_name', 'baseline_proportion', 'proportion']),
                        theme='default',
                        pagination='remote',
                        page_size=25,       # Use a cleaner visual theme
                        layout='fit_data', # Make columns fit the available width
                        show_index=True
                    ),
                    pn.pane.Markdown(f"""
                        **Open Access:**
                        Simon Fraser University has a total of {int(sfu_open_access['true'].iloc[0]):,} open access works out of a total of {int(sfu_open_access['total'].iloc[0]):,} works identified. This results in a ratio of about {sfu_open_access['true_rate'].head(1).iloc[0]:.2f}.

                        Compared to the U15, Simon Fraser University is {abs(difference)*100:.2f} percentage points lower than the U15 average.  
                    """),
                    pn.widgets.Tabulator(
                        open_access.join(df['display_name']).drop(columns=['id']),
                        theme='default',
                        pagination='remote',
                        page_size=25,       # Use a cleaner visual theme
                        layout='fit_data', # Make columns fit the available width
                        show_index=False
                    ),
                    pn.pane.Markdown(f"""
                        **Open Access Status:**
                        Of the open access works that pertain to SFU the breakdown is as follows:
                        
                        - {int(sfu_open_access_status['green'].iloc[0]):,} green status open access works.

                        - {int(sfu_open_access_status['bronze'].iloc[0]):,} bronze status open access works.

                        - {int(sfu_open_access_status['gold'].iloc[0]):,} gold status open access works.

                        - {int(sfu_open_access_status['diamond'].iloc[0]):,} diamond status open access works.

                        - {int(sfu_open_access_status['hybrid'].iloc[0]):,} hybrid status open access works.
                    """),
                    pn.widgets.Tabulator(
                        open_access_status_df.join(df['display_name']).drop(columns=['id']),
                        theme='default',
                        pagination='remote',
                        page_size=25,       # Use a cleaner visual theme
                        layout='fit_data', # Make columns fit the available width
                        show_index=False
                    ),
                )

            def works_composition_numerical_markdown():
                local_df = df.copy()
                
                sfu = local_df[local_df['id'] == SFU_TARGET_INSTITUTION_ID]
                u15 = local_df[local_df['id'] != SFU_TARGET_INSTITUTION_ID]


                return pn.pane.Markdown(
                    f"""
                    **Notable Takeaways:**

                    - **Distinct Countries:** The average number of countries involved in an SFU work is {sfu['avg_distinct_countries'].iloc[0]:.2f}, as compared to the U15 average of {u15['avg_distinct_countries'].mean():.2f}. This represents a relative difference of {(sfu['avg_distinct_countries'].iloc[0]-u15['avg_distinct_countries'].mean())/abs(u15['avg_distinct_countries'].mean())*100:.2f}%.
                    
                    - **Distinct Institutions:** The average number of institutions involved in an SFU work is {sfu['avg_distinct_institutions'].iloc[0]:.2f}, as compared to the U15 average of {u15['avg_distinct_institutions'].mean():.2f}. This represents a relative difference of {(sfu['avg_distinct_institutions'].iloc[0]-u15['avg_distinct_institutions'].mean())/abs(u15['avg_distinct_institutions'].mean())*100:.2f}%. 

                    - **Field-Weighted Citation Impact:** The FWCI of an SFU work is {sfu['avg_fwci'].iloc[0]:.2f}, as compared to the U15 average of {u15['avg_fwci'].mean():.2f}. This represents a relative difference of {(sfu['avg_fwci'].iloc[0]-u15['avg_fwci'].mean())/abs(u15['avg_fwci'].mean())*100:.2f}%. 

                    - **Citation Normalized Percentile:** The average citation normalized percentile of an SFU work is {sfu['avg_citation_normalized_percentile'].iloc[0]:.2f}, as compared to the U15 average of {u15['avg_citation_normalized_percentile'].mean():.2f}. This represents a relative difference of {(sfu['avg_citation_normalized_percentile'].iloc[0]-u15['avg_citation_normalized_percentile'].mean())/abs(u15['avg_citation_normalized_percentile'].mean())*100:.2f}%. 

                    - **Article Processing Charge:** The average article processing charge involved in an SFU work is {sfu['avg_apc_paid'].iloc[0]:.2f}, as compared to the U15 average of {u15['avg_apc_paid'].mean():.2f}. This represents a relative difference of {(sfu['avg_apc_paid'].iloc[0]-u15['avg_apc_paid'].mean())/abs(u15['avg_apc_paid'].mean())*100:.2f}%. 
                    """
                )

            def works_composition_temporal_markdown():
                local_df = df.copy(deep=True)
                local_df = local_df[['id', 'display_name', 'total_by_publication_year']]
                publication_data = local_df['total_by_publication_year'].apply(literal_eval)
                local_df.loc[:,'year'] = [list(d.keys()) for d in publication_data]
                local_df.loc[:,'total'] = [list(d.values()) for d in publication_data]
                local_df = local_df.drop(columns=temporal_columns)

                publication_data = local_df.explode(['year', 'total'])
                publication_data['year'] = publication_data['year'].apply(literal_eval)

                # Filter out all years before 2000
                publication_data = publication_data[(publication_data['year'] >=2000)&(publication_data['year']<2025)]

                sfu = publication_data[publication_data['id']==SFU_TARGET_INSTITUTION_ID]
                u15 = publication_data[publication_data['id']!=SFU_TARGET_INSTITUTION_ID]

                beginning_year = sfu['year'].min()
                end_year = sfu['year'].max()

                sfu_start_val = sfu[sfu['year']==beginning_year]['total'].iloc[0]
                sfu_end_val = sfu[sfu['year']==end_year]['total'].iloc[0]

                u15_start_val = u15[u15['year']==beginning_year]['total'].mean()
                u15_end_val = u15[u15['year']==end_year]['total'].mean()

                relative_change_sfu = (sfu_end_val-sfu_start_val)/sfu_start_val*100
                relative_change_u15 = (u15_end_val-u15_start_val)/u15_start_val*100
                relative_difference = (relative_change_sfu-relative_change_u15)/relative_change_u15*100

                return pn.pane.Markdown(
                    f"""
                    **Notable Takeaways:**

                    - In the year {beginning_year}, SFU had a total of {sfu_start_val} works noted by OpenAlex and U15 institutions had an average of {u15_start_val} works.

                    - By the year {end_year}, SFU had {sfu_end_val} affiliated works for that year and U15 institutions had an average of {u15_end_val} affiliated works.

                    - This represents a relative change of {relative_change_sfu:.2f}% for SFU and a relative change of {relative_change_u15:.2f}% for the U15 average. The rate of change relative to the U15 is {relative_difference:.2f}%.
                    """
                )
            
            return pn.Column(
                pn.pane.Markdown("""
                    #### Works Analysis
                     
                    The following contents are a further inspection of OpenAlex works. 
                """),
                categorical,
                works_composition_categorical_markdown(),
                numerical,
                works_composition_numerical_markdown(),
                temporal,
                works_composition_temporal_markdown()
            )

        def authors_analysis():
            '''
            Authors counts by year -> cited by count, works count
            affiliated per year
            '''
            
            df = pd.read_csv(VisualizationDataPaths.author_analysis.value)

            # Split the dataframe in two due to large discrepancy between year values

            # author_counts_by_year is nested list
            authors_by_year = df.copy(deep=True)
            authors_by_year['author_counts_by_year'] = authors_by_year['author_counts_by_year'].apply(literal_eval)

            authors_by_year = authors_by_year[['id','display_name', 'author_counts_by_year']].to_dict('records')

            authors_by_year = pd.json_normalize(
                authors_by_year,
                record_path='author_counts_by_year',
                meta=['id', 'display_name']
            )

            authors_by_year = authors_by_year[authors_by_year['year'] < 2025]

            # Split them because of the timeframe distance
            affiliated_per_year = df.copy(deep=True)
            affiliated_per_year = affiliated_per_year[['id', 'display_name', 'affiliated_per_year']]
            affiliated_data = affiliated_per_year['affiliated_per_year'].apply(literal_eval)

            affiliated_per_year.loc[:, 'year'] = [list(d.keys()) for d in affiliated_data]
            affiliated_per_year.loc[:, 'affiliations'] = [list(d.values()) for d in affiliated_data]

            affiliated_per_year = affiliated_per_year.drop(columns=['affiliated_per_year'])
            affiliated_per_year = affiliated_per_year.explode(['year', 'affiliations'])

            # Filter out affiliations before 2000
            affiliated_per_year['year'] = affiliated_per_year['year'].apply(literal_eval)
            affiliated_per_year = affiliated_per_year[(affiliated_per_year['year'] >= 2000)&(affiliated_per_year['year'] < 2025)]

            authors = graph_viz.create_choice_graph(
                graph_types=[VisualizationType.THEME_RIVER_CHART, VisualizationType.BAR_RACE_CHART, VisualizationType.STACKED_AREA_CHART, VisualizationType.SMOOTHED_LINE_CHART],
                dataframe=authors_by_year,
                data_columns=['cited_by_count', 'works_count'],
                title='SFU Author Information'
            )

            affiliated = graph_viz.create_choice_graph(
                graph_types=[VisualizationType.THEME_RIVER_CHART, VisualizationType.BAR_RACE_CHART, VisualizationType.STACKED_AREA_CHART, VisualizationType.SMOOTHED_LINE_CHART],
                dataframe=affiliated_per_year,
                data_columns=['affiliations'],
                title='Author Affiliations'
            )

            def authors_by_year_markdown():
                local_df = authors_by_year

                sfu = local_df[local_df['id']==SFU_TARGET_INSTITUTION_ID]
                u15 = local_df[local_df['id']!=SFU_TARGET_INSTITUTION_ID]

                beginning_year = sfu['year'].min()
                end_year = sfu['year'].max()

                sfu_start_val_counts = sfu[sfu['year']==beginning_year]['cited_by_count'].iloc[0]
                sfu_end_val_counts = sfu[sfu['year']==end_year]['cited_by_count'].iloc[0]

                u15_start_val_counts = u15[u15['year']==beginning_year]['cited_by_count'].mean()
                u15_end_val_counts = u15[u15['year']==end_year]['cited_by_count'].mean()

                relative_change_sfu_counts = (sfu_end_val_counts-sfu_start_val_counts)/sfu_start_val_counts*100
                relative_change_u15_counts = (u15_end_val_counts-u15_start_val_counts)/u15_start_val_counts*100
                relative_difference_counts = (relative_change_sfu_counts-relative_change_u15_counts)/relative_change_u15_counts*100

                sfu_start_val_works = sfu[sfu['year']==beginning_year]['works_count'].iloc[0]
                sfu_end_val_works = sfu[sfu['year']==end_year]['works_count'].iloc[0]

                u15_start_val_works = u15[u15['year']==beginning_year]['works_count'].mean()
                u15_end_val_works = u15[u15['year']==end_year]['works_count'].mean()

                relative_change_sfu_works = (sfu_end_val_works-sfu_start_val_works)/sfu_start_val_works*100
                relative_change_u15_works = (u15_end_val_works-u15_start_val_works)/u15_start_val_works*100
                relative_difference_works = (relative_change_sfu_works-relative_change_u15_works)/relative_change_u15_works*100
                
                return pn.pane.Markdown(
                    f"""
                    **Notable Takeaways:**

                    **Cited By Count:**
                    - In the year {beginning_year}, SFU authors had a total of {sfu_start_val_counts:,} citations and U15 institutions had an average of {u15_start_val_counts:,.2f} citations.

                    - By the year {end_year}, SFU authors had {sfu_end_val_counts:,} citations and U15 institutions had an average of {u15_end_val_counts:,.2f} citations.

                    - This represents a relative change of {relative_change_sfu_counts:.2f}% for SFU and a relative change of {relative_change_u15_counts:.2f}% for the U15 average. The rate of change relative to the U15 is {relative_difference_counts:.2f}%.
                    
                    **Works Count:**
                    - In the year {beginning_year}, SFU authors had a total of {sfu_start_val_works:,} works noted by OpenAlex and U15 institutions had an average of {u15_start_val_works:,.2f} works.

                    - By the year {end_year}, SFU authors had {sfu_end_val_works:,} affiliated works for that year and U15 institutions had an average of {u15_end_val_works:,.2f} affiliated works.

                    - This represents a relative change of {relative_change_sfu_works:.2f}% for SFU and a relative change of {relative_change_u15_works:.2f}% for the U15 average. The rate of change relative to the U15 is {relative_difference_works:.2f}%.
                    """
                )
            
            def affiliated_by_year_markdown():
                sfu = affiliated_per_year[affiliated_per_year['id']==SFU_TARGET_INSTITUTION_ID]
                u15 = affiliated_per_year[affiliated_per_year['id']!=SFU_TARGET_INSTITUTION_ID]

                beginning_year = sfu['year'].min()
                end_year = sfu['year'].max()

                sfu_start_val = sfu[sfu['year']==beginning_year]['affiliations'].iloc[0]
                sfu_end_val = sfu[sfu['year']==end_year]['affiliations'].iloc[0]

                u15_start_val = u15[u15['year']==beginning_year]['affiliations'].mean()
                u15_end_val = u15[u15['year']==end_year]['affiliations'].mean()

                relative_change_sfu = (sfu_end_val-sfu_start_val)/sfu_start_val*100
                relative_change_u15 = (u15_end_val-u15_start_val)/u15_start_val*100
                relative_difference = (relative_change_sfu-relative_change_u15)/relative_change_u15*100

                return pn.pane.Markdown(
                    f"""
                    **Notable Takeaways:**

                    - In the year {beginning_year}, SFU had a total of {sfu_start_val:,} affiliated authors noted by OpenAlex and U15 institutions had an average of {u15_start_val:,.2f} affiliated authors.

                    - By the year {end_year}, SFU had {sfu_end_val:,} affiliated authors for that year and U15 institutions had an average of {u15_end_val:,.2f} affiliated authors.

                    - This represents a relative change of {relative_change_sfu:.2f}% for SFU and a relative change of {relative_change_u15:.2f}% for the U15 average. The rate of change relative to the U15 is {relative_difference:.2f}%.
                    """
                )

            return pn.Column(
                authors,
                authors_by_year_markdown(),
                pn.HSpacer(),
                affiliated,
                affiliated_by_year_markdown()
            )
        

        return pn.Column(
            pn.pane.Markdown(
                f"""
                ## A Closer Look

                The following section will provide a closer inspection of the data, and its internal breakdowns. Works, authors, and institutions will be looked at more closely in order to investigate their internal structures and how they differ between institutions.
                """
            ),
            pn.HSpacer(),
            works_analysis(),
            pn.HSpacer(),
            authors_analysis(),
        )

    def topics_section(self):
            '''
            Section containing to topics
            Overview + filtering based on topics
            '''

            def process_topics_dataframes():
                '''
                Will break down the topics dataframe and return corresponding dataframes for each level in the hierarchy
                '''
                df = pd.read_csv(VisualizationDataPaths.topics_works.value)


                metric_cols = ['sum_distinct_countries', 'sum_distinct_institutions', 'sum_fwci', 'sum_citation_normalized_percentile', 'sum_apc_paid']
                topics = df.copy(deep=True)[['id', 'institution_display_name', 
                                            'subfield_id', 'subfield_display_name',
                                            'topic_id', 'topic_display_name',
                                            'total_works',
                                            'sum_distinct_countries',
                                            'sum_distinct_institutions',
                                            'sum_fwci',
                                            'sum_citation_normalized_percentile',
                                            'sum_apc_paid'
                                            ]]

                averaged_cols = ['avg_distinct_countries', 'avg_distinct_institutions', 'avg_fwci', 'avg_citation_normalized_percentile', 'avg_apc_paid']
                for i in range(len(metric_cols)):
                    topics[averaged_cols[i]] = topics[metric_cols[i]]/topics['total_works']
                    topics = topics.drop(columns=[metric_cols[i]])
                topics = topics.reset_index()

                subfields = df.copy(deep=True)[['id', 'institution_display_name',
                                                'field_id', 'field_display_name',
                                            'subfield_id', 'subfield_display_name',
                                            'total_works',
                                            'sum_distinct_countries',
                                            'sum_distinct_institutions',
                                            'sum_fwci',
                                            'sum_citation_normalized_percentile',
                                            'sum_apc_paid'
                                            ]]

                subfields = subfields.groupby(['id', 'institution_display_name', 'field_id', 'field_display_name', 'subfield_id', 'subfield_display_name']).agg('sum')
                for i in range(len(metric_cols)):
                    subfields[averaged_cols[i]] = subfields[metric_cols[i]]/subfields['total_works']
                    subfields = subfields.drop(columns=[metric_cols[i]])
                subfields = subfields.reset_index()

                fields = df.copy(deep=True)[['id', 'institution_display_name',
                                            'domain_id', 'domain_display_name',
                                            'field_id', 'field_display_name',
                                            'total_works',
                                            'sum_distinct_countries',
                                            'sum_distinct_institutions',
                                            'sum_fwci',
                                            'sum_citation_normalized_percentile',
                                            'sum_apc_paid'
                                            ]]

                fields_group = fields.groupby(['id', 'institution_display_name', 'domain_id', 'domain_display_name', 'field_id', 'field_display_name'])
                fields = fields_group.agg('sum')
                for i in range(len(metric_cols)):
                    fields[averaged_cols[i]] = fields[metric_cols[i]]/fields['total_works']
                    fields = fields.drop(columns=[metric_cols[i]])
                fields = fields.reset_index()

                domains = df.copy(deep=True)[['id', 'institution_display_name', 
                                            'domain_id', 'domain_display_name',
                                            'total_works',
                                            'sum_distinct_countries',
                                            'sum_distinct_institutions',
                                            'sum_fwci',
                                            'sum_citation_normalized_percentile',
                                            'sum_apc_paid'
                                            ]]

                domains = domains.groupby(['id', 'institution_display_name', 'domain_id', 'domain_display_name']).agg('sum')
                for i in range(len(metric_cols)):
                    domains[averaged_cols[i]] = domains[metric_cols[i]]/domains['total_works']
                    domains = domains.drop(columns=[metric_cols[i]])
                domains = domains.reset_index()
                
                return {
                    'domain': domains, 
                    'field': fields, 
                    'subfield':subfields, 
                    'topic': topics
                }
            
            def topic_overview():
                '''
                General overview of Topics by institution
                '''

                topic_app = graph_viz.topic_hierarchy(
                    graph_types=[VisualizationType.BAR_CHART, VisualizationType.BUBBLE_CHART, VisualizationType.PIE_CHART],
                    dataframes=process_topics_dataframes(),
                    filters=[
                        ('domain_display_name', 'domain_id'),
                        ('field_display_name', 'field_id'),
                        ('subfield_display_name', 'subfield_id'),
                        ('topic_display_name', 'topic_id'),
                    ]
                )

                return topic_app.layout()
            
            def dict_to_html_list(data_dict: dict, numbered: bool = True, header: str = None, suffix: str ='') -> str:
                if not data_dict:
                    return ""

                # Sort the dictionary items by value (from high to low) if requested
                items = data_dict.items()

                # --- REVISED SECTION ---
                # Use a standard for loop for clarity and to build the list items
                html_lines = []
                for key, value in items:
                    # Check if the value is a number that can be formatted
                    if isinstance(value, (int, float)):
                        # If it's a float, format with 2 decimals and commas
                        if isinstance(value, float):
                            formatted_value = f"{value:,.2f}"
                        # Otherwise (it's an int), just format with commas
                        else:
                            formatted_value = f"{value:,}"
                    else:
                        # If it's not a number (e.g., a string), just display it as is
                        formatted_value = str(value)
                        
                    html_lines.append(f"<li>{key}: {formatted_value} {suffix}</li>")

                # Join the formatted lines into a single string
                list_items = "".join(html_lines)
                # --- END REVISED SECTION ---

                # Wrap the generated list items in the appropriate HTML tag
                if numbered:
                    return f"<ol>{list_items}</ol>"
                else:
                    return f"<ul>{list_items}</ul>"
                
            def topics_composition_markdown():
                dataframes = process_topics_dataframes()

                domain = dataframes['domain']
                field = dataframes['field']
                subfield = dataframes['subfield']
                topic = dataframes['topic']

                # Only look at SFU for all of them
                domain = domain[domain['id']==SFU_TARGET_INSTITUTION_ID]
                field = field[field['id']==SFU_TARGET_INSTITUTION_ID]
                subfield = subfield[subfield['id']==SFU_TARGET_INSTITUTION_ID]
                topic = topic[topic['id']==SFU_TARGET_INSTITUTION_ID]
                
                return pn.Column(
                    pn.pane.Markdown("### Total Works"),
                    pn.pane.Markdown("### Domains"),
                    pn.widgets.Tabulator(
                        domain.drop(columns=['id', 'domain_id']),
                        theme='default',
                        pagination='remote',
                        page_size=25,       # Use a cleaner visual theme
                        layout='fit_data', # Make columns fit the available width
                        formatters={
                            # Format large numbers with commas
                            'number_of_collaborations': NumberFormatter(format='0,0'),
                            # Format floats to 2 decimal places
                            'avg_fwci': NumberFormatter(format='0.00'),
                            'avg_citation_normalized_percentile': NumberFormatter(format='0.00'), 
                            'avg_distinct_countries': NumberFormatter(format='0.00'), 
                            'avg_distinct_institutions': NumberFormatter(format='0.00'), 
                            'avg_apc_paid': NumberFormatter(format='0.00')
                        },
                    show_index=False
                    ),
                    pn.pane.Markdown("### Fields"),
                    pn.widgets.Tabulator(
                        field.drop(columns=['id', 'domain_id', 'field_id']),
                        theme='default',
                        pagination='remote',
                        page_size=25,       # Use a cleaner visual theme
                        layout='fit_data', # Make columns fit the available width
                        formatters={
                            # Format large numbers with commas
                            'number_of_collaborations': NumberFormatter(format='0,0'),
                            # Format floats to 2 decimal places
                            'avg_fwci': NumberFormatter(format='0.00'),
                            'avg_citation_normalized_percentile': NumberFormatter(format='0.00'), 
                            'avg_distinct_countries': NumberFormatter(format='0.00'), 
                            'avg_distinct_institutions': NumberFormatter(format='0.00'), 
                            'avg_apc_paid': NumberFormatter(format='0.00')
                        },
                    show_index=False
                    ),
                    pn.pane.Markdown("### Subfields"),
                    pn.widgets.Tabulator(
                        subfield.drop(columns=['id', 'field_id', 'subfield_id']),
                        theme='default',
                        pagination='remote',
                        page_size=25,       # Use a cleaner visual theme
                        layout='fit_data', # Make columns fit the available width
                        formatters={
                            # Format large numbers with commas
                            'number_of_collaborations': NumberFormatter(format='0,0'),
                            # Format floats to 2 decimal places
                            'avg_fwci': NumberFormatter(format='0.00'),
                            'avg_citation_normalized_percentile': NumberFormatter(format='0.00'), 
                            'avg_distinct_countries': NumberFormatter(format='0.00'), 
                            'avg_distinct_institutions': NumberFormatter(format='0.00'), 
                            'avg_apc_paid': NumberFormatter(format='0.00')
                        },
                    show_index=False
                    ),
                    pn.pane.Markdown("### Topics"),
                    pn.widgets.Tabulator(
                        topic.drop(columns=['id', 'topic_id', 'subfield_id']),
                        theme='default',
                        pagination='remote',
                        page_size=25,       # Use a cleaner visual theme
                        layout='fit_data', # Make columns fit the available width
                        formatters={
                            # Format large numbers with commas
                            'number_of_collaborations': NumberFormatter(format='0,0'),
                            # Format floats to 2 decimal places
                            'avg_fwci': NumberFormatter(format='0.00'),
                            'avg_citation_normalized_percentile': NumberFormatter(format='0.00'), 
                            'avg_distinct_countries': NumberFormatter(format='0.00'), 
                            'avg_distinct_institutions': NumberFormatter(format='0.00'), 
                            'avg_apc_paid': NumberFormatter(format='0.00')
                        },
                    show_index=False
                    ),
                )
            
            return pn.Column(
                pn.pane.Markdown(f"""
                    #### Topics Analysis

                    The following contents visualizations and information pertaining works produced and the topics they concern.
                """),
                topic_overview(),
                topics_composition_markdown()
            )
        
    def topics_page(self):
        return pn.Column(
            self.topics_section()
        )

    def analysis_page(self):
        return pn.Column(
            self._analysis_section()
        )
    
    def journals_page(self):
        return pn.Column(
        )
    
    def research_page(self):
        return pn.Column(

        )
    
    def network_analysis_page(self):
        '''
        Detail the collaborations between SFU and other institutions
        '''
        path = VisualizationDataPaths.geographic_topics_collaborations.value
        df = pd.read_csv(path)

        df = df[['country_id', 'country_name',
                'number_of_collaborations',
                'avg_citation_normalized_percentile', 
                'avg_fwci', 
                'avg_distinct_countries', 
                'avg_distinct_institutions', 
                'avg_apc_paid'
                ]].groupby(['country_id', 'country_name'])\
                .aggregate({
                    'number_of_collaborations': 'sum',
                    'avg_citation_normalized_percentile': 'mean', 
                    'avg_fwci': 'mean', 
                    'avg_distinct_countries': 'mean', 
                    'avg_distinct_institutions': 'mean', 
                    'avg_apc_paid': 'mean'
                }).reset_index()\
                .rename(columns={
                    'country_id': 'id',
                    'country_name': 'display_name'
                }).fillna(0)

        # Remove Canada from the comparisons.
        df = df[df['display_name'] != 'Canada']


        collaborations_df = df.copy(deep=True)
        collaborations = graph_viz.create_choice_graph(
            graph_types=[VisualizationType.CHORD_CHART, VisualizationType.BUBBLE_CHART, VisualizationType.TREEMAP_CHART],
            dataframe=collaborations_df,
            data_columns=['number_of_collaborations',
                        'avg_citation_normalized_percentile',
                        'avg_fwci',
                        'avg_distinct_countries',
                        'avg_distinct_institutions',
                        'avg_apc_paid'],
            title='International Collaborations'
        )


        collaborations_filtered_df = df.copy(deep=True)
        collaborations_filtered_df = collaborations_filtered_df[collaborations_filtered_df['number_of_collaborations']>100]
        collaborations_filtered = graph_viz.create_choice_graph(
            graph_types=[VisualizationType.CHORD_CHART, VisualizationType.BUBBLE_CHART, VisualizationType.TREEMAP_CHART],
            dataframe=collaborations_filtered_df,
            data_columns=['number_of_collaborations',
                        'avg_citation_normalized_percentile',
                        'avg_fwci',
                        'avg_distinct_countries',
                        'avg_distinct_institutions',
                        'avg_apc_paid'],
            title='>100 Collaborations'
        )

        def network_analysis_unfiltered_markdown():
            USA = df[df['id'] == 'US']
            biggest_25 = df.nlargest(25, "number_of_collaborations")

            configured_grid = pn.widgets.Tabulator(
                biggest_25.drop(columns=['id']),
                theme='default',
                pagination='remote',
                page_size=25,       # Use a cleaner visual theme
                layout='fit_columns', # Make columns fit the available width
                formatters={
                    # Format large numbers with commas
                    'number_of_collaborations': NumberFormatter(format='0,0'),
                    # Format floats to 2 decimal places
                    'avg_fwci': NumberFormatter(format='0.00'),
                    'avg_citation_normalized_percentile': NumberFormatter(format='0.00'), 
                    'avg_distinct_countries': NumberFormatter(format='0.00'), 
                    'avg_distinct_institutions': NumberFormatter(format='0.00'), 
                    'avg_apc_paid': NumberFormatter(format='0.00')
                },
                show_index=False
            )
            return pn.Column(
                pn.pane.Markdown("### Collaborators:"),
                configured_grid,
                pn.pane.Markdown(f"""
                    - The United States is by far the largest international collaborative partner with Simon Fraser University authors with a total of {int(USA['number_of_collaborations'].iloc[0]):,},
                    followed by {str(biggest_25.iloc[1]['display_name'])} with a total of {int(biggest_25.iloc[1]['number_of_collaborations']):,}.
                    
                    - A large number of small nations dominate citation impact and normalized citation scores, this could be due to the relatively small number of works overall.
                """
                )
            )

        def network_analysis_markdown():
        
            filtered = df[df['number_of_collaborations']>100]
            configured_grid = pn.widgets.Tabulator(
                filtered.drop(columns=['id']),
                theme='default',
                pagination='remote',
                page_size=25,       # Use a cleaner visual theme
                layout='fit_columns', # Make columns fit the available width
                formatters={
                    # Format large numbers with commas
                    'number_of_collaborations': NumberFormatter(format='0,0'),
                    # Format floats to 2 decimal places
                    'avg_fwci': NumberFormatter(format='0.00'),
                    'avg_citation_normalized_percentile': NumberFormatter(format='0.00'), 
                    'avg_distinct_countries': NumberFormatter(format='0.00'), 
                    'avg_distinct_institutions': NumberFormatter(format='0.00'), 
                    'avg_apc_paid': NumberFormatter(format='0.00')
                },
                show_index=False
            )

            return pn.Column(
                pn.pane.Markdown("### Collaborators: (Filtered)"),
                configured_grid
            )


        return pn.Column(
            pn.pane.Markdown(
                f"""
                ### Collaborations 
                 
                This section covers the interational collaborations between SFU authors and the international community. Information pertaining to Simon Fraser University and Canada as a whole has been removed from the dataset to strictly focus on the international interactions. 
                """
            ),
            collaborations,
            network_analysis_unfiltered_markdown(),
            pn.HSpacer(),
            pn.pane.Markdown(
                f"""
                ### Filtering the dataset

                There are a notable amount of nations with a small number of collaborations, as a result many have high field-weighted citation impact scores and normalized citation scores due to having a small number of well-received works. To better investigate the data, only nations with more than 100 collaborations are included.
                """
            ),
            collaborations_filtered,
            network_analysis_markdown()
        )
    
    def appendix_page(self):
        return pn.Column(
            pn.pane.Markdown(f"""
            ### Appendix

            The following section will detail the process, including how data was collected, transformed and assessed in order to interpret the OpenAlex data.

            As a general overview the steps of the process can be summarized as:
            * Data extraction over HTTP using the OpenAlex API.
            * Cleaning and transforming the extracted data.
            * Saving the processed data as parquet for graph database ingestion.
            * Importing the parquet data as nodes and relationships into a Neo4J database.
            * Performing analysis on the data using Cypher.

            ### Extracting the Data

            OpenAlex presents an API for the retrieval of its constituent data types. For each institution, API client was utilized in conjunction with the corresponding institution ID to extract the following data:
            * Source objects by host institutions lineage.
            * Works objects with authorships that include the institution in its institution lineage values.
            * All works where the target institution has bestowed a grant upon it.
            * All institutions within the target institution's lineage.
            * All authors that have the target institution in its affiliation IDs.
            * A targeted list of 204 journals by ISSN.
            * Data pertaining to the target institution as funder, if possible.

            The extracted data is then converted into newline delimited JSON format and saved.

            ### Data Cleaning and Transformation
            The resultant newline delimited JSON data is then loaded into Polars dataframes. Each type of OpenAlex object is loaded into its own LazyFrame. The objects and their corresponding LazyFrame formats are as follows:

            - Institution
                * id
                * display_name
                * works_count
                * h_index
                * i10_index
                * 2yr_mean_citedness
                * counts_by_year
                * cited_by_count
                * lineage_root
                
            - Author
                * id
                * display_name
                * cited_by_count
                * works_count
                * h_index
                * i10_index
                * 2yr_mean_citedness

            - Funder
                * id
                * display_name
                * cited_by_count
                * country_code
                * grants_count
                * works_count
                * h_index
                * i10_index
                * 2yr_mean_citedness

            - Source
                * id
                * display_name
                * country_code
                * cited_by_count
                * host_organization
                * apc_usd
                * is_core
                * is_in_doaj
                * is_oa
                * works_count
                * issn_l
                * h_index
                * i10_index
                * 2yr_mean_citedness

            - Work
                * apc_paid
                * display_name
                * citation_normalized_percentile
                * cited_by_count
                * countries_distinct_count
                * fwci
                * id 
                * institutions_distinct_count
                * publication year
                * type
                * is_oa
                * oa_status
                * issns

            From the above objects, secondary nodes were derived in order to be represent relationships in the graph database. The secondary node types are as follows:
            * Topic  - Uses OpenAlex topics, each topic is given its own node.
            * Subfield - Derived from Topics, each topic is part of a subfield.
            * Field - Derived from Topics, each subfield is part of a field.
            * Domain - Derived from Topics, each field is part of a domain. 
            * Affiliated Institution - A dehydrated institutions object. Unlike primary institutions nodes, these nodes do not contain full information relating to an institution, and are introduced only to help map relationships between target institutions and non-target institutions. These nodes are derived from OpenAlex data, including from the affiliated insitutions and authorships fields.
            * ISSN - A basic node that only uses the ISSN as an ID. This node exists to make it easy to find works affiliated with a target journal.
            * Authorship - This node is derived from the authorship data found in OpenAlex works objects. An authorship will be related to a work, a target institution, and an author. A single work can have multiple authorships related to it.

            Furthermore to help complete the dataset the following nodes were added to the dataset.
            * Geographic - Nodes that contain geographic information pertaining to the world's nations. Uses ISO_3166 format.
            * Year - A basic node that uses the year as an id. This node is used to map relationships that relate to specific years easily, and make it easy to find these relationships in the graph database. 


            **Definitions:**
            - id - Contains the OpenAlex ID.
            - display_name - The OpenAlex display name.
            - works_count - The number of works affiliated with this institution.
            - h_index - Flattened from OpenAlex summary stats.
            - i10_index - Flattened from OpenAlex summary stats.
            - 2yr_mean_citedness - Flattened from OpenAlex summary stats.
            - counts_by_year - The number works produced by this institution in a given year
            - cited_by_count - The number of citations this institution has received.
            - lineage_root - Whether or not this object is the absolute root of the lineage tree. Or the base institution.
            - topic_share - A breakdown of the topics related to this object and their corresponding proportion of the total.
            - apc_usd - The article processing charge in USD.
            - host_organization - The organization that hosts the corresponding object.
            - country_code - An ISO_3166-1_alpha-2 country code corresponding to the country that the object is associated with.
            - is_core - Whether or not the source is identified as a core source by CWTS.
            - is_in_doaj - Whether or not the journal is listed in the Directory of Open Access Journals (DOAJ)
            - is_oa - Whether or not this object is open access.
            - issn_l - The lineage root of the source ISSN.
            - citation_normalized_percentile - The percentile of the object's citation count normalized by work type, publication year and subfield.
            - countries_distinct_count - The distinct number of countries involved with this object.
            - fwci - Field weighted citation impact.
            - institutions_distinct_count - The number of distinct institutions involved in this object (ex. number of institutions involved in a work via authorships.)
            - publication_year - The year that this object was published.
            - type - The type of pertaining to this object.
            - oa_status - The open access status of the object. 
            - issns - A list of ISSNS related to journals that host this object.

            In addition to these nodes, relationship nodes are detailed for later usage in the Graph Database, they are as follows:

            * Affiliated Institution &#8594; Affiliated Institution: Details any connecting lineage between affiliated institutions.
            * Affiliated Institution &#8594; Geographic: What geographic area the institution is situated in.
            * Affiliated Institution &#8594; Institution: A relationship helping map target instutitions to their dehydrated variants.
            * Author &#8594; Authorship: A relationship mapping an authorship to its author
            * Author &#8594; Affiliated Institution: A relationship mapping an author to their last known institution.
            * Author &#8594; Topic: A relationship mapping the types of topics an author has been involved in.
            * Author &#8594; Work: A relationship mapping a produced work to its author.
            * Author &#8594; Years: A relationship mapping the citations and works count of an author in a given year.
            * Authorship &#8594; Affiliated Instition: Maps an authorship with an affiliated institution.
            * Authorship &#8594; Work: Maps an authorship to the work it concerns.
            * Field &#8594; Domain: What domain the field belongs to.
            * Funder &#8594; Insitution: Maps the target funder to its target institution counterpart (SFU + U15 only).
            * Funder &#8594; Year: Gives details on funder actions in a given year.
            * Publisher &#8594; Source: Maps a source object to its host.
            * Institution &#8594; Author: Maps authors directly affiliated with the target institution.
            * Institution &#8594; Funders: Maps a target institution to its funder counterpart.
            * Institution &#8594; Geographic: What geographic area this insitution is situated in.
            * Institution &#8594; Publisher: Maps a target institution to its publisher role counterpart if applicable.
            * Institution &#8594; Institution: Details the lineage of institutions. This relationship exists between parent/child/sibling institutions.
            * Institution &#8594; Source: Source objects hosted by the institution. (Respositories, journals etc)
            * Institution &#8594; Topic: The topics an institution has works involving, including their proportion of the total.
            * Institution &#8594; Year: Contains information relating to actions within a given year (works, citations, etc).
            * Source &#8594; ISSN: Maps a source object to its ISSN if applicable.
            * Source &#8594; Topic: Describes the topics a source object contains, including the number of works involving said topic.
            * Source &#8594; Year: Contains information relating to actions within a given year (works, citations, etc).
            * Subfield &#8594; Field: The field that the subfield is in.
            * Topic &#8594;  Subfield: The subfield that the given topic is in.
            * Work &#8594; ISSN: The ISSN number of the journals that the work is hosted in, if applicable.
            * Work &#8594; Topic: The topics that a given work involves.
            * Work &#8594; Work: A relationship describing works referenced by the given work.
            * Work &#8594; Year: Describes the citations of a work in a given year.

            The resultant LazyFrames detailing the nodes and relationships are saved in Parquet format.

            ### Utilizing the Graph Database

            The parquet data is imported into the Neo4J graph database using the APOC plugin. The data is the grouped and analysed within the database. The data used for data visualization is saved in .csv format locally as running the Cypher queries can take some time. 
            """)
        )
    
    def _journal_section(self):
        # By journal, look at number of works by institution for journal
        pass

    def __init__(self):

        self.pages = {
            "Introduction": self.introduction_page,
            "General Comparison": self.general_comparison_page,
            "A Closer Look": self.analysis_page,
            "Topics": self.topics_page,
            #"Journals": self.journals_page,
            #"Research Strengths": self.research_page,
            "Collaborations": self.network_analysis_page,
            "Appendix": self.appendix_page
        }

        self.subsections = {
            "Introduction": {
            },
            "General Comparison": {
            }
        }