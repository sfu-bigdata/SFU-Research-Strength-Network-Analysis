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
                
                ### Include more stuff later on, breakdown of objects etc.
                """
            )
        )
    
    def introduction_page(self):
        return pn.Column(
            self._introduction_section(),
    )

    def _institution_comparison_section(self):
        '''
        This section will provide a general overview of the data.
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
                dataframe=df,
                data_columns=target_columns,
                title='Data Over Time'
            )

            min_year = df['year'].min()
            max_year = df['year'].max()

            sfu = df[df['id'] == SFU_TARGET_INSTITUTION_ID].set_index('year')

            u15 = df[df['id'] != SFU_TARGET_INSTITUTION_ID][['year']+target_columns]
            u15 = u15.groupby('year').mean()

            df = df.set_index('id')
            df = pd.concat([df['year'], df[['year']+target_columns].groupby('year').rank(method='first', ascending=False)], axis=1).reset_index()
            sfu_ranked = df[df['id'] == SFU_TARGET_INSTITUTION_ID].set_index('year')

            return pn.Column(
                graph,
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

                    The following section will contain a visualization of general OpenAlex data relating to the target institutions.

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
                    additional_filters=['id']
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
                    data_columns=numerical_columns
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
                data_columns=['total']
            )

            
            return pn.Column(
                categorical,
                numerical,
                temporal
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
                data_columns=['cited_by_count', 'works_count']
            )

            affiliated = graph_viz.create_choice_graph(
                graph_types=[VisualizationType.THEME_RIVER_CHART, VisualizationType.BAR_RACE_CHART, VisualizationType.STACKED_AREA_CHART, VisualizationType.SMOOTHED_LINE_CHART],
                dataframe=affiliated_per_year,
                data_columns=['affiliations']
            )

            return pn.Column(
                authors,
                pn.HSpacer(),
                affiliated
            )
        
        def funders_analysis():
            pass

        def topics_analysis():
            '''
            General overview of Topics by institution
            Topics by year
            '''
            
            '''
            When looking at topics include:
                fwci by topic
                institutions by topic
                countries by topic
                type by topic
                citations count - total and by year
            '''
            pass

        return pn.Column(
            works_analysis(),
            pn.HSpacer(),
            authors_analysis(),
            #pn.HSpacer(),
            #topics_analysis()
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
        return pn.Column(

        )
    
    def appendix_page(self):
        return pn.Column(

        )
    
    def _journal_section(self):
        # By journal, look at number of works by institution for journal
        pass

    def __init__(self):
        viz_data = VisualizationData()

        self.pages = {
            "Introduction": self.introduction_page,
            "General Comparison": self.general_comparison_page,
            "A Closer Look": self.analysis_page,
            "Journals": self.journals_page,
            "Research Strengths": self.research_page,
            "Network Analysis": self.network_analysis_page,
            "Appendix": self.appendix_page
        }

        self.subsections = {
            "Introduction": {
            },
            "General Comparison": {
            }
        }