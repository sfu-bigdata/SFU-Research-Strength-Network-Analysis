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

    
    def _institution_comparison_section(self):
        '''
        This section will provide a general overview of the data.
        '''
        # As institution
            # Beginning with the total count of each type of data.
            # Number of works, Number of authors, Number of total related authorships
            # Citations by year for each institution - filter by domain, field, subfield, topic
            # Topics, Topic Share, Counts by Topic

        # As funders, where do these institutions stand?
            # Number of grants, works, citations
            # Summary stats scores comparison, and then adjusted for works output
            # Citations by year for each institution - filter by domain, field, subfield, topic
            # Topics, Topic Share, Counts by Topic

        # Another section, filter by Topic and Institution


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
        
        return pn.Column(
            pn.pane.Markdown(
                f"""
                    ## A General Comparison

                    The following section will contain a visualization of general OpenAlex data relating to the target institutions.

                """
            ),
            work_comparison(),
            pn.HSpacer(),
            author_comparison()
        )

        


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
            self._institution_comparison_section()
    )
    
    
    def _journal_section(self):
        # By journal, look at number of works by institution for journal
        pass

    def __init__(self):
        viz_data = VisualizationData()

        self.pages = {
            "Introduction": self.introduction_page,
            "Other": self.introduction_page
        }

        self.subsections = {
            "Introduction": {
                "Summary": "#introduction",
                "Basic Comparisons": "#a-general-comparison"
            }
        }