'''
Utility class to generate panel report
'''
from dataclasses import dataclass
import panel as pn
from .visualization_data import VisualizationData
import pandas as pd
from config import NodeType
from .config import VisualizationDataPaths
import holoviews as hv
from . import css
from ast import literal_eval

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
                """<a id='introduction_summmary'></a>"""
            ),
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
            self._introduction_section()
        )
    
    def _summary_section(self):
        '''
        This section will provide a general overview of the data.
        '''
        pass

    
    def summary_page(self):
        return pn.Column(
            self._summary_section()
        )
    
    def __init__(self):
        viz_data = VisualizationData()

        self.pages = {
            "Introduction": self.introduction_page,
            "Other": self.introduction_page
        }

        self.subsections = {
            "Introduction": {
                "Summary": "#introduction_summary"
            },
            "Other": {
                "Other": "#introduction_summary"
            }
        }