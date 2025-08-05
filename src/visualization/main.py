import panel as pn
import holoviews as hv
import panel.widgets as pw

from . import css
from .report import Report
import param

def display_report():
    '''
    Generate and display the Panel report template
    '''
    pn.extension(
        'echarts',
        raw_css=[css.GLOBAL_CSS]
    )

    report = Report()

    generated_pages = {}
    
    current_page = pn.reactive.Reactive()
    current_page.param.add_parameter(
        'value', param.String(
            default="Introduction"
        )
    )

    @pn.depends(current_page.param.value)
    def sidebar_navigation(value):
        sidebar = [
            pn.pane.Markdown("## Contents")
        ]

        for page in report.pages.keys():

            link = pn.widgets.Button(name=page,
                                     css_classes = ["sidebar-page-link", "active-page"] if page==value else ["sidebar-page-link"],
                                     align="start")

            def click_fx(event, name=page):
                current_page.value = name
            
            link.on_click(click_fx)

            sidebar.append(link)

            if page==value:
                subsections = report.subsections.get(value, {})
                for name, ref in subsections.items():
                    sidebar.append(
                        pn.pane.Markdown(f"- <a href='{ref}' class='sidepar-subsection-link'>{name}</a>", margin=(0, 0, 0, 15))
                    )
        
        return pn.Column(*sidebar, sizing_mode="stretch_width")
    

    @pn.depends(current_page.param.value)
    def serve_page(value):
        # If the page has not been generated.
        if value not in generated_pages:
            generated_pages[value] = report.pages[value]()

        return generated_pages[value]

    template = pn.template.FastListTemplate(
                title="Research Strength Network Analysis",
                collapsed_right_sidebar=True,
                sidebar=[
                    sidebar_navigation
                ],
                main=[serve_page]
            )

    pn.serve(template)