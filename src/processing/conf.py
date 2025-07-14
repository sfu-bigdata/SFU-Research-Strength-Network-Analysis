from dataclasses import dataclass
import polars as pl
from polars import LazyFrame
from typing import Iterable
from config import NodeType

@dataclass
class GraphRelationship:
    data : LazyFrame
    start_type: NodeType
    target_type: NodeType

@dataclass
class GraphTable:
    name: str
    type: NodeType
    data: LazyFrame

@dataclass
class GraphDataCollection:
    relationships: Iterable[GraphRelationship]
    nodes: Iterable[GraphTable]

designatedDirectories = {
    'authors': NodeType.author,
    'funders': NodeType.funder,
    'institutions': NodeType.SFU_U15_institution,
    'sources': NodeType.source,
    'works': NodeType.work,
    'topics': NodeType.topic,
}

schemas = {
        
        'works': pl.Schema({
            "id": pl.String,
            "doi": pl.String,
            "title": pl.String,
            "display_name": pl.String,
            "publication_year": pl.UInt32,
            "publication_date": pl.String,
            "ids": pl.String,
            "language": pl.String,
            "primary_location": pl.Struct([
                pl.Field("is_oa", pl.Boolean),
                pl.Field("landing_page_url", pl.String),
                pl.Field("pdf_url", pl.String),
                pl.Field("source", pl.Struct([ # Dehydrated Source object
                    pl.Field("id", pl.String),
                    pl.Field("display_name", pl.String),
                    pl.Field("issn_l", pl.String),
                    pl.Field("issn", pl.List(pl.String)),
                    pl.Field("type", pl.String),
                    pl.Field("host_organization", pl.String)
                ])),
                pl.Field("license", pl.String),
                pl.Field("license_id", pl.String),
                pl.Field("version", pl.String),
                pl.Field("is_accepted", pl.Boolean),
                pl.Field("is_published", pl.Boolean)
            ]),
            "type": pl.String,
            "type_crossref": pl.String,
            "indexed_in": pl.List(pl.String),
            "open_access": pl.Struct([
                pl.Field("is_oa", pl.Boolean),
                pl.Field("oa_status", pl.String),
                pl.Field("oa_url", pl.String),
                pl.Field("any_repository_has_fulltext", pl.Boolean),
            ]),
            "authorships": pl.List(pl.Struct([
                pl.Field("author_position", pl.String),
                pl.Field("author", pl.Struct([
                    pl.Field("id", pl.String),
                    pl.Field("display_name", pl.String),
                    pl.Field("orcid", pl.String)
                ])),
                pl.Field("institutions", pl.List(pl.Struct([
                    pl.Field("id", pl.String),
                    pl.Field("display_name", pl.String),
                    pl.Field("ror", pl.String),
                    pl.Field("country_code", pl.String),
                    pl.Field("type", pl.String),
                    pl.Field("lineage", pl.List(pl.String))
                ]))),
                pl.Field("countries", pl.List(pl.String)),
                pl.Field("is_corresponding", pl.Boolean),
                pl.Field("raw_author_name", pl.String),
                pl.Field("raw_affiliation_strings", pl.List(pl.String)),
                pl.Field("affiliations", pl.List(pl.Struct([
                    pl.Field("raw_affiliation_string", pl.String),
                    pl.Field("institution_ids", pl.List(pl.String))
                ])))
            ])),
            "institution_assertions": pl.List(pl.String),
            "countries_distinct_count": pl.UInt32,
            "institutions_distinct_count": pl.UInt32,
            "corresponding_author_ids": pl.List(pl.String),
            "corresponding_institution_ids": pl.List(pl.String),
            "apc_list": pl.Struct([
                pl.Field("value", pl.Int32),
                pl.Field("currency", pl.String),
                pl.Field("provenance", pl.String),
                pl.Field("value_usd", pl.Int32),
            ]),
            "apc_paid": pl.Struct([
                pl.Field("value", pl.Int32),
                pl.Field("currency", pl.String),
                pl.Field("provenance", pl.String),
                pl.Field("value_usd", pl.Int32),
            ]),
            "fwci": pl.Float64,
            "has_fulltext": pl.Boolean,
            "cited_by_count": pl.UInt32,
            "citation_normalized_percentile": pl.Struct([
                pl.Field("value", pl.Float64),
                pl.Field("is_in_top_1_percent", pl.Boolean),
                pl.Field("is_in_top_10_percent", pl.Boolean),
            ]),
            "cited_by_percentile_year": pl.Struct([
                pl.Field("min", pl.UInt32),
                pl.Field("max", pl.UInt32),
            ]),
            "biblio": pl.Struct([
                pl.Field("volume", pl.String),
                pl.Field("issue", pl.String),
                pl.Field("first_page", pl.String),
                pl.Field("last_page", pl.String),
            ]),
            "is_retracted": pl.Boolean,
            "is_paratext": pl.Boolean,
            "primary_topic": pl.Struct([
                pl.Field("id", pl.String),
                pl.Field("display_name", pl.String),
                pl.Field("subfield", pl.Struct([
                    pl.Field("id", pl.String),
                    pl.Field("display_name", pl.String),
                ])),
                pl.Field("field", pl.Struct([
                    pl.Field("id", pl.String),
                    pl.Field("display_name", pl.String),
                ])),
                pl.Field("domain", pl.Struct([
                    pl.Field("id", pl.String),
                    pl.Field("display_name", pl.String),
                ])),
                pl.Field("score", pl.Float32)
            ]),
            "topics": pl.List(pl.Struct([
                pl.Field("id", pl.String),
                pl.Field("display_name", pl.String),
                pl.Field("subfield", pl.Struct([
                    pl.Field("id", pl.String),
                    pl.Field("display_name", pl.String),
                ])),
                pl.Field("field", pl.Struct([
                    pl.Field("id", pl.String),
                    pl.Field("display_name", pl.String),
                ])),
                pl.Field("domain", pl.Struct([
                    pl.Field("id", pl.String),
                    pl.Field("display_name", pl.String),
                ])),
                pl.Field("score", pl.Float32)
            ])),
            "keywords": pl.List(pl.Struct([
                pl.Field("id", pl.String),
                pl.Field("display_name", pl.String),
                pl.Field("score", pl.Float32),
            ])),
            "concepts": pl.List(pl.Struct([
                pl.Field("id", pl.String),
                pl.Field("wikidata", pl.String),
                pl.Field("display_name", pl.String),
                pl.Field("level", pl.Int8),
                pl.Field("score", pl.Float32),
            ])),
            "mesh": pl.List(pl.Struct([
                pl.Field("descriptor_ui", pl.String),
                pl.Field("descriptor_name", pl.String),
                pl.Field("qualifier_ui", pl.String),
                pl.Field("qualifier_name", pl.String),
                pl.Field("is_major_topic", pl.Boolean)
            ])),
            "locations_count": pl.UInt32,
            "locations": pl.List(pl.Struct([
                pl.Field("is_oa", pl.Boolean),
                pl.Field("landing_page_url", pl.String),
                pl.Field("pdf_url", pl.String),
                pl.Field("source", pl.Struct([ # Dehydrated Source object
                    pl.Field("id", pl.String),
                    pl.Field("display_name", pl.String),
                    pl.Field("issn_l", pl.String),
                    pl.Field("issn", pl.List(pl.String)),
                    pl.Field("type", pl.String),
                    pl.Field("host_organization", pl.String)
                ])),
                pl.Field("license", pl.String),
                pl.Field("license_id", pl.String),
                pl.Field("version", pl.String),
                pl.Field("is_accepted", pl.Boolean),
                pl.Field("is_published", pl.Boolean)
            ])),
            "best_oa_location": pl.Struct([
                pl.Field("is_oa", pl.Boolean),
                pl.Field("landing_page_url", pl.String),
                pl.Field("pdf_url", pl.String),
                pl.Field("source", pl.Struct([ # Dehydrated Source object
                    pl.Field("id", pl.String),
                    pl.Field("display_name", pl.String),
                    pl.Field("issn_l", pl.String),
                    pl.Field("issn", pl.List(pl.String)),
                    pl.Field("type", pl.String),
                    pl.Field("host_organization", pl.String)
                ])),
                pl.Field("license", pl.String),
                pl.Field("license_id", pl.String),
                pl.Field("version", pl.String),
                pl.Field("is_accepted", pl.Boolean),
                pl.Field("is_published", pl.Boolean)
            ]),
            "sustainable_development_goals": pl.List(pl.Struct([
                pl.Field("id", pl.String),
                pl.Field("display_name", pl.String),
                pl.Field("score", pl.Float32),
            ])),
            "grants": pl.List(pl.Struct([
                pl.Field("funder",pl.String),
                pl.Field("funder_display_name", pl.String),
                pl.Field("award_id", pl.String),
            ])),
            "datasets": pl.List(pl.String),
            "versions": pl.List(pl.String),
            "referenced_works_count": pl.Int32,
            "referenced_works": pl.List(pl.String),
            "related_works": pl.List(pl.String),
            "abstract_inverted_index": pl.String, # Best to load as Object and process if keys are unknown/dynamic.
                                                  # Alternatively, a specific `pl.Struct` if all keys are known and static.
            "abstract_inverted_index_v3": pl.String,
            "cited_by_api_url": pl.String,
            "counts_by_year": pl.List(pl.Struct([
                pl.Field("year", pl.UInt32),
                pl.Field("cited_by_count", pl.UInt32),
            ])),
            "updated_date": pl.String,
            "created_date": pl.String,
            "institutions_distinct_count": pl.UInt32,
        })
}