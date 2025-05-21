'''
Tests for the OpenAlex API utility class
'''

import pytest, httpx
from time import sleep
from config import conf
from src import openalex_api

# Ensure that the endpoint urls work
def test_entities():
    with httpx.Client() as client:
        for endpoint in conf.APIEndpoints:
            res = client.get(endpoint.value)
            assert res.status_code == 200
            sleep(0.1)


# Test pagination (Basic)
def test_basic_paging():
    api = openalex_api.OpenAlexApi()
    res = api.retrieve_list(
        endpoint=conf.APIEndpoints.WORKS,
        pagination=True,
        pagination_type=conf.PaginationTypes.BASIC,
        items_per_page=100,
        page=10
    )
    assert(len(res)) == 1
    res = res[-1]
    assert res.status_code == 200
    res_json = res.json()    
    assert "results" in res_json
    assert len(res_json["results"])==100 


# Test cursor based pagination for a set number of pages
def test_cursor_paging():
    api = openalex_api.OpenAlexApi()
    
    requested_number_of_pages = 2
    items_per_page = 200

    res_set = api.retrieve_list(
        endpoint=conf.APIEndpoints.WORKS,
        pagination=True,
        pagination_type=conf.PaginationTypes.CURSOR,
        items_per_page=items_per_page,
        pages_count=requested_number_of_pages
    )

    assert len(res_set)==requested_number_of_pages

    total_size = 0

    for res in res_set:
        assert res.status_code == 200
        res_json = res.json()
        assert "meta" and "results" in res_json
        total_size += len(res_json["results"])

    assert total_size == items_per_page*requested_number_of_pages    

'''
Test cursor based pagination on a relatively small dataset
Country code: MA - Morocco
May 20 2025 - there should be 178 institutions
Will effective test if paging successfully terminates when encountering a null value and gets all the results
'''
def test_cursor_paging_unrestricted():
    api = openalex_api.OpenAlexApi()

    res_set = api.retrieve_list(
        endpoint=conf.APIEndpoints.INSTITUTIONS,
        pagination=True,
        pagination_type=conf.PaginationTypes.CURSOR,
        items_per_page=100,
        filter='country_code:ma'
    )
    
    # At the current time, the total registered institutions is 178
    total_institutions = 0
    for res in res_set:
        assert res.status_code == 200
        res_json = res.json()
        assert "results" in res_json
        total_institutions+=len(res_json["results"])

    assert total_institutions == 178