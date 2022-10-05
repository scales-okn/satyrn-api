'''
This file is part of Satyrn.
Satyrn is free software: you can redistribute it and/or modify it under 
the terms of the GNU General Public License as published by the Free Software Foundation, 
either version 3 of the License, or (at your option) any later version.
Satyrn is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. 
See the GNU General Public License for more details.
You should have received a copy of the GNU General Public License along with Satyrn. 
If not, see <https://www.gnu.org/licenses/>.
'''
import unittest

import os


import requests

from helper_func import make_results_url
import json

class TestFilter(unittest.TestCase):

    def setUp(self):

        self.apikey = os.environ.get("API_KEY", "thisisatestkey")
        self.url = os.environ.get("TEST_URL", "http://127.0.0.1:5000/")
        self.ringid = "20e114c2-ef05-490c-bdd8-f6f271a6733f"
        self.versionid = "1"
        self.headers ={
            "x-api-key": self.apikey
        }

    def test_filter_year(self):
        # ground truth verified
        opts = {"electionYear": "2010"}
        
        urllink = make_results_url(self.url, self.ringid, self.versionid, "Contribution", "results", opts)
        resp = requests.get(urllink, headers=self.headers)
        results = json.loads(resp.content.decode('utf-8'))

        expected_results = {'totalCount': 129, 'page': 0, 'batchSize': 10, 'activeCacheRange': [0, 100], "results": [{}, {}, {}, {}, {}, {}, {},{}, {}, {}]}
        self.assertEqual(expected_results, results)

    def test_filter_contributor(self):
        # ground truth verified
        opts = {}
        json_opts = {
            "query": {
                "AND": [
                    [
                        {"entity": "Contributor",
                        "field": "name"},
                        "INTERNATIONAL BROTHERHOOD OF ELECTRICAL WORKERS / IBEW",
                        "exact"
                    ]
                ]
            },
            "relationships": ["ContribToContributorInfo"]
        }
        urllink = make_results_url(self.url, self.ringid, self.versionid, "Contribution", "results", opts)

        resp = requests.get(urllink, headers=self.headers, json=json_opts)

        # resp = requests.get(urllink, headers=self.headers)
        results = json.loads(resp.content.decode('utf-8'))
        expected_results = {"totalCount": 10, "page": 0, "batchSize": 10, "activeCacheRange": [0, 100], "results": [{}, {}, {}, {}, {}, {}, {}, {},{}, {}]}
        self.assertEqual(expected_results, results)

    def test_filter_contributor_targetentity(self):
        # ground truth verified
        opts = {}
        json_opts = {
            "query": {
                "AND": [
                    [
                        {"entity": "Contributor",
                        "field": "name"},
                        "INTERNATIONAL BROTHERHOOD OF ELECTRICAL WORKERS / IBEW",
                        "exact"
                    ]
                ]
            },
            "relationships": []
        }
        urllink = make_results_url(self.url, self.ringid, self.versionid, "Contributor", "results", opts)
        resp = requests.get(urllink, headers=self.headers, json=json_opts)

        results = json.loads(resp.content.decode('utf-8'))
        expected_results = {"totalCount": 1, "page": 0, "batchSize": 10, "activeCacheRange": [0, 100], "results": [{}]}

        self.assertEqual(expected_results, results)


    def test_filter_area_year(self):
        # ground truth verified
        opts = {}
        json_opts = {
            "query": {
                "AND": [
                    [
                        {"entity": "Contributor",
                        "field": "area"},
                        "Labor unions",
                        "exact"
                    ],
                    [
                        {"entity": "Contribution",
                        "field": "electionYear"},
                        2014,
                        "exact"
                    ]
                ]
            },
            "relationships": ["ContribToContributorInfo"]
        }
        urllink = make_results_url(self.url, self.ringid, self.versionid, "Contribution", "results", opts)

        resp = requests.get(urllink, headers=self.headers, json=json_opts)

        results = json.loads(resp.content.decode('utf-8'))
        expected_results = {"totalCount": 3, "page": 0, "batchSize": 10, "activeCacheRange": [0, 100], "results": [{}, {}, {}]}
        self.assertEqual(expected_results, results)

    def test_filter_fail_no_result(self):
        # ground truth verified
        opts = {"electionYear": "20132312320"}
        urllink = make_results_url(self.url, self.ringid, self.versionid, "Contribution", "results", opts)

        resp = requests.get(urllink, headers=self.headers)
        results = json.loads(resp.content.decode('utf-8'))
        expected_results = {"totalCount": 0, "page": 0, "batchSize": 10, "activeCacheRange": [0, 100], "results": []}
        self.assertEqual(expected_results, results)

    def tearDown(self):
        pass


    def test_filter_area_contains(self):
        # ground truth verified
        opts = {"batchSize": 5}
        json_opts = {
            "query": {
                "AND": [
                    [
                        {"entity": "Contributor",
                        "field": "area"},
                        "union",
                        "contains"
                    ]
                ]
            },
            "relationships": ["ContribToContributorInfo"]
        }
        urllink = make_results_url(self.url, self.ringid, self.versionid, "Contribution", "results", opts)
        resp = requests.get(urllink, headers=self.headers, json=json_opts)

        results = json.loads(resp.content.decode('utf-8'))
        expected_results = {"totalCount": 178, "page": 0, "batchSize": 5, "activeCacheRange": [0, 50], "results": [{}, {}, {}, {}, {}]}
        self.assertEqual(expected_results, results)


    def test_filter_amount_range(self):
        # ground truth verified
        opts = {"batchSize": 1, "page": 1}
        json_opts = {
            "query": {
                "AND": [
                    [
                        {"entity": "Contribution",
                        "field": "amount"},
                        [0,200],
                        "range"
                    ]
                ]
            },
            "relationships": []
        }

        urllink = make_results_url(self.url, self.ringid, self.versionid, "Contribution", "results", opts)
        resp = requests.get(urllink, headers=self.headers, json=json_opts)

        results = json.loads(resp.content.decode('utf-8'))
        expected_results = {"totalCount": 10, "page": 1, "batchSize": 1, "activeCacheRange": [0, 10], "results": [{}]}
        self.assertEqual(expected_results, results)

    def test_filter_amount_lessthan(self):
        # ground truth verified
        opts = {"batchSize": 1, "page": 1}
        json_opts = {
            "query": {
                "AND": [
                    [
                        {"entity": "Contribution",
                        "field": "amount"},
                        1000,
                        "lessthan"
                    ]
                ]
            },
            "relationships": []
        }
        urllink = make_results_url(self.url, self.ringid, self.versionid, "Contribution", "results", opts)

        resp = requests.get(urllink, headers=self.headers, json=json_opts)

        results = json.loads(resp.content.decode('utf-8'))
        expected_results = {"totalCount": 60, "page": 1, "batchSize": 1, "activeCacheRange": [0, 10], "results": [{}]}

        self.assertEqual(expected_results, results)

if __name__ == '__main__':
    unittest.main()