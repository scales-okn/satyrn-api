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

        expected_results = {'totalCount': 129, 'page': 0, 'batchSize': 10, 'activeCacheRange': [0, 100], 'results': [{'amount': '10000.0', 'inState': 'False', 'electionYear': '2010', 'contributionRecipient': 'HYNES, DANIEL W', 'contributionDate': '2020-04-02 14:30:21'}, {'amount': '2500.0', 'inState': 'True', 'electionYear': '2010', 'contributionRecipient': 'QUINN III, PATRICK JOSEPH (PAT) & SIMON, SHEILA J', 'contributionDate': 'None'}, {'amount': '5000.0', 'inState': 'True', 'electionYear': '2010', 'contributionRecipient': 'QUINN III, PATRICK JOSEPH (PAT) & SIMON, SHEILA J', 'contributionDate': 'None'}, {'amount': '100.0', 'inState': 'True', 'electionYear': '2010', 'contributionRecipient': 'HYNES, DANIEL W', 'contributionDate': 'None'}, {'amount': '25000.0', 'inState': 'True', 'electionYear': '2010', 'contributionRecipient': 'QUINN III, PATRICK JOSEPH (PAT) & SIMON, SHEILA J', 'contributionDate': 'None'}, {'amount': '5000.0', 'inState': 'True', 'electionYear': '2010', 'contributionRecipient': 'HYNES, DANIEL W', 'contributionDate': 'None'}, {'amount': '1000.0', 'inState': 'True', 'electionYear': '2010', 'contributionRecipient': 'QUINN III, PATRICK JOSEPH (PAT) & SIMON, SHEILA J', 'contributionDate': 'None'}, {'amount': '500.0', 'inState': 'True', 'electionYear': '2010', 'contributionRecipient': 'QUINN III, PATRICK JOSEPH (PAT) & SIMON, SHEILA J', 'contributionDate': 'None'}, {'amount': '1000.0', 'inState': 'True', 'electionYear': '2010', 'contributionRecipient': 'HYNES, DANIEL W', 'contributionDate': 'None'}, {'amount': '250.0', 'inState': 'True', 'electionYear': '2010', 'contributionRecipient': 'QUINN III, PATRICK JOSEPH (PAT) & SIMON, SHEILA J', 'contributionDate': 'None'}]}
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
            "relationships": ["ContribToContributor"]
        }
        urllink = make_results_url(self.url, self.ringid, self.versionid, "Contribution", "results", opts)

        resp = requests.get(urllink, headers=self.headers, json=json_opts)

        # resp = requests.get(urllink, headers=self.headers)
        results = json.loads(resp.content.decode('utf-8'))
        expected_results = {'totalCount': 10, 'page': 0, 'batchSize': 10, 'activeCacheRange': [0, 100], 'results': [{'amount': '10000.0', 'inState': 'False', 'electionYear': '2010', 'contributionRecipient': 'HYNES, DANIEL W', 'contributionDate': '2020-04-02 14:30:21'}, {'amount': '3000.0', 'inState': 'False', 'electionYear': '2014', 'contributionRecipient': 'QUINN III, PATRICK JOSEPH (PAT) & VALLAS, PAUL GUST', 'contributionDate': 'None'}, {'amount': '250.0', 'inState': 'False', 'electionYear': '2010', 'contributionRecipient': 'HYNES, DANIEL W', 'contributionDate': 'None'}, {'amount': '5000.0', 'inState': 'False', 'electionYear': '2014', 'contributionRecipient': 'QUINN III, PATRICK JOSEPH (PAT) & VALLAS, PAUL GUST', 'contributionDate': 'None'}, {'amount': '5000.0', 'inState': 'False', 'electionYear': '2014', 'contributionRecipient': 'QUINN III, PATRICK JOSEPH (PAT) & VALLAS, PAUL GUST', 'contributionDate': 'None'}, {'amount': '10000.0', 'inState': 'False', 'electionYear': '2014', 'contributionRecipient': 'QUINN III, PATRICK JOSEPH (PAT) & VALLAS, PAUL GUST', 'contributionDate': 'None'}, {'amount': '1500.0', 'inState': 'False', 'electionYear': '2010', 'contributionRecipient': 'HYNES, DANIEL W', 'contributionDate': 'None'}, {'amount': '5000.0', 'inState': 'False', 'electionYear': '2014', 'contributionRecipient': 'QUINN III, PATRICK JOSEPH (PAT) & VALLAS, PAUL GUST', 'contributionDate': 'None'}, {'amount': '7602.3', 'inState': 'False', 'electionYear': '2014', 'contributionRecipient': 'QUINN III, PATRICK JOSEPH (PAT) & VALLAS, PAUL GUST', 'contributionDate': 'None'}, {'amount': '1000.0', 'inState': 'False', 'electionYear': '2014', 'contributionRecipient': 'QUINN III, PATRICK JOSEPH (PAT) & VALLAS, PAUL GUST', 'contributionDate': 'None'}]}
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
        expected_results = {'totalCount': 1, 'page': 0, 'batchSize': 10, 'activeCacheRange': [0, 100], 'results': [{'name': 'INTERNATIONAL BROTHERHOOD OF ELECTRICAL WORKERS / IBEW', 'parentOrg': 'NATIONAL AFL-CIO', 'area': 'Electrical workers/IBEW'}]}

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
            "relationships": ["ContribToContributor"]
        }
        urllink = make_results_url(self.url, self.ringid, self.versionid, "Contribution", "results", opts)

        resp = requests.get(urllink, headers=self.headers, json=json_opts)

        results = json.loads(resp.content.decode('utf-8'))
        expected_results = {'totalCount': 3, 'page': 0, 'batchSize': 10, 'activeCacheRange': [0, 100], 'results': [{'amount': '5300.0', 'inState': 'True', 'electionYear': '2014', 'contributionRecipient': 'QUINN III, PATRICK JOSEPH (PAT) & VALLAS, PAUL GUST', 'contributionDate': 'None'}, {'amount': '5000.0', 'inState': 'True', 'electionYear': '2014', 'contributionRecipient': 'QUINN III, PATRICK JOSEPH (PAT) & VALLAS, PAUL GUST', 'contributionDate': 'None'}, {'amount': '500.0', 'inState': 'True', 'electionYear': '2014', 'contributionRecipient': 'QUINN III, PATRICK JOSEPH (PAT) & VALLAS, PAUL GUST', 'contributionDate': 'None'}]}
        self.assertEqual(expected_results, results)

    def test_filter_fail_no_result(self):
        # ground truth verified
        opts = {"electionYear": "20132312320"}
        urllink = make_results_url(self.url, self.ringid, self.versionid, "Contribution", "results", opts)

        resp = requests.get(urllink, headers=self.headers)
        results = json.loads(resp.content.decode('utf-8'))
        expected_results = {'totalCount': 0, 'page': 0, 'batchSize': 10, 'activeCacheRange': [0, 100], 'results': []}
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
            "relationships": ["ContribToContributor"]
        }
        urllink = make_results_url(self.url, self.ringid, self.versionid, "Contribution", "results", opts)

        resp = requests.get(urllink, headers=self.headers, json=json_opts)

        results = json.loads(resp.content.decode('utf-8'))
        expected_results = {"totalCount": 178, "page": 0, "batchSize": 5, "activeCacheRange": [0, 50], "results": [{"amount": "2500.0", "inState":
                            "True", "electionYear": "2010", "contributionRecipient": "QUINN III, PATRICK JOSEPH (PAT) & SIMON, SHEILA J",
                            "contributionDate": "None"}, {"amount": "5000.0", "inState": "True", "electionYear": "2010", "contributionRecipient":
                            "QUINN III, PATRICK JOSEPH (PAT) & SIMON, SHEILA J", "contributionDate": "None"}, {"amount": "100.0", "inState": "True",
                            "electionYear": "2010", "contributionRecipient": "HYNES, DANIEL W", "contributionDate": "None"}, {"amount": "25000.0",
                            "inState": "True", "electionYear": "2010", "contributionRecipient": "QUINN III, PATRICK JOSEPH (PAT) & SIMON, SHEILA J",
                            "contributionDate": "None"}, {"amount": "1000.0", "inState": "False", "electionYear": "2014", "contributionRecipient":
                            "QUINN III, PATRICK JOSEPH (PAT) & VALLAS, PAUL GUST", "contributionDate": "None"}]}
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
        expected_results = {"totalCount": 10, "page": 1, "batchSize": 1, "activeCacheRange": [0, 10], "results": [{"amount": "100.0", "inState":
                            "True", "electionYear": "2010", "contributionRecipient": "QUINN III, PATRICK JOSEPH (PAT) & SIMON, SHEILA J",
                            "contributionDate": "None"}]}
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
        expected_results = {"totalCount": 60, "page": 1, "batchSize": 1, "activeCacheRange": [0, 10], "results": [{"amount": "250.0", "inState":
"True", "electionYear": "2014", "contributionRecipient": "QUINN III, PATRICK JOSEPH (PAT) & VALLAS, PAUL GUST",
"contributionDate": "None"}]}
        self.assertEqual(expected_results, results)

if __name__ == '__main__':
    unittest.main()