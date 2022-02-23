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
        opts = {"contributor": "INTERNATIONAL BROTHERHOOD OF ELECTRICAL WORKERS / IBEW"}
        urllink = make_results_url(self.url, self.ringid, self.versionid, "Contribution", "results", opts)

        resp = requests.get(urllink, headers=self.headers)
        results = json.loads(resp.content.decode('utf-8'))
        expected_results = {'totalCount': 10, 'page': 0, 'batchSize': 10, 'activeCacheRange': [0, 100], 'results': [{'contributorName': 'INTERNATIONAL BROTHERHOOD OF ELECTRICAL WORKERS / IBEW', 'contributorArea': 'Electrical workers/IBEW', 'electionYear': 2010, 'recipient': 'HYNES, DANIEL W', 'amount': 10000.0}, {'contributorName': 'INTERNATIONAL BROTHERHOOD OF ELECTRICAL WORKERS / IBEW', 'contributorArea': 'Electrical workers/IBEW', 'electionYear': 2014, 'recipient': 'QUINN III, PATRICK JOSEPH (PAT) & VALLAS, PAUL GUST', 'amount': 3000.0}, {'contributorName': 'INTERNATIONAL BROTHERHOOD OF ELECTRICAL WORKERS / IBEW', 'contributorArea': 'Electrical workers/IBEW', 'electionYear': 2010, 'recipient': 'HYNES, DANIEL W', 'amount': 250.0}, {'contributorName': 'INTERNATIONAL BROTHERHOOD OF ELECTRICAL WORKERS / IBEW', 'contributorArea': 'Electrical workers/IBEW', 'electionYear': 2014, 'recipient': 'QUINN III, PATRICK JOSEPH (PAT) & VALLAS, PAUL GUST', 'amount': 5000.0}, {'contributorName': 'INTERNATIONAL BROTHERHOOD OF ELECTRICAL WORKERS / IBEW', 'contributorArea': 'Electrical workers/IBEW', 'electionYear': 2014, 'recipient': 'QUINN III, PATRICK JOSEPH (PAT) & VALLAS, PAUL GUST', 'amount': 5000.0}, {'contributorName': 'INTERNATIONAL BROTHERHOOD OF ELECTRICAL WORKERS / IBEW', 'contributorArea': 'Electrical workers/IBEW', 'electionYear': 2014, 'recipient': 'QUINN III, PATRICK JOSEPH (PAT) & VALLAS, PAUL GUST', 'amount': 10000.0}, {'contributorName': 'INTERNATIONAL BROTHERHOOD OF ELECTRICAL WORKERS / IBEW', 'contributorArea': 'Electrical workers/IBEW', 'electionYear': 2010, 'recipient': 'HYNES, DANIEL W', 'amount': 1500.0}, {'contributorName': 'INTERNATIONAL BROTHERHOOD OF ELECTRICAL WORKERS / IBEW', 'contributorArea': 'Electrical workers/IBEW', 'electionYear': 2014, 'recipient': 'QUINN III, PATRICK JOSEPH (PAT) & VALLAS, PAUL GUST', 'amount': 5000.0}, {'contributorName': 'INTERNATIONAL BROTHERHOOD OF ELECTRICAL WORKERS / IBEW', 'contributorArea': 'Electrical workers/IBEW', 'electionYear': 2014, 'recipient': 'QUINN III, PATRICK JOSEPH (PAT) & VALLAS, PAUL GUST', 'amount': 7602.3}, {'contributorName': 'INTERNATIONAL BROTHERHOOD OF ELECTRICAL WORKERS / IBEW', 'contributorArea': 'Electrical workers/IBEW', 'electionYear': 2014, 'recipient': 'QUINN III, PATRICK JOSEPH (PAT) & VALLAS, PAUL GUST', 'amount': 1000.0}]}
        print(results)
        self.assertEqual(expected_results, results)

    def test_filter_area_year(self):
        # ground truth verified
        opts = {"electionYear": "2014", "contributorArea": "Labor unions"}
        urllink = make_results_url(self.url, self.ringid, self.versionid, "Contribution", "results", opts)

        resp = requests.get(urllink, headers=self.headers)
        results = json.loads(resp.content.decode('utf-8'))
        expected_results = {'totalCount': 3, 'page': 0, 'batchSize': 10, 'activeCacheRange': [0, 100], 'results': [{'contributorName': '12TH CONGRESSIONAL DISTRICT OF ILLINOIS AFL-CIO', 'contributorArea': 'Labor unions', 'electionYear': 2014, 'recipient': 'QUINN III, PATRICK JOSEPH (PAT) & VALLAS, PAUL GUST', 'amount': 5300.0}, {'contributorName': 'ILLINOIS AFL-CIO', 'contributorArea': 'Labor unions', 'electionYear': 2014, 'recipient': 'QUINN III, PATRICK JOSEPH (PAT) & VALLAS, PAUL GUST', 'amount': 5000.0}, {'contributorName': 'WILL-GRUNDY COUNTIES CENTRAL TRADES & LABOR COUNCIL', 'contributorArea': 'Labor unions', 'electionYear': 2014, 'recipient': 'QUINN III, PATRICK JOSEPH (PAT) & VALLAS, PAUL GUST', 'amount': 500.0}]}
        print(results)
        self.assertEqual(expected_results, results)


    def test_filter_fail_no_result(self):
        # ground truth verified
        opts = {"electionYear": "20132312320"}
        urllink = make_results_url(self.url, self.ringid, self.versionid, "Contribution", "results", opts)

        resp = requests.get(urllink, headers=self.headers)
        results = json.loads(resp.content.decode('utf-8'))
        expected_results = {'totalCount': 0, 'page': 0, 'batchSize': 10, 'activeCacheRange': [0, 100], 'results': []}
        print(results)
        self.assertEqual(expected_results, results)

    def tearDown(self):
        pass



if __name__ == '__main__':
    unittest.main()