import unittest

import os


import requests

from tests.helper_func import make_results_url
import json

class TestMuliColumnFormat(unittest.TestCase):

    def setUp(self):

        self.apikey = os.environ.get("API_KEY", "thisisatestkey")
        self.url = os.environ.get("TEST_URL", "http://127.0.0.1:5000/")
        self.ringid = "40d447dd-ef05-490c-fff8-f6f271a6733f"
        self.versionid = "1"
        self.headers ={
            "x-api-key": self.apikey
        }

    def test_multiColumnResult(self):
        # ground truth verified
        opts = {"state": "Oregon", "county": "Lane"}

        urllink = make_results_url(self.url, self.ringid, self.versionid, "CountyLevel", "results", opts)

        resp = requests.get(urllink, headers=self.headers)
        results = json.loads(resp.content.decode('utf-8'))

        expected_results = {"totalCount": 587, "page": 0, "batchSize": 10, "activeCacheRange": [0, 100], "results": [{"date": "2020-03-17", "county": "Lane, Oregon", "state": "Oregon", "fips": "41039", "cases": "2", "deaths": "1"}, {"date": "2020-03-18", "county": "Lane, Oregon", "state": "Oregon", "fips": "41039", "cases": "2", "deaths": "1"}, {"date": "2020-03-19", "county": "Lane, Oregon", "state": "Oregon", "fips": "41039", "cases": "2", "deaths": "1"}, {"date": "2020-03-20", "county": "Lane, Oregon", "state": "Oregon", "fips": "41039", "cases": "2", "deaths": "1"}, {"date": "2020-03-21", "county": "Lane, Oregon", "state": "Oregon", "fips": "41039", "cases": "3", "deaths": "1"}, {"date": "2020-03-25", "county": "Lane, Oregon", "state": "Oregon", "fips": "41039", "cases": "5", "deaths": "1"}, {"date": "2020-03-24", "county": "Lane, Oregon", "state": "Oregon", "fips": "41039", "cases": "4", "deaths": "1"}, {"date": "2020-03-22", "county": "Lane, Oregon", "state": "Oregon", "fips": "41039", "cases": "4", "deaths": "1"}, {"date": "2020-03-23", "county": "Lane, Oregon", "state": "Oregon", "fips": "41039", "cases": "4", "deaths": "1"}, {"date": "2020-03-27", "county": "Lane, Oregon", "state": "Oregon", "fips": "41039", "cases": "9", "deaths": "1"}]}
        self.assertEqual(expected_results, results)


    def tearDown(self):
        pass


""" 
## Commenting this out because it uses the covid ring. 
if __name__ == '__main__':
    unittest.main()
""" 