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


class TestAnalysis(unittest.TestCase):

    def setUp(self):

        self.apikey = os.environ.get("API_KEY", "thisisatestkey")
        self.url = os.environ.get("TEST_URL", "http://127.0.0.1:5000/")
        self.ringid = "20e114c2-ef05-490c-bdd8-f6f271a6733f"
        self.versionid = "1"
        self.headers ={
            "x-api-key": self.apikey
        }


    def test_count_contribs_groupby_amount_bucket(self):
        # ground truth verified
        search_opts = {}


        analysis_opts = {
            "target": {
                "entity":"Contribution",
                "field": "id"
            },
            "op": "count",
            "groupBy": [{
                "entity": "Contribution",
                "field": "amount",
                "transform":"threshold"
            }],
            "relationships": []
        }

        expected_results = {
            "counts": {
                "Contribution//id": 200
            },
            "fieldNames": [
                {
                    "entity": "Contribution",
                    "field": "amount",
                    "transform": "threshold"
                },
                {
                    "entity": "Contribution",
                    "field": "id",
                    "op": "count"
                }
            ],
            "length": 4,
            "results": [
                [
                    "1000 < x",
                    100
                ],
                [
                    "250 < x <= 500",
                    32
                ],
                [
                    "500 < x <= 1000",
                    42
                ],
                [
                    "x <= 250",
                    26
                ]
            ],
            "units": {
                "results": [
                    "dollar",
                    "Contribution"
                ]
            }
        }

        urllink = make_results_url(self.url, self.ringid, self.versionid, "Contribution", "analysis", search_opts)

        resp = requests.get(urllink, headers=self.headers, json=analysis_opts)
        results = json.loads(resp.content.decode('utf-8'))
        self.assertEqual(expected_results, results)




if __name__ == '__main__':
    unittest.main()