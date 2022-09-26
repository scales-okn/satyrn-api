
import unittest

import os


import requests

from helper_func import make_results_url
import json


class TestAnalysis(unittest.TestCase):

    def setUp(self):

        self.apikey = os.environ.get("API_KEY", "thisisatestkey")
        self.url = os.environ.get("TEST_URL", "http://127.0.0.1:5000/")
        self.ringid = "90e114c2-ef05-490c-bdd8-f6f271a6733f"
        self.versionid = "1"
        self.headers ={
            "x-api-key": self.apikey
        }


    def test_sum_billablePages(self):
        # ground truth verified
        search_opts = {}


        analysis_opts = {
            "target": {
                "entity": "Case",
                "field": "billable_pages"
            },
            "op": "sum",
            "relationships": []
        }

        expected_results = {
            "counts": {
                "Case//id": 1
            },
            "fieldNames": [
                {
                    "entity": "Case",
                    "field": "billable_pages",
                    "op": "sum"
                }
            ],
            "length": 1,
            "results": [
                [
                    "3979092.00"
                ]
            ],
            "units": {
                "results": [
                    "Billable Page"
                ]
            }
        }

        urllink = make_results_url(self.url, self.ringid, self.versionid, "Case", "analysis", search_opts)

        resp = requests.get(urllink, headers=self.headers, json=analysis_opts)
        results = json.loads(resp.content.decode('utf-8'))
        self.assertEqual(expected_results, results)


    def test_median_billablePages_groupby_jurisdiction(self):
        # ground truth verified
        search_opts = {}


        analysis_opts =  {
            "target": {
                "entity": "Case",
                "field": "billable_pages"
            },
            "op": "median",
            "groupBy":[{
                "entity": "Case",
                "field": "jurisdiction"
            }],
            "relationships": []
        }

        expected_results = {
            "counts": {
                "Case//id": 1
            },
            "fieldNames": [
                {
                    "entity": "Case",
                    "field": "jurisdiction"
                },
                {
                    "entity": "Case",
                    "field": "billable_pages",
                    "op": "median"
                }
            ],
            "length": 33,
            "results": [
                [
                    " 18:111(a) Impeding a Federal Officer",
                    "1.00"
                ],
                [
                    " 18:922(g)(1) and 924(a)(2) Felon in Possession of a Firearm and Ammunition",
                    "1.00"
                ],
                [
                    "18 U.S.C.",
                    "5.00"
                ],
                [
                    " 18 USC 922(g)(1)",
                    "2.00"
                ],
                [
                    " 18 USC Section 1955 Illegal Gambling Business",
                    "3.00"
                ],
                [
                    "21:841(a)(1)",
                    "3.00"
                ],
                [
                    "21:841 Possession with Intent to Distribute 25.4 Kilograms of Marijuana.",
                    "3.00"
                ],
                [
                    " 21:846",
                    "1.00"
                ],
                [
                    " 21 USC",
                    "3.00"
                ],
                [
                    "21 USC",
                    "1.00"
                ],
                [
                    " 26:5861(d) Possession of a Firearm not Registered in the National Firearms Registration and Transfe",
                    "1.00"
                ],
                [
                    "26 USC 5861(d) Possession of an Unregistered Firearm",
                    "2.00"
                ],
                [
                    "8:1324(a)(1)(A)(v)(I)",
                    "4.00"
                ],
                [
                    " 8:1325(a)(2) and 18:2 Aid and Abet Two Undocumented Minor Alien to Attempt to Elude Examination or",
                    "1.00"
                ],
                [
                    " Bring or Attempt to Bring into the United States an undocumented alien who had not received prior a",
                    "1.00"
                ],
                [
                    " Conspiracy to Possess with Intent to Distribute a Quantity of Actual Methamphetamine",
                    "1.00"
                ],
                [
                    " Conspiracy to Transport Undocumented Aliens",
                    "1.00"
                ],
                [
                    " Count 1",
                    "3.00"
                ],
                [
                    "Count 1",
                    "3.00"
                ],
                [
                    "Count 1s",
                    "3.00"
                ],
                [
                    " Ct 1",
                    "1.00"
                ],
                [
                    "Ct 1",
                    "1.00"
                ],
                [
                    "Ct. 1s",
                    "2.00"
                ],
                [
                    "Custody of the Bureau of Prisons for a term of 97 months as to Count 3. Supervised Release for a ter",
                    "1.00"
                ],
                [
                    "Defendant sentenced to the BOP for 60 months",
                    "3.00"
                ],
                [
                    " Distribution of Crack Cocaine",
                    "1.00"
                ],
                [
                    "Diversity",
                    "3.00"
                ],
                [
                    "Federal Question",
                    "3.00"
                ],
                [
                    "Local Question",
                    "1.00"
                ],
                [
                    "MARIJUANA",
                    "3.00"
                ],
                [
                    "No value",
                    "4.00"
                ],
                [
                    "U.S. Government Defendant",
                    "2.00"
                ],
                [
                    "U.S. Government Plaintiff",
                    "2.00"
                ]
            ],
            "units": {
                "results": [
                    "Jurisdiction",
                    "Billable Page"
                ]
            }
        }

        urllink = make_results_url(self.url, self.ringid, self.versionid, "Case", "analysis", search_opts)

        resp = requests.get(urllink, headers=self.headers, json=analysis_opts)
        results = json.loads(resp.content.decode('utf-8'))
        self.assertEqual(expected_results, results)


    def test_filter_year_min_billablePages(self):
        # ground truth verified
        search_opts = {"year": "2016"}
        analysis_opts = {
            "target": {
                "entity": "Case",
                "field": "billable_pages"
            },
            "op": "min",
            "relationships": []
        }
        expected_results = {
            "counts": {
                "Case//id": 1
            },
            "fieldNames": [
                {
                    "entity": "Case",
                    "field": "billable_pages",
                    "op": "min"
                }
            ],
            "length": 1,
            "results": [
                [
                    "1.00"
                ]
            ],
            "units": {
                "results": [
                    "Billable Page"
                ]
            }
        }
        urllink = make_results_url(self.url, self.ringid, self.versionid, "Case", "analysis", search_opts)

        resp = requests.get(urllink, headers=self.headers, json=analysis_opts)
        results = json.loads(resp.content.decode('utf-8'))
        self.assertEqual(expected_results, results)
    
    def test_filter_year_max_billablePages_CaseType(self):
        # ground truth verified
        search_opts = {}
        analysis_opts = {
            "target": {
                "entity": "Case",
                "field": "billable_pages"
            },
            "query": {
                "AND": [
                    [
                        {"entity": "CaseType",
                        "field": "name"},
                        "cv",
                        "exact"
                    ]
                ]
            },
            "op": "max",
            "relationships": []
        }
        expected_results = {
            "counts": {
                "Case//id": 1
            },
            "fieldNames": [
                {
                    "entity": "Case",
                    "field": "billable_pages",
                    "op": "max"
                }
            ],
            "length": 1,
            "results": [
                [
                    "30.00"
                ]
            ],
            "units": {
                "results": [
                    "Billable Page"
                ]
            }
        }
        urllink = make_results_url(self.url, self.ringid, self.versionid, "Case", "analysis", search_opts)

        resp = requests.get(urllink, headers=self.headers, json=analysis_opts)
        results = json.loads(resp.content.decode('utf-8'))
        self.assertEqual(expected_results, results)
    

    def test_max_cost_groupby_jurisdiction(self):
        # ground truth verified        
        search_opts = {"year": "2016"}
        analysis_opts = {
            "target": {
                "entity": "Contribution",
                "field": "amount"
            },
            "op": "max",
            "groupBy": [{
                "entity": "Contribution",
                "field": "inState"
            }],
            "relationships": []
        }
        expected_results = {}
            ## FIX 
        urllink = make_results_url(self.url, self.ringid, self.versionid, "Case", "analysis", search_opts)

        resp = requests.get(urllink, headers=self.headers, json=analysis_opts)
        results = json.loads(resp.content.decode('utf-8'))
        self.assertEqual(expected_results, results)


    def test_count_case(self):
        # ground truth verified
        search_opts = {}
        analysis_opts = {
            "target": {
                "entity": "Case",
                "field": "id"
            },
            "op": "count",
            "relationships": []
        }
        expected_results = {
            "counts": {
                "Case//id": 1
            },
            "fieldNames": [
                {
                    "entity": "Case",
                    "field": "id",
                    "op": "count"
                }
            ],
            "length": 1,
            "results": [
                [
                    0
                ]
            ],
            "units": {
                "results": [
                    "Case"
                ]
            }
        }
        urllink = make_results_url(self.url, self.ringid, self.versionid, "Case", "analysis", search_opts)

        resp = requests.get(urllink, headers=self.headers, json=analysis_opts)
        results = json.loads(resp.content.decode('utf-8'))
        self.assertEqual(expected_results, results)


if __name__ == '__main__':
    unittest.main()