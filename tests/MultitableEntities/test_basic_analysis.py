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


    def test_sum_amount(self):
        # ground truth verified
        search_opts = {}


        analysis_opts = {
            "target": {
                "entity": "Contribution",
                "field": "amount"
            },
            "op": "sum",
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
                    "op": "sum"
                }
            ],
            "length": 1,
            "results": [
                [
                    3745502.3
                ]
            ],
            "units": {
                "results": [
                    "dollar"
                ]
            }
        }

        urllink = make_results_url(self.url, self.ringid, self.versionid, "Contribution", "analysis", search_opts)

        resp = requests.get(urllink, headers=self.headers, json=analysis_opts)
        results = json.loads(resp.content.decode('utf-8'))
        self.assertEqual(expected_results, results)

    
    def test_median_amount_groupby_recipient(self):
        # ground truth verified
        search_opts = {}


        analysis_opts = {
            "target": {
                "entity": "Contribution",
                "field": "amount"
            },
            "op": "median",
            "groupBy":[{
                "entity": "Contribution",
                "field": "contributionRecipient"
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
                    "field": "contributionRecipient"
                },
                {
                    "entity": "Contribution",
                    "field": "amount",
                    "op": "median"
                }
            ],
            "length": 11,
            "results": [
                [
                    "BISS, DANIEL K & WALLACE, LITESA E",
                    1500.0
                ],
                [
                    "BOLAND, MIKE J",
                    625.0
                ],
                [
                    "CASTILLO, THOMAS MICHAEL",
                    200.0
                ],
                [
                    "DAIBER, ROBERT A (BOB) & TODD, JONATHAN",
                    350.0
                ],
                [
                    "HENDON, RICKEY R",
                    750.0
                ],
                [
                    "HYNES, DANIEL W",
                    1000.0
                ],
                [
                    "LINK, TERRY",
                    750.0
                ],
                [
                    "PRITZKER, JAY ROBERT (J B) & STRATTON, JULIANA W",
                    1000.0
                ],
                [
                    "QUINN III, PATRICK JOSEPH (PAT) & SIMON, SHEILA J",
                    2500.0
                ],
                [
                    "QUINN III, PATRICK JOSEPH (PAT) & VALLAS, PAUL GUST",
                    2500.0
                ],
                [
                    "TURNER SR, ARTHUR",
                    900.0
                ]
            ],
            "units": {
                "results": [
                    "Recipient",
                    "dollar"
                ]
            }
        }

        urllink = make_results_url(self.url, self.ringid, self.versionid, "Contribution", "analysis", search_opts)

        resp = requests.get(urllink, headers=self.headers, json=analysis_opts)
        results = json.loads(resp.content.decode('utf-8'))
        self.assertEqual(expected_results, results)


    def test_filter_year_min_amount(self):
        # ground truth verified
        search_opts = {"electionYear": "2010"}
        analysis_opts = {
            "target": {
                "entity": "Contribution",
                "field": "amount"
            },
            "op": "min",
            "relationships": []
        }
        expected_results = {
            "counts": {
                "Contribution//id": 129
            },
            "fieldNames": [
                {
                    "entity": "Contribution",
                    "field": "amount",
                    "op": "min"
                }
            ],
            "length": 1,
            "results": [
                [
                    100.0
                ]
            ],
            "units": {
                "results": [
                    "dollar"
                ]
            }
        }
        urllink = make_results_url(self.url, self.ringid, self.versionid, "Contribution", "analysis", search_opts)

        resp = requests.get(urllink, headers=self.headers, json=analysis_opts)
        results = json.loads(resp.content.decode('utf-8'))
        self.assertEqual(expected_results, results)

    def test_filter_year_min_amount_json(self):
        # ground truth verified
        search_opts = {}
        analysis_opts = {
            "target": {
                "entity": "Contribution",
                "field": "amount"
            },
            "query": {
                "AND": [
                    [
                        {"entity": "Contribution",
                        "field": "electionYear"},
                        2010,
                        "exact"
                    ]
                ]
            },
            "op": "min",
            "relationships": []
        }
        expected_results = {
            "counts": {
                "Contribution//id": 129
            },
            "fieldNames": [
                {
                    "entity": "Contribution",
                    "field": "amount",
                    "op": "min"
                }
            ],
            "length": 1,
            "results": [
                [
                    100.0
                ]
            ],
            "units": {
                "results": [
                    "dollar"
                ]
            }
        }
        urllink = make_results_url(self.url, self.ringid, self.versionid, "Contribution", "analysis", search_opts)

        resp = requests.get(urllink, headers=self.headers, json=analysis_opts)
        results = json.loads(resp.content.decode('utf-8'))
        self.assertEqual(expected_results, results)


    def test_max_amount_groupby_instate(self):
        # ground truth verified        
        search_opts = {"electionYear": "2010"}
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
        expected_results = {
            "counts": {
                "Contribution//id": 129
            },
            "fieldNames": [
                {
                    "entity": "Contribution",
                    "field": "inState"
                },
                {
                    "entity": "Contribution",
                    "field": "amount",
                    "op": "max"
                }
            ],
            "length": 2,
            "results": [
                [
                    False,
                    500000.0
                ],
                [
                    True,
                    200000.0
                ]
            ],
            "units": {
                "results": [
                    "In State Contribution Status",
                    "dollar"
                ]
            }
        }
        urllink = make_results_url(self.url, self.ringid, self.versionid, "Contribution", "analysis", search_opts)

        resp = requests.get(urllink, headers=self.headers, json=analysis_opts)
        results = json.loads(resp.content.decode('utf-8'))
        self.assertEqual(expected_results, results)


    def test_count_contribution(self):
        # ground truth verified
        search_opts = {}
        analysis_opts = {
            "target": {
                "entity": "Contribution",
                "field": "id"
            },
            "op": "count",
            "relationships": []
        }
        expected_results = {
            "counts": {
                "Contribution//id": 200
            },
            "fieldNames": [
                {
                    "entity": "Contribution",
                    "field": "id",
                    "op": "count"
                }
            ],
            "length": 1,
            "results": [
                [
                    200
                ]
            ],
            "units": {
                "results": [
                    "Contribution"
                ]
            }
        }
        urllink = make_results_url(self.url, self.ringid, self.versionid, "Contribution", "analysis", search_opts)

        resp = requests.get(urllink, headers=self.headers, json=analysis_opts)
        results = json.loads(resp.content.decode('utf-8'))
        self.assertEqual(expected_results, results)


    def test_count_contributor_groupby_area(self):
        # ground truth verified
        search_opts = {}
        analysis_opts = {
            "target": {
                "entity": "Contributor",
                "field": "id"
            },
            "op": "count",
            "groupBy": [{
                "entity": "Contributor",
                "field": "area"
            }],
            "relationships": ["ContribToContributorInfo"]
        }
        expected_results = {
            "counts": {
                "Contributor//id": 118
            },
            "fieldNames": [
                {
                    "entity": "Contributor",
                    "field": "area"
                },
                {
                    "entity": "Contributor",
                    "field": "id",
                    "op": "count"
                }
            ],
            "length": 17,
            "results": [
                [
                    "Air transport unions",
                    1
                ],
                [
                    "Automotive unions",
                    2
                ],
                [
                    "Communications & hi-tech unions",
                    1
                ],
                [
                    "Construction unions",
                    53
                ],
                [
                    "Electrical workers/IBEW",
                    10
                ],
                [
                    "Entertainment unions",
                    4
                ],
                [
                    "Fire fighters unions and associations",
                    2
                ],
                [
                    "Health worker unions",
                    2
                ],
                [
                    "Labor unions",
                    5
                ],
                [
                    "Manufacturing unions",
                    1
                ],
                [
                    "Other unions",
                    4
                ],
                [
                    "Police unions & associations",
                    3
                ],
                [
                    "Railroad unions",
                    4
                ],
                [
                    "Retail trade unions",
                    3
                ],
                [
                    "State & local government employee unions",
                    3
                ],
                [
                    "Teachers unions",
                    6
                ],
                [
                    "Teamsters unions",
                    14
                ]
            ],
            "units": {
                "results": [
                    "Contributor Sector",
                    "Contributor"
                ]
            }
        }
        urllink = make_results_url(self.url, self.ringid, self.versionid, "Contribution", "analysis", search_opts)

        resp = requests.get(urllink, headers=self.headers, json=analysis_opts)
        results = json.loads(resp.content.decode('utf-8'))
        self.assertEqual(expected_results, results)

    
    def test_filter_year_count_contributor(self):
        # ground truth verified
        search_opts = {"electionYear": "2010"}
        analysis_opts = {
            "target": {
                "entity": "Contributor",
                "field": "id"
            },
            "op": "count",
            "relationships": ["ContribToContributorInfo"]
        }
        expected_results = {
            "counts": {
                "Contributor//id": 84
            },
            "fieldNames": [
                {
                    "entity": "Contributor",
                    "field": "id",
                    "op": "count"
                }
            ],
            "length": 1,
            "results": [
                [
                    84
                ]
            ],
            "units": {
                "results": [
                    "Contributor"
                ]
            }
        }
        urllink = make_results_url(self.url, self.ringid, self.versionid, "Contribution", "analysis", search_opts)

        resp = requests.get(urllink, headers=self.headers, json=analysis_opts)
        results = json.loads(resp.content.decode('utf-8'))
        self.assertEqual(expected_results, results)
    
    def test_filter_year_sum_amount_groupby_contributor(self):
        # ground truth verified
        search_opts = {"electionYear": "2014"}
        analysis_opts = {
            "target": {
                "entity": "Contribution",
                "field": "amount"
            },
            "op": "sum",
            "groupBy": [{
                "entity": "Contributor",
                "field": "id"
            }],
            "relationships": ["ContribToContributorInfo"]
        }
        expected_results = {
            "counts": {
                "Contribution//id": 66,
                "Contributor//id": 47
            },
            "fieldNames": [
                {
                    "entity": "Contributor",
                    "field": "id"
                },
                {
                    "entity": "Contributor",
                    "field": "reference"
                },
                {
                    "entity": "Contribution",
                    "field": "amount",
                    "op": "sum"
                }
            ],
            "length": 47,
            "results": [
                [
                    1,
                    "INTERNATIONAL BROTHERHOOD OF ELECTRICAL WORKERS / IBEW from NATIONAL AFL-CIO",
                    36602.3
                ],
                [
                    6,
                    "INTERNATIONAL ASSOCIATION OF SHEET METAL AIR RAIL & TRANSPORTATION WORKERS / SMART from No value",
                    1000.0
                ],
                [
                    8,
                    "ELECTRICAL WORKERS LOCAL 145 from INTERNATIONAL BROTHERHOOD OF ELECTRICAL WORKERS / IBEW",
                    250.0
                ],
                [
                    9,
                    "IRONWORKERS LOCAL 63 from INTERNATIONAL ASSOCIATION OF BRIDGE STRUCTURAL ORNAMENTAL & REINFORCING IRON WORKERS",
                    5000.0
                ],
                [
                    12,
                    "12TH CONGRESSIONAL DISTRICT OF ILLINOIS AFL-CIO from ILLINOIS AFL-CIO",
                    5300.0
                ],
                [
                    13,
                    "FOOD & COMMERCIAL WORKERS LOCAL 881 from UNITED FOOD & COMMERCIAL WORKERS INTERNATIONAL UNION / UFCW",
                    84200.0
                ],
                [
                    18,
                    "CHICAGO FRATERNAL ORDER OF POLICE LODGE 7 from FRATERNAL ORDER OF POLICE ASSOCIATES / FOP",
                    2500.0
                ],
                [
                    20,
                    "TEAMSTERS LOCAL 705 from INTERNATIONAL BROTHERHOOD OF TEAMSTERS / IBT",
                    25000.0
                ],
                [
                    21,
                    "ILLINOIS AFL-CIO from NATIONAL AFL-CIO",
                    5000.0
                ],
                [
                    23,
                    "STATE UNIVERSITIES ANNUITANTS ASSOCIATION from No value",
                    200.0
                ],
                [
                    24,
                    "UAW REGION 4 from UNITED AUTOMOBILE AEROSPACE & AGRICULTURAL IMPLEMENT WORKERS OF AMERICA / UAW",
                    50500.0
                ],
                [
                    27,
                    "TEAMSTERS LOCAL 777 from INTERNATIONAL BROTHERHOOD OF TEAMSTERS / IBT",
                    500.0
                ],
                [
                    32,
                    "THEATRICAL STAGE EMPLOYEES LOCAL 2 from INTERNATIONAL ALLIANCE OF THEATRICAL STAGE EMPLOYEES / IATSE",
                    5000.0
                ],
                [
                    33,
                    "BROTHERHOOD OF LOCOMOTIVE ENGINEERS & TRAINMEN / BLET from INTERNATIONAL BROTHERHOOD OF TEAMSTERS / IBT",
                    1000.0
                ],
                [
                    36,
                    "CHICAGO & COOK COUNTY BUILDING & CONSTRUCTION TRADES COUNCIL from No value",
                    1000.0
                ],
                [
                    37,
                    "TEAMSTERS JOINT COUNCIL 25 from INTERNATIONAL BROTHERHOOD OF TEAMSTERS / IBT",
                    2500.0
                ],
                [
                    41,
                    "SHEET METAL WORKERS LOCAL 265 from INTERNATIONAL ASSOCIATION OF SHEET METAL AIR RAIL & TRANSPORTATION WORKERS / SMART",
                    1000.0
                ],
                [
                    47,
                    "IRONWORKERS DISTRICT COUNCIL OF CHICAGO & VICINITY from INTERNATIONAL ASSOCIATION OF BRIDGE STRUCTURAL ORNAMENTAL & REINFORCING IRON WORKERS",
                    20000.0
                ],
                [
                    48,
                    "BROTHERHOOD OF RAILROAD SIGNALMEN / BRS from No value",
                    1000.0
                ],
                [
                    49,
                    "UNITED ASSOCIATION OF JOURNEYMEN & APPRENTICES OF THE PLUMBING & PIPE FITTING INDUSTRY OF THE UNITED STATES & CANADA / UA from No value",
                    100000.0
                ],
                [
                    51,
                    "TEAMSTERS LOCAL 50 from INTERNATIONAL BROTHERHOOD OF TEAMSTERS / IBT",
                    3500.0
                ],
                [
                    52,
                    "SEIU HCII from SERVICE EMPLOYEES INTERNATIONAL UNION / SEIU",
                    750000.0
                ],
                [
                    58,
                    "SHEET METAL WORKERS LOCAL 268 from INTERNATIONAL ASSOCIATION OF SHEET METAL AIR RAIL & TRANSPORTATION WORKERS / SMART",
                    500.0
                ],
                [
                    59,
                    "LABORERS INTERNATIONAL UNION OF NORTH AMERICA / LIUNA from LABORERS INTERNATIONAL UNION OF NORTH AMERICA / LIUNA",
                    200000.0
                ],
                [
                    60,
                    "FOOD & COMMERCIAL WORKERS LOCAL 1546 from UNITED FOOD & COMMERCIAL WORKERS INTERNATIONAL UNION / UFCW",
                    2500.0
                ],
                [
                    62,
                    "ILLINOIS PIPE TRADES ASSOCIATION from UNITED ASSOCIATION OF JOURNEYMEN & APPRENTICES OF THE PLUMBING & PIPE FITTING INDUSTRY OF THE UNITED STATES & CANADA / UA",
                    75000.0
                ],
                [
                    63,
                    "PLUMBERS & PIPEFITTERS LOCAL 99 from UNITED ASSOCIATION OF JOURNEYMEN & APPRENTICES OF THE PLUMBING & PIPE FITTING INDUSTRY OF THE UNITED STATES & CANADA / UA",
                    5600.0
                ],
                [
                    64,
                    "CHICAGO & NORTHEASTERN ILLINOIS DISTRICT COUNCIL OF CARPENTERS from UNITED BROTHERHOOD OF CARPENTERS & JOINERS / UBC",
                    5000.0
                ],
                [
                    65,
                    "PEORIA FIRE FIGHTERS LOCAL 50 from INTERNATIONAL ASSOCIATION OF FIRE FIGHTERS / IAFF",
                    2500.0
                ],
                [
                    66,
                    "ILLINOIS EDUCATION ASSOCIATION from NATIONAL EDUCATION ASSOCIATION / NEA",
                    1000.0
                ],
                [
                    69,
                    "UNITED FOOD & COMMERCIAL WORKERS INTERNATIONAL UNION / UFCW from No value",
                    250000.0
                ],
                [
                    72,
                    "THEATRICAL STAGE EMPLOYEES LOCAL 476 from INTERNATIONAL ALLIANCE OF THEATRICAL STAGE EMPLOYEES / IATSE",
                    4000.0
                ],
                [
                    76,
                    "525 POLITICAL CLUB from No value",
                    750.0
                ],
                [
                    78,
                    "DUPAGE COUNTY BUILDING & CONSTRUCTION TRADES COUNCIL from NATIONAL AFL-CIO",
                    1000.0
                ],
                [
                    81,
                    "THEATRICAL STAGE EMPLOYEES LOCAL 750 from INTERNATIONAL ALLIANCE OF THEATRICAL STAGE EMPLOYEES / IATSE",
                    1000.0
                ],
                [
                    82,
                    "ILLINOIS BROTHERHOOD OF LOCOMOTIVE ENGINEERS & TRAINMEN from BROTHERHOOD OF LOCOMOTIVE ENGINEERS & TRAINMEN / BLET",
                    250.0
                ],
                [
                    86,
                    "PLUMBERS & PIPEFITTERS LOCAL 597 from UNITED ASSOCIATION OF JOURNEYMEN & APPRENTICES OF THE PLUMBING & PIPE FITTING INDUSTRY OF THE UNITED STATES & CANADA / UA",
                    20000.0
                ],
                [
                    95,
                    "TEAMSTERS LOCAL 727 from INTERNATIONAL BROTHERHOOD OF TEAMSTERS / IBT",
                    10000.0
                ],
                [
                    99,
                    "COMMUNICATIONS WORKERS DISTRICT 4 from COMMUNICATIONS WORKERS OF AMERICA / CWA",
                    2500.0
                ],
                [
                    100,
                    "UNITED AUTOMOBILE AEROSPACE & AGRICULTURAL IMPLEMENT WORKERS OF AMERICA / UAW from UNITED AUTOMOBILE AEROSPACE & AGRICULTURAL IMPLEMENT WORKERS OF AMERICA / UAW",
                    250000.0
                ],
                [
                    102,
                    "ELECTRICAL WORKERS LOCAL 364 from INTERNATIONAL BROTHERHOOD OF ELECTRICAL WORKERS / IBEW",
                    5000.0
                ],
                [
                    103,
                    "ELECTRICAL WORKERS LOCAL 146 from INTERNATIONAL BROTHERHOOD OF ELECTRICAL WORKERS / IBEW",
                    3750.0
                ],
                [
                    104,
                    "ELECTRICAL WORKERS LOCAL 34 from INTERNATIONAL BROTHERHOOD OF ELECTRICAL WORKERS / IBEW",
                    5000.0
                ],
                [
                    106,
                    "WILL-GRUNDY COUNTIES CENTRAL TRADES & LABOR COUNCIL from No value",
                    500.0
                ],
                [
                    108,
                    "CARPENTERS & JOINERS LOCAL 790 from UNITED BROTHERHOOD OF CARPENTERS & JOINERS / UBC",
                    500.0
                ],
                [
                    109,
                    "INTERNATIONAL ASSOCIATION OF HEAT & FROST INSULATORS & ALLIED WORKERS / HFIAW from No value",
                    10600.0
                ],
                [
                    114,
                    "TEAMSTERS LOCAL 627 from INTERNATIONAL BROTHERHOOD OF TEAMSTERS / IBT",
                    500.0
                ]
            ],
            "units": {
                "results": [
                    "Contributor",
                    "Contributor",
                    "dollar"
                ]
            }
        }
        urllink = make_results_url(self.url, self.ringid, self.versionid, "Contribution", "analysis", search_opts)

        resp = requests.get(urllink, headers=self.headers, json=analysis_opts)
        results = json.loads(resp.content.decode('utf-8'))
        self.assertEqual(expected_results, results)
    
    def test_averagecount_contribution_contributor(self):
        # ground truth verified
        search_opts = {}
        analysis_opts = {
            "target": {
                "entity":"Contribution",
                "field": "id"
            },
            "per": {
                "entity":"Contributor",
                "field": "id"
            },
            "op": "averageCount",
            "relationships": ["ContribToContributorInfo"]
        }
        expected_results = {
            "counts": {
                "Contribution//id": 200,
                "Contributor//id": 118
            },
            "fieldNames": [
                {
                    "entity": "Contribution",
                    "field": "id",
                    "op": "averageCount",
                    "per": {
                        "entity": "Contributor",
                        "field": "id"
                    }
                }
            ],
            "length": 1,
            "results": [
                [
                    1.69
                ]
            ],
            "units": {
                "results": [
                    "Contribution/Contributor"
                ]
            }
        }
        urllink = make_results_url(self.url, self.ringid, self.versionid, "Contribution", "analysis", search_opts)

        resp = requests.get(urllink, headers=self.headers, json=analysis_opts)
        results = json.loads(resp.content.decode('utf-8'))
        self.assertEqual(expected_results, results)
    
    def test_averagecount_contribution_contributor_groupby_area(self):
        # ground truth verified
        search_opts = {}
        analysis_opts = {
            "target": {
                "entity":"Contribution",
                "field": "id"
            },
            "per": {
                "entity":"Contributor",
                "field": "id"
            },
            "op": "averageCount",
            "groupBy": [{
                "entity": "Contributor",
                "field": "area"
            }],
            "relationships": ["ContribToContributorInfo"]
        }
        expected_results = {
            "counts": {
                "Contribution//id": 200,
                "Contributor//id": 118
            },
            "fieldNames": [
                {
                    "entity": "Contributor",
                    "field": "area"
                },
                {
                    "entity": "Contribution",
                    "field": "id",
                    "op": "averageCount",
                    "per": {
                        "entity": "Contributor",
                        "field": "id"
                    }
                }
            ],
            "length": 17,
            "results": [
                [
                    "Air transport unions",
                    1.0
                ],
                [
                    "Automotive unions",
                    4.0
                ],
                [
                    "Communications & hi-tech unions",
                    1.0
                ],
                [
                    "Construction unions",
                    1.55
                ],
                [
                    "Electrical workers/IBEW",
                    2.2
                ],
                [
                    "Entertainment unions",
                    1.75
                ],
                [
                    "Fire fighters unions and associations",
                    2.0
                ],
                [
                    "Health worker unions",
                    3.5
                ],
                [
                    "Labor unions",
                    2.2
                ],
                [
                    "Manufacturing unions",
                    1.0
                ],
                [
                    "Other unions",
                    1.5
                ],
                [
                    "Police unions & associations",
                    2.0
                ],
                [
                    "Railroad unions",
                    1.5
                ],
                [
                    "Retail trade unions",
                    2.33
                ],
                [
                    "State & local government employee unions",
                    1.33
                ],
                [
                    "Teachers unions",
                    1.83
                ],
                [
                    "Teamsters unions",
                    1.14
                ]
            ],
            "units": {
                "results": [
                    "Contributor Sector",
                    "Contribution/Contributor"
                ]
            }
        }
        urllink = make_results_url(self.url, self.ringid, self.versionid, "Contribution", "analysis", search_opts)

        resp = requests.get(urllink, headers=self.headers, json=analysis_opts)
        results = json.loads(resp.content.decode('utf-8'))
        self.assertEqual(expected_results, results)
       
    def test_count_contributor_groupby_parentorg(self):
        # ground truth verified
        search_opts = {}
        analysis_opts = {
            "target": {
                "entity":"Contributor",
                "field": "id"
            },
            "op": "count",
            "groupBy": [{
                "entity": "Contributor",
                "field": "parentOrg"
            }],
            "relationships": ["ContribToContributorInfo"]
        }
        expected_results = {
            "counts": {
                "Contributor//id": 118
            },
            "fieldNames": [
                {
                    "entity": "Contributor",
                    "field": "parentOrg"
                },
                {
                    "entity": "Contributor",
                    "field": "id",
                    "op": "count"
                }
            ],
            "length": 28,
            "results": [
                [
                    "AMERICAN FEDERATION OF TEACHERS / AFT",
                    3
                ],
                [
                    "BROTHERHOOD OF LOCOMOTIVE ENGINEERS & TRAINMEN / BLET",
                    1
                ],
                [
                    "COMMUNICATIONS WORKERS OF AMERICA / CWA",
                    1
                ],
                [
                    "FRATERNAL ORDER OF POLICE ASSOCIATES / FOP",
                    1
                ],
                [
                    "ILLINOIS AFL-CIO",
                    1
                ],
                [
                    "INTERNATIONAL ALLIANCE OF THEATRICAL STAGE EMPLOYEES / IATSE",
                    3
                ],
                [
                    "INTERNATIONAL ASSOCIATION OF BRIDGE STRUCTURAL ORNAMENTAL & REINFORCING IRON WORKERS",
                    5
                ],
                [
                    "INTERNATIONAL ASSOCIATION OF FIRE FIGHTERS / IAFF",
                    2
                ],
                [
                    "INTERNATIONAL ASSOCIATION OF HEAT & FROST INSULATORS & ALLIED WORKERS / HFIAW",
                    1
                ],
                [
                    "INTERNATIONAL ASSOCIATION OF SHEET METAL AIR RAIL & TRANSPORTATION WORKERS / SMART",
                    2
                ],
                [
                    "INTERNATIONAL BROTHERHOOD OF BOILERMAKERS IRON SHIP BUILDERS BLACKSMITHS FORGERS & HELPERS / IBB",
                    2
                ],
                [
                    "INTERNATIONAL BROTHERHOOD OF ELECTRICAL WORKERS / IBEW",
                    9
                ],
                [
                    "INTERNATIONAL BROTHERHOOD OF TEAMSTERS / IBT",
                    13
                ],
                [
                    "INTERNATIONAL UNION OF OPERATING ENGINEERS / IUOE",
                    6
                ],
                [
                    "INTERNATIONAL UNION OF PAINTERS & ALLIED TRADES / IUPAT",
                    2
                ],
                [
                    "LABORERS INTERNATIONAL UNION OF NORTH AMERICA / LIUNA",
                    7
                ],
                [
                    "NATIONAL AFL-CIO",
                    5
                ],
                [
                    "NATIONAL EDUCATION ASSOCIATION / NEA",
                    1
                ],
                [
                    "No value",
                    25
                ],
                [
                    "SERVICE EMPLOYEES INTERNATIONAL UNION / SEIU",
                    3
                ],
                [
                    "SHEET METAL WORKERS INTERNATIONAL ASSOCIATION / SMWIA",
                    1
                ],
                [
                    "UNITE HERE! INTERNATIONAL UNION",
                    1
                ],
                [
                    "UNITED ASSOCIATION OF JOURNEYMEN & APPRENTICES OF THE PLUMBING & PIPE FITTING INDUSTRY OF THE UNITED STATES & CANADA / UA",
                    10
                ],
                [
                    "UNITED AUTOMOBILE AEROSPACE & AGRICULTURAL IMPLEMENT WORKERS OF AMERICA / UAW",
                    2
                ],
                [
                    "UNITED BROTHERHOOD OF CARPENTERS & JOINERS / UBC",
                    7
                ],
                [
                    "UNITED FOOD & COMMERCIAL WORKERS INTERNATIONAL UNION / UFCW",
                    2
                ],
                [
                    "UNITED STEEL PAPER & FORESTRY RUBBER MANUFACTURING ENERGY ALLIED INDUSTRIAL & SERVICE WORKERS INTERNATIONAL / USW",
                    1
                ],
                [
                    "UNITED UNION OF ROOFERS WATERPROOFERS & ALLIED WORKERS",
                    1
                ]
            ],
            "units": {
                "results": [
                    "Contributor Parent Organization",
                    "Contributor"
                ]
            }
        }
        urllink = make_results_url(self.url, self.ringid, self.versionid, "Contribution", "analysis", search_opts)

        resp = requests.get(urllink, headers=self.headers, json=analysis_opts)
        results = json.loads(resp.content.decode('utf-8'))
        self.assertEqual(expected_results, results)
    
    def test_count_contributor_groupby_parentorg(self):
        # ground truth verified
        search_opts = {}
        analysis_opts = {
            "target": {
                "entity":"Contributor",
                "field": "id"
            },
            "op": "count",
            "groupBy": [{
                "entity": "Contributor",
                "field": "parentOrg"
            }],
            "relationships": []
        }
        expected_results = {
            "counts": {
                "Contributor//id": 118
            },
            "fieldNames": [
                {
                    "entity": "Contributor",
                    "field": "parentOrg"
                },
                {
                    "entity": "Contributor",
                    "field": "id",
                    "op": "count"
                }
            ],
            "length": 28,
            "results": [
                [
                    "AMERICAN FEDERATION OF TEACHERS / AFT",
                    3
                ],
                [
                    "BROTHERHOOD OF LOCOMOTIVE ENGINEERS & TRAINMEN / BLET",
                    1
                ],
                [
                    "COMMUNICATIONS WORKERS OF AMERICA / CWA",
                    1
                ],
                [
                    "FRATERNAL ORDER OF POLICE ASSOCIATES / FOP",
                    1
                ],
                [
                    "ILLINOIS AFL-CIO",
                    1
                ],
                [
                    "INTERNATIONAL ALLIANCE OF THEATRICAL STAGE EMPLOYEES / IATSE",
                    3
                ],
                [
                    "INTERNATIONAL ASSOCIATION OF BRIDGE STRUCTURAL ORNAMENTAL & REINFORCING IRON WORKERS",
                    5
                ],
                [
                    "INTERNATIONAL ASSOCIATION OF FIRE FIGHTERS / IAFF",
                    2
                ],
                [
                    "INTERNATIONAL ASSOCIATION OF HEAT & FROST INSULATORS & ALLIED WORKERS / HFIAW",
                    1
                ],
                [
                    "INTERNATIONAL ASSOCIATION OF SHEET METAL AIR RAIL & TRANSPORTATION WORKERS / SMART",
                    2
                ],
                [
                    "INTERNATIONAL BROTHERHOOD OF BOILERMAKERS IRON SHIP BUILDERS BLACKSMITHS FORGERS & HELPERS / IBB",
                    2
                ],
                [
                    "INTERNATIONAL BROTHERHOOD OF ELECTRICAL WORKERS / IBEW",
                    9
                ],
                [
                    "INTERNATIONAL BROTHERHOOD OF TEAMSTERS / IBT",
                    13
                ],
                [
                    "INTERNATIONAL UNION OF OPERATING ENGINEERS / IUOE",
                    6
                ],
                [
                    "INTERNATIONAL UNION OF PAINTERS & ALLIED TRADES / IUPAT",
                    2
                ],
                [
                    "LABORERS INTERNATIONAL UNION OF NORTH AMERICA / LIUNA",
                    7
                ],
                [
                    "NATIONAL AFL-CIO",
                    5
                ],
                [
                    "NATIONAL EDUCATION ASSOCIATION / NEA",
                    1
                ],
                [
                    "No value",
                    25
                ],
                [
                    "SERVICE EMPLOYEES INTERNATIONAL UNION / SEIU",
                    3
                ],
                [
                    "SHEET METAL WORKERS INTERNATIONAL ASSOCIATION / SMWIA",
                    1
                ],
                [
                    "UNITE HERE! INTERNATIONAL UNION",
                    1
                ],
                [
                    "UNITED ASSOCIATION OF JOURNEYMEN & APPRENTICES OF THE PLUMBING & PIPE FITTING INDUSTRY OF THE UNITED STATES & CANADA / UA",
                    10
                ],
                [
                    "UNITED AUTOMOBILE AEROSPACE & AGRICULTURAL IMPLEMENT WORKERS OF AMERICA / UAW",
                    2
                ],
                [
                    "UNITED BROTHERHOOD OF CARPENTERS & JOINERS / UBC",
                    7
                ],
                [
                    "UNITED FOOD & COMMERCIAL WORKERS INTERNATIONAL UNION / UFCW",
                    2
                ],
                [
                    "UNITED STEEL PAPER & FORESTRY RUBBER MANUFACTURING ENERGY ALLIED INDUSTRIAL & SERVICE WORKERS INTERNATIONAL / USW",
                    1
                ],
                [
                    "UNITED UNION OF ROOFERS WATERPROOFERS & ALLIED WORKERS",
                    1
                ]
            ],
            "units": {
                "results": [
                    "Contributor Parent Organization",
                    "Contributor"
                ]
            }
        }
        urllink = make_results_url(self.url, self.ringid, self.versionid, "Contributor", "analysis", search_opts)

        resp = requests.get(urllink, headers=self.headers, json=analysis_opts)
        results = json.loads(resp.content.decode('utf-8'))
        self.assertEqual(expected_results, results)
    
    def test_filter_area_average_amount(self):

        search_opts = {}
        analysis_opts = {
            "target": {
                "entity": "Contribution",
                "field": "amount"
            },
            "op": "average",
            "relationships": ["ContribToContributorInfo"],
            "query": {
                "AND": [
                    [
                        {"entity": "Contributor",
                        "field": "area"},
                        "Other unions",
                        "exact"
                    ]
                ]
            }
        }
        expected_results = {'counts': {'Contribution//id': 6}, 'fieldNames': [{'entity': 'Contribution', 'field': 'amount', 'op': 'average'}], 
                    'length': 1, 'results': [[1500.0]], 'units': {'results': ['dollar']}}
        urllink = make_results_url(self.url, self.ringid, self.versionid, "Contribution", "analysis", search_opts)

        resp = requests.get(urllink, headers=self.headers, json=analysis_opts)
        results = json.loads(resp.content.decode('utf-8'))
        self.assertEqual(expected_results, results)
        # pass
    
    def test_filter_area_sum_amount_time_year(self):
        search_opts = {}
        analysis_opts = {
            "target": {
                "entity": "Contribution",
                "field": "amount"
            },
            "op": "sum",
            "timeSeries": {
                "entity": "Contribution",
                "field": "electionYear"
            },
            "relationships": ["ContribToContributorInfo"],
            "query": {
                "AND": [
                    [
                        {"entity": "Contributor",
                        "field": "area"},
                        "Other unions",
                        "exact"
                    ]
                ]
            }
        }
        expected_results = {'counts': {'Contribution//id': 6}, 'fieldNames': [{'entity': 'Contribution', 'field': 'electionYear'}, {'entity': 'Contribution', 'field': 'amount', 'op': 'sum'}], 
                            'length': 2, 'results': [[2010.0, 8000.0], [2014.0, 1000.0]], 'units': {'results': ['Election Year', 'dollar']}}
        urllink = make_results_url(self.url, self.ringid, self.versionid, "Contribution", "analysis", search_opts)

        resp = requests.get(urllink, headers=self.headers, json=analysis_opts)
        results = json.loads(resp.content.decode('utf-8'))
        self.assertEqual(expected_results, results)
    
    def test_filter_area_average_amount_groupby_instate_time_year(self):
        search_opts = {}
        analysis_opts = {
            "target": {
                "entity": "Contribution",
                "field": "amount"
            },
            "op": "average",
            "timeSeries": {
                "entity": "Contribution",
                "field": "electionYear"
            },
            "groupBy": [{
                "entity": "Contribution",
                "field": "inState"
            }],
            "relationships": ["ContribToContributorInfo"],
            "query": {
                "AND": [
                    [
                        {"entity": "Contributor",
                        "field": "area"},
                        "Labor unions",
                        "contains"
                    ]
                ]
            }
        }
        expected_results = {'counts': {'Contribution//id': 11}, 
                            'fieldNames': [{'entity': 'Contribution', 'field': 'inState'}, {'entity': 'Contribution', 'field': 'electionYear'}, {'entity': 'Contribution', 'field': 'amount', 'op': 'average'}], 
                            'length': 2, 'results': [[True, 2010.0, 7406.25], [True, 2014.0, 3600.0]],
                             'units': {'results': ['In State Contribution Status', 'Election Year', 'dollar']}}
        urllink = make_results_url(self.url, self.ringid, self.versionid, "Contribution", "analysis", search_opts)

        resp = requests.get(urllink, headers=self.headers, json=analysis_opts)
        results = json.loads(resp.content.decode('utf-8'))
        self.assertEqual(expected_results, results)
    

if __name__ == '__main__':
    unittest.main()