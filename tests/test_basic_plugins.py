
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


    def test_comparison_sum_amount_instate(self):
        # ground truth verified
        search_opts = {}


        analysis_opts = {
            "target1": {
                "entity":"Contribution",
                "field": "id",
                "op": "count"
            },
            "target2": {
                "entity":"Contribution",
                "field": "amount",
                "op": "sum"
            },
            "op": "comparison",
            "group": {
                "entity": "Contribution",
                "field": "inState"
            },
            "relationships": []
        }

        expected_results = {
            "counts": {
                "Contribution//id": 200
            },
            "fieldNames": [
                {
                    "entity": "Contribution",
                    "field": "inState"
                },
                {
                    "entity": "Contribution",
                    "field": "id",
                    "op": "count"
                },
                {
                    "entity": "Contribution",
                    "field": "amount",
                    "op": "sum"
                }
            ],
            "length": 2,
            "results": [
                [
                    False,
                    30,
                    1443552.3
                ],
                [
                    True,
                    170,
                    2301950.0
                ]
            ],
            "units": [
                    "In State Contribution Status",
                    "Contribution",
                    "dollar"
                ]
        }

        urllink = make_results_url(self.url, self.ringid, self.versionid, "Contribution", "analysis", search_opts)

        resp = requests.get(urllink, headers=self.headers, json=analysis_opts)
        results = json.loads(resp.content.decode('utf-8'))

        self.assertEqual(expected_results, results)


    def test_comparison_filter_year_sum_amount_contributor(self):
        # ground truth verified        
        search_opts = {"electionYear":"2014"}
        analysis_opts = {
            "target1": {
                "entity":"Contribution",
                "field": "id",
                "op": "count"
            },
            "target2": {
                "entity":"Contribution",
                "field": "amount",
                "op": "sum"
            },
            "op": "comparison",
            "group": {
                "entity": "Contributor",
                "field": "id"
            },
            "relationships": ["ContribToContributor"]
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
                    "field": "id",
                    "op": "count"
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
                    7,
                    36602.3
                ],
                [
                    6,
                    "INTERNATIONAL ASSOCIATION OF SHEET METAL AIR RAIL & TRANSPORTATION WORKERS / SMART from No value",
                    1,
                    1000.0
                ],
                [
                    8,
                    "ELECTRICAL WORKERS LOCAL 145 from INTERNATIONAL BROTHERHOOD OF ELECTRICAL WORKERS / IBEW",
                    1,
                    250.0
                ],
                [
                    9,
                    "IRONWORKERS LOCAL 63 from INTERNATIONAL ASSOCIATION OF BRIDGE STRUCTURAL ORNAMENTAL & REINFORCING IRON WORKERS",
                    1,
                    5000.0
                ],
                [
                    12,
                    "12TH CONGRESSIONAL DISTRICT OF ILLINOIS AFL-CIO from ILLINOIS AFL-CIO",
                    1,
                    5300.0
                ],
                [
                    13,
                    "FOOD & COMMERCIAL WORKERS LOCAL 881 from UNITED FOOD & COMMERCIAL WORKERS INTERNATIONAL UNION / UFCW",
                    3,
                    84200.0
                ],
                [
                    18,
                    "CHICAGO FRATERNAL ORDER OF POLICE LODGE 7 from FRATERNAL ORDER OF POLICE ASSOCIATES / FOP",
                    1,
                    2500.0
                ],
                [
                    20,
                    "TEAMSTERS LOCAL 705 from INTERNATIONAL BROTHERHOOD OF TEAMSTERS / IBT",
                    1,
                    25000.0
                ],
                [
                    21,
                    "ILLINOIS AFL-CIO from NATIONAL AFL-CIO",
                    1,
                    5000.0
                ],
                [
                    23,
                    "STATE UNIVERSITIES ANNUITANTS ASSOCIATION from No value",
                    1,
                    200.0
                ],
                [
                    24,
                    "UAW REGION 4 from UNITED AUTOMOBILE AEROSPACE & AGRICULTURAL IMPLEMENT WORKERS OF AMERICA / UAW",
                    3,
                    50500.0
                ],
                [
                    27,
                    "TEAMSTERS LOCAL 777 from INTERNATIONAL BROTHERHOOD OF TEAMSTERS / IBT",
                    1,
                    500.0
                ],
                [
                    32,
                    "THEATRICAL STAGE EMPLOYEES LOCAL 2 from INTERNATIONAL ALLIANCE OF THEATRICAL STAGE EMPLOYEES / IATSE",
                    1,
                    5000.0
                ],
                [
                    33,
                    "BROTHERHOOD OF LOCOMOTIVE ENGINEERS & TRAINMEN / BLET from INTERNATIONAL BROTHERHOOD OF TEAMSTERS / IBT",
                    1,
                    1000.0
                ],
                [
                    36,
                    "CHICAGO & COOK COUNTY BUILDING & CONSTRUCTION TRADES COUNCIL from No value",
                    1,
                    1000.0
                ],
                [
                    37,
                    "TEAMSTERS JOINT COUNCIL 25 from INTERNATIONAL BROTHERHOOD OF TEAMSTERS / IBT",
                    1,
                    2500.0
                ],
                [
                    41,
                    "SHEET METAL WORKERS LOCAL 265 from INTERNATIONAL ASSOCIATION OF SHEET METAL AIR RAIL & TRANSPORTATION WORKERS / SMART",
                    1,
                    1000.0
                ],
                [
                    47,
                    "IRONWORKERS DISTRICT COUNCIL OF CHICAGO & VICINITY from INTERNATIONAL ASSOCIATION OF BRIDGE STRUCTURAL ORNAMENTAL & REINFORCING IRON WORKERS",
                    1,
                    20000.0
                ],
                [
                    48,
                    "BROTHERHOOD OF RAILROAD SIGNALMEN / BRS from No value",
                    1,
                    1000.0
                ],
                [
                    49,
                    "UNITED ASSOCIATION OF JOURNEYMEN & APPRENTICES OF THE PLUMBING & PIPE FITTING INDUSTRY OF THE UNITED STATES & CANADA / UA from No value",
                    1,
                    100000.0
                ],
                [
                    51,
                    "TEAMSTERS LOCAL 50 from INTERNATIONAL BROTHERHOOD OF TEAMSTERS / IBT",
                    2,
                    3500.0
                ],
                [
                    52,
                    "SEIU HCII from SERVICE EMPLOYEES INTERNATIONAL UNION / SEIU",
                    1,
                    750000.0
                ],
                [
                    58,
                    "SHEET METAL WORKERS LOCAL 268 from INTERNATIONAL ASSOCIATION OF SHEET METAL AIR RAIL & TRANSPORTATION WORKERS / SMART",
                    1,
                    500.0
                ],
                [
                    59,
                    "LABORERS INTERNATIONAL UNION OF NORTH AMERICA / LIUNA from LABORERS INTERNATIONAL UNION OF NORTH AMERICA / LIUNA",
                    2,
                    200000.0
                ],
                [
                    60,
                    "FOOD & COMMERCIAL WORKERS LOCAL 1546 from UNITED FOOD & COMMERCIAL WORKERS INTERNATIONAL UNION / UFCW",
                    1,
                    2500.0
                ],
                [
                    62,
                    "ILLINOIS PIPE TRADES ASSOCIATION from UNITED ASSOCIATION OF JOURNEYMEN & APPRENTICES OF THE PLUMBING & PIPE FITTING INDUSTRY OF THE UNITED STATES & CANADA / UA",
                    2,
                    75000.0
                ],
                [
                    63,
                    "PLUMBERS & PIPEFITTERS LOCAL 99 from UNITED ASSOCIATION OF JOURNEYMEN & APPRENTICES OF THE PLUMBING & PIPE FITTING INDUSTRY OF THE UNITED STATES & CANADA / UA",
                    2,
                    5600.0
                ],
                [
                    64,
                    "CHICAGO & NORTHEASTERN ILLINOIS DISTRICT COUNCIL OF CARPENTERS from UNITED BROTHERHOOD OF CARPENTERS & JOINERS / UBC",
                    1,
                    5000.0
                ],
                [
                    65,
                    "PEORIA FIRE FIGHTERS LOCAL 50 from INTERNATIONAL ASSOCIATION OF FIRE FIGHTERS / IAFF",
                    1,
                    2500.0
                ],
                [
                    66,
                    "ILLINOIS EDUCATION ASSOCIATION from NATIONAL EDUCATION ASSOCIATION / NEA",
                    1,
                    1000.0
                ],
                [
                    69,
                    "UNITED FOOD & COMMERCIAL WORKERS INTERNATIONAL UNION / UFCW from No value",
                    1,
                    250000.0
                ],
                [
                    72,
                    "THEATRICAL STAGE EMPLOYEES LOCAL 476 from INTERNATIONAL ALLIANCE OF THEATRICAL STAGE EMPLOYEES / IATSE",
                    3,
                    4000.0
                ],
                [
                    76,
                    "525 POLITICAL CLUB from No value",
                    1,
                    750.0
                ],
                [
                    78,
                    "DUPAGE COUNTY BUILDING & CONSTRUCTION TRADES COUNCIL from NATIONAL AFL-CIO",
                    2,
                    1000.0
                ],
                [
                    81,
                    "THEATRICAL STAGE EMPLOYEES LOCAL 750 from INTERNATIONAL ALLIANCE OF THEATRICAL STAGE EMPLOYEES / IATSE",
                    1,
                    1000.0
                ],
                [
                    82,
                    "ILLINOIS BROTHERHOOD OF LOCOMOTIVE ENGINEERS & TRAINMEN from BROTHERHOOD OF LOCOMOTIVE ENGINEERS & TRAINMEN / BLET",
                    1,
                    250.0
                ],
                [
                    86,
                    "PLUMBERS & PIPEFITTERS LOCAL 597 from UNITED ASSOCIATION OF JOURNEYMEN & APPRENTICES OF THE PLUMBING & PIPE FITTING INDUSTRY OF THE UNITED STATES & CANADA / UA",
                    1,
                    20000.0
                ],
                [
                    95,
                    "TEAMSTERS LOCAL 727 from INTERNATIONAL BROTHERHOOD OF TEAMSTERS / IBT",
                    1,
                    10000.0
                ],
                [
                    99,
                    "COMMUNICATIONS WORKERS DISTRICT 4 from COMMUNICATIONS WORKERS OF AMERICA / CWA",
                    1,
                    2500.0
                ],
                [
                    100,
                    "UNITED AUTOMOBILE AEROSPACE & AGRICULTURAL IMPLEMENT WORKERS OF AMERICA / UAW from UNITED AUTOMOBILE AEROSPACE & AGRICULTURAL IMPLEMENT WORKERS OF AMERICA / UAW",
                    1,
                    250000.0
                ],
                [
                    102,
                    "ELECTRICAL WORKERS LOCAL 364 from INTERNATIONAL BROTHERHOOD OF ELECTRICAL WORKERS / IBEW",
                    1,
                    5000.0
                ],
                [
                    103,
                    "ELECTRICAL WORKERS LOCAL 146 from INTERNATIONAL BROTHERHOOD OF ELECTRICAL WORKERS / IBEW",
                    3,
                    3750.0
                ],
                [
                    104,
                    "ELECTRICAL WORKERS LOCAL 34 from INTERNATIONAL BROTHERHOOD OF ELECTRICAL WORKERS / IBEW",
                    1,
                    5000.0
                ],
                [
                    106,
                    "WILL-GRUNDY COUNTIES CENTRAL TRADES & LABOR COUNCIL from No value",
                    1,
                    500.0
                ],
                [
                    108,
                    "CARPENTERS & JOINERS LOCAL 790 from UNITED BROTHERHOOD OF CARPENTERS & JOINERS / UBC",
                    1,
                    500.0
                ],
                [
                    109,
                    "INTERNATIONAL ASSOCIATION OF HEAT & FROST INSULATORS & ALLIED WORKERS / HFIAW from No value",
                    1,
                    10600.0
                ],
                [
                    114,
                    "TEAMSTERS LOCAL 627 from INTERNATIONAL BROTHERHOOD OF TEAMSTERS / IBT",
                    1,
                    500.0
                ]
            ],
            "units":  [
                    "Contributor",
                    "Contributor",
                    "Contribution",
                    "dollar"
                ]
        }
        urllink = make_results_url(self.url, self.ringid, self.versionid, "Contribution", "analysis", search_opts)

        resp = requests.get(urllink, headers=self.headers, json=analysis_opts)
        results = json.loads(resp.content.decode('utf-8'))
        self.assertEqual(expected_results, results)


    def test_correlation_sum_amount_contributor(self):
        # ground truth verified
        search_opts = {}
        analysis_opts = {
            "target1": {
                "entity":"Contribution",
                "field": "id",
                "op": "count"
            },
            "target2": {
                "entity":"Contribution",
                "field": "amount",
                "op": "sum"
            },
            "op": "correlation",
            "group": {
                "entity": "Contributor",
                "field": "id"
            },
            "relationships": ["ContribToContributor"]
        }
        expected_results = {
            "counts": {
                "Contribution//id": 200,
                "Contributor//id": 118
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
                    "field": "id",
                    "op": "count"
                },
                {
                    "entity": "Contribution",
                    "field": "amount",
                    "op": "sum"
                }
                    ],
            "length": 118,
            "results": [
                [
                    1,
                    "INTERNATIONAL BROTHERHOOD OF ELECTRICAL WORKERS / IBEW from NATIONAL AFL-CIO",
                    10,
                    48352.3
                ],
                [
                    2,
                    "MACHINERY MOVERS RIGGERS & MACHINERY ERECTORS LOCAL 136 from No value",
                    2,
                    3000.0
                ],
                [
                    3,
                    "TEAMSTERS LOCAL 916 from INTERNATIONAL BROTHERHOOD OF TEAMSTERS / IBT",
                    1,
                    5000.0
                ],
                [
                    4,
                    "BOILERMAKERS LOCAL 60 from INTERNATIONAL BROTHERHOOD OF BOILERMAKERS IRON SHIP BUILDERS BLACKSMITHS FORGERS & HELPERS / IBB",
                    1,
                    100.0
                ],
                [
                    5,
                    "MIDWEST REGION LABORERS from LABORERS INTERNATIONAL UNION OF NORTH AMERICA / LIUNA",
                    1,
                    25000.0
                ],
                [
                    6,
                    "INTERNATIONAL ASSOCIATION OF SHEET METAL AIR RAIL & TRANSPORTATION WORKERS / SMART from No value",
                    1,
                    1000.0
                ],
                [
                    7,
                    "ILLINOIS FEDERATION OF TEACHERS from AMERICAN FEDERATION OF TEACHERS / AFT",
                    3,
                    107500.0
                ],
                [
                    8,
                    "ELECTRICAL WORKERS LOCAL 145 from INTERNATIONAL BROTHERHOOD OF ELECTRICAL WORKERS / IBEW",
                    1,
                    250.0
                ],
                [
                    9,
                    "IRONWORKERS LOCAL 63 from INTERNATIONAL ASSOCIATION OF BRIDGE STRUCTURAL ORNAMENTAL & REINFORCING IRON WORKERS",
                    2,
                    6000.0
                ],
                [
                    10,
                    "CHICAGO & CENTRAL STATES UNITE HERE! from UNITE HERE! INTERNATIONAL UNION",
                    1,
                    500.0
                ],
                [
                    11,
                    "CARPENTERS & JOINERS LOCAL 141 from UNITED BROTHERHOOD OF CARPENTERS & JOINERS / UBC",
                    1,
                    1000.0
                ],
                [
                    12,
                    "12TH CONGRESSIONAL DISTRICT OF ILLINOIS AFL-CIO from ILLINOIS AFL-CIO",
                    2,
                    5550.0
                ],
                [
                    13,
                    "FOOD & COMMERCIAL WORKERS LOCAL 881 from UNITED FOOD & COMMERCIAL WORKERS INTERNATIONAL UNION / UFCW",
                    5,
                    85650.0
                ],
                [
                    14,
                    "MOTION PICTURE PROJECTIONISTS OPERATORS & VIDEO TECHNICIANS LOCAL 110 from No value",
                    1,
                    100.0
                ],
                [
                    15,
                    "CHICAGO FIRE FIGHTERS LOCAL 2 from INTERNATIONAL ASSOCIATION OF FIRE FIGHTERS / IAFF",
                    3,
                    12100.0
                ],
                [
                    16,
                    "SPRINKLER FITTERS LOCAL 281 from UNITED ASSOCIATION OF JOURNEYMEN & APPRENTICES OF THE PLUMBING & PIPE FITTING INDUSTRY OF THE UNITED STATES & CANADA / UA",
                    2,
                    1500.0
                ],
                [
                    17,
                    "CHICAGO FEDERATION OF TEACHERS LOCAL 1 from AMERICAN FEDERATION OF TEACHERS / AFT",
                    2,
                    11000.0
                ],
                [
                    18,
                    "CHICAGO FRATERNAL ORDER OF POLICE LODGE 7 from FRATERNAL ORDER OF POLICE ASSOCIATES / FOP",
                    4,
                    4250.0
                ],
                [
                    19,
                    "PLUMBERS & PIPEFITTERS LOCAL 501 from UNITED ASSOCIATION OF JOURNEYMEN & APPRENTICES OF THE PLUMBING & PIPE FITTING INDUSTRY OF THE UNITED STATES & CANADA / UA",
                    1,
                    1000.0
                ],
                [
                    20,
                    "TEAMSTERS LOCAL 705 from INTERNATIONAL BROTHERHOOD OF TEAMSTERS / IBT",
                    2,
                    26000.0
                ],
                [
                    21,
                    "ILLINOIS AFL-CIO from NATIONAL AFL-CIO",
                    4,
                    18000.0
                ],
                [
                    22,
                    "PAINTERS & ALLIED TRADES DISTRICT COUNCIL 14 from INTERNATIONAL UNION OF PAINTERS & ALLIED TRADES / IUPAT",
                    4,
                    13300.0
                ],
                [
                    23,
                    "STATE UNIVERSITIES ANNUITANTS ASSOCIATION from No value",
                    2,
                    450.0
                ],
                [
                    24,
                    "UAW REGION 4 from UNITED AUTOMOBILE AEROSPACE & AGRICULTURAL IMPLEMENT WORKERS OF AMERICA / UAW",
                    7,
                    65250.0
                ],
                [
                    25,
                    "ELECTRICAL WORKERS LOCAL 197 from INTERNATIONAL BROTHERHOOD OF ELECTRICAL WORKERS / IBEW",
                    1,
                    500.0
                ],
                [
                    26,
                    "UNITED TRANSPORTATION UNION / UTU from No value",
                    3,
                    600.0
                ],
                [
                    27,
                    "TEAMSTERS LOCAL 777 from INTERNATIONAL BROTHERHOOD OF TEAMSTERS / IBT",
                    1,
                    500.0
                ],
                [
                    28,
                    "OPERATING ENGINEERS LOCAL 965 from INTERNATIONAL UNION OF OPERATING ENGINEERS / IUOE",
                    1,
                    50000.0
                ],
                [
                    29,
                    "SOUTHERN ILLINOIS DISTRICT COUNCIL OF CARPENTERS from UNITED BROTHERHOOD OF CARPENTERS & JOINERS / UBC",
                    2,
                    2250.0
                ],
                [
                    30,
                    "TEAMSTERS (UNIDENTIFIABLE) from INTERNATIONAL BROTHERHOOD OF TEAMSTERS / IBT",
                    1,
                    50000.0
                ],
                [
                    31,
                    "CHICAGO & GENERAL LABORERS DISTRICT COUNCIL OF CHICAGO & VICINITY from LABORERS INTERNATIONAL UNION OF NORTH AMERICA / LIUNA",
                    3,
                    61500.0
                ],
                [
                    32,
                    "THEATRICAL STAGE EMPLOYEES LOCAL 2 from INTERNATIONAL ALLIANCE OF THEATRICAL STAGE EMPLOYEES / IATSE",
                    1,
                    5000.0
                ],
                [
                    33,
                    "BROTHERHOOD OF LOCOMOTIVE ENGINEERS & TRAINMEN / BLET from INTERNATIONAL BROTHERHOOD OF TEAMSTERS / IBT",
                    1,
                    1000.0
                ],
                [
                    34,
                    "PLUMBERS & STEAMFITTERS LOCAL 137 from UNITED ASSOCIATION OF JOURNEYMEN & APPRENTICES OF THE PLUMBING & PIPE FITTING INDUSTRY OF THE UNITED STATES & CANADA / UA",
                    3,
                    1450.0
                ],
                [
                    35,
                    "OPERATING ENGINEERS LOCAL 148 from INTERNATIONAL UNION OF OPERATING ENGINEERS / IUOE",
                    1,
                    500.0
                ],
                [
                    36,
                    "CHICAGO & COOK COUNTY BUILDING & CONSTRUCTION TRADES COUNCIL from No value",
                    3,
                    4000.0
                ],
                [
                    37,
                    "TEAMSTERS JOINT COUNCIL 25 from INTERNATIONAL BROTHERHOOD OF TEAMSTERS / IBT",
                    1,
                    2500.0
                ],
                [
                    38,
                    "TEAMSTERS LOCAL 179 from INTERNATIONAL BROTHERHOOD OF TEAMSTERS / IBT",
                    1,
                    250.0
                ],
                [
                    39,
                    "CARPENTERS & JOINERS LOCAL 1 from UNITED BROTHERHOOD OF CARPENTERS & JOINERS / UBC",
                    1,
                    1000.0
                ],
                [
                    40,
                    "OPERATING ENGINEERS LOCAL 150 from INTERNATIONAL UNION OF OPERATING ENGINEERS / IUOE",
                    1,
                    50000.0
                ],
                [
                    41,
                    "SHEET METAL WORKERS LOCAL 265 from INTERNATIONAL ASSOCIATION OF SHEET METAL AIR RAIL & TRANSPORTATION WORKERS / SMART",
                    1,
                    1000.0
                ],
                [
                    42,
                    "ELECTRICAL WORKERS LOCAL 15 from INTERNATIONAL BROTHERHOOD OF ELECTRICAL WORKERS / IBEW",
                    1,
                    15000.0
                ],
                [
                    43,
                    "OPERATING ENGINEERS LOCAL 649 from INTERNATIONAL UNION OF OPERATING ENGINEERS / IUOE",
                    2,
                    26000.0
                ],
                [
                    44,
                    "PLUMBERS & PIPEFITTERS LOCAL 422 from UNITED ASSOCIATION OF JOURNEYMEN & APPRENTICES OF THE PLUMBING & PIPE FITTING INDUSTRY OF THE UNITED STATES & CANADA / UA",
                    2,
                    2000.0
                ],
                [
                    45,
                    "SEIU ILLINOIS COUNCIL from SERVICE EMPLOYEES INTERNATIONAL UNION / SEIU",
                    1,
                    100000.0
                ],
                [
                    46,
                    "STEELWORKERS LOCAL 17 from UNITED STEEL PAPER & FORESTRY RUBBER MANUFACTURING ENERGY ALLIED INDUSTRIAL & SERVICE WORKERS INTERNATIONAL / USW",
                    1,
                    500.0
                ],
                [
                    47,
                    "IRONWORKERS DISTRICT COUNCIL OF CHICAGO & VICINITY from INTERNATIONAL ASSOCIATION OF BRIDGE STRUCTURAL ORNAMENTAL & REINFORCING IRON WORKERS",
                    1,
                    20000.0
                ],
                [
                    48,
                    "BROTHERHOOD OF RAILROAD SIGNALMEN / BRS from No value",
                    1,
                    1000.0
                ],
                [
                    49,
                    "UNITED ASSOCIATION OF JOURNEYMEN & APPRENTICES OF THE PLUMBING & PIPE FITTING INDUSTRY OF THE UNITED STATES & CANADA / UA from No value",
                    1,
                    100000.0
                ],
                [
                    50,
                    "MISSOURI PLUMBING INDUSTRY COUNCIL from UNITED ASSOCIATION OF JOURNEYMEN & APPRENTICES OF THE PLUMBING & PIPE FITTING INDUSTRY OF THE UNITED STATES & CANADA / UA",
                    1,
                    5000.0
                ],
                [
                    51,
                    "TEAMSTERS LOCAL 50 from INTERNATIONAL BROTHERHOOD OF TEAMSTERS / IBT",
                    2,
                    3500.0
                ],
                [
                    52,
                    "SEIU HCII from SERVICE EMPLOYEES INTERNATIONAL UNION / SEIU",
                    5,
                    925000.0
                ],
                [
                    53,
                    "ELECTRICAL WORKERS LOCAL 193 from INTERNATIONAL BROTHERHOOD OF ELECTRICAL WORKERS / IBEW",
                    1,
                    1000.0
                ],
                [
                    54,
                    "LAKE COUNTY FEDERATION OF TEACHERS LOCAL 504 from AMERICAN FEDERATION OF TEACHERS / AFT",
                    1,
                    10000.0
                ],
                [
                    55,
                    "SEIU LOCAL 73 from SERVICE EMPLOYEES INTERNATIONAL UNION / SEIU",
                    2,
                    1500.0
                ],
                [
                    56,
                    "ILLINOIS FRATERNAL ORDER OF POLICE LABOR COUNCIL from No value",
                    1,
                    300.0
                ],
                [
                    57,
                    "IRONWORKERS LOCAL 111 from INTERNATIONAL ASSOCIATION OF BRIDGE STRUCTURAL ORNAMENTAL & REINFORCING IRON WORKERS",
                    1,
                    250.0
                ],
                [
                    58,
                    "SHEET METAL WORKERS LOCAL 268 from INTERNATIONAL ASSOCIATION OF SHEET METAL AIR RAIL & TRANSPORTATION WORKERS / SMART",
                    1,
                    500.0
                ],
                [
                    59,
                    "LABORERS INTERNATIONAL UNION OF NORTH AMERICA / LIUNA from LABORERS INTERNATIONAL UNION OF NORTH AMERICA / LIUNA",
                    2,
                    200000.0
                ],
                [
                    60,
                    "FOOD & COMMERCIAL WORKERS LOCAL 1546 from UNITED FOOD & COMMERCIAL WORKERS INTERNATIONAL UNION / UFCW",
                    1,
                    2500.0
                ],
                [
                    61,
                    "ELECTRICAL WORKERS LOCAL 134 from INTERNATIONAL BROTHERHOOD OF ELECTRICAL WORKERS / IBEW",
                    1,
                    5000.0
                ],
                [
                    62,
                    "ILLINOIS PIPE TRADES ASSOCIATION from UNITED ASSOCIATION OF JOURNEYMEN & APPRENTICES OF THE PLUMBING & PIPE FITTING INDUSTRY OF THE UNITED STATES & CANADA / UA",
                    2,
                    75000.0
                ],
                [
                    63,
                    "PLUMBERS & PIPEFITTERS LOCAL 99 from UNITED ASSOCIATION OF JOURNEYMEN & APPRENTICES OF THE PLUMBING & PIPE FITTING INDUSTRY OF THE UNITED STATES & CANADA / UA",
                    3,
                    6600.0
                ],
                [
                    64,
                    "CHICAGO & NORTHEASTERN ILLINOIS DISTRICT COUNCIL OF CARPENTERS from UNITED BROTHERHOOD OF CARPENTERS & JOINERS / UBC",
                    4,
                    41750.0
                ],
                [
                    65,
                    "PEORIA FIRE FIGHTERS LOCAL 50 from INTERNATIONAL ASSOCIATION OF FIRE FIGHTERS / IAFF",
                    1,
                    2500.0
                ],
                [
                    66,
                    "ILLINOIS EDUCATION ASSOCIATION from NATIONAL EDUCATION ASSOCIATION / NEA",
                    2,
                    201000.0
                ],
                [
                    67,
                    "INTERNATIONAL UNION OF PAINTERS & ALLIED TRADES / IUPAT from No value",
                    2,
                    11000.0
                ],
                [
                    68,
                    "SHEET METAL WORKERS LOCAL 265 from SHEET METAL WORKERS INTERNATIONAL ASSOCIATION / SMWIA",
                    2,
                    1500.0
                ],
                [
                    69,
                    "UNITED FOOD & COMMERCIAL WORKERS INTERNATIONAL UNION / UFCW from No value",
                    1,
                    250000.0
                ],
                [
                    70,
                    "IRONWORKERS LOCAL 392 from INTERNATIONAL ASSOCIATION OF BRIDGE STRUCTURAL ORNAMENTAL & REINFORCING IRON WORKERS",
                    1,
                    200.0
                ],
                [
                    71,
                    "INTERNATIONAL BROTHERHOOD OF TEAMSTERS / IBT from No value",
                    1,
                    2500.0
                ],
                [
                    72,
                    "THEATRICAL STAGE EMPLOYEES LOCAL 476 from INTERNATIONAL ALLIANCE OF THEATRICAL STAGE EMPLOYEES / IATSE",
                    4,
                    5000.0
                ],
                [
                    73,
                    "LABORERS LOCAL 397 from LABORERS INTERNATIONAL UNION OF NORTH AMERICA / LIUNA",
                    1,
                    500.0
                ],
                [
                    74,
                    "LABORERS LOCAL 309 from LABORERS INTERNATIONAL UNION OF NORTH AMERICA / LIUNA",
                    2,
                    1500.0
                ],
                [
                    75,
                    "PAINTERS & ALLIED TRADES LOCAL 157 from INTERNATIONAL UNION OF PAINTERS & ALLIED TRADES / IUPAT",
                    1,
                    200.0
                ],
                [
                    76,
                    "525 POLITICAL CLUB from No value",
                    1,
                    750.0
                ],
                [
                    77,
                    "CHICAGO FEDERATION OF LABOR & INDUSTRIAL UNION COUCIL from No value",
                    2,
                    40000.0
                ],
                [
                    78,
                    "DUPAGE COUNTY BUILDING & CONSTRUCTION TRADES COUNCIL from NATIONAL AFL-CIO",
                    2,
                    1000.0
                ],
                [
                    79,
                    "SHEET METAL WORKERS LOCAL 73 from No value",
                    3,
                    7000.0
                ],
                [
                    80,
                    "MID-CENTRAL ILLINOIS DISTRICT COUNCIL OF CARPENTERS from UNITED BROTHERHOOD OF CARPENTERS & JOINERS / UBC",
                    1,
                    50000.0
                ],
                [
                    81,
                    "THEATRICAL STAGE EMPLOYEES LOCAL 750 from INTERNATIONAL ALLIANCE OF THEATRICAL STAGE EMPLOYEES / IATSE",
                    1,
                    1000.0
                ],
                [
                    82,
                    "ILLINOIS BROTHERHOOD OF LOCOMOTIVE ENGINEERS & TRAINMEN from BROTHERHOOD OF LOCOMOTIVE ENGINEERS & TRAINMEN / BLET",
                    1,
                    250.0
                ],
                [
                    83,
                    "OPERATING ENGINEERS LOCAL 841 from INTERNATIONAL UNION OF OPERATING ENGINEERS / IUOE",
                    2,
                    15000.0
                ],
                [
                    84,
                    "LAKE COUNTY BUILDING & CONSTRUCTION TRADES COUNCIL from NATIONAL AFL-CIO",
                    1,
                    1300.0
                ],
                [
                    85,
                    "ELECTRICAL WORKERS LOCAL 51 from INTERNATIONAL BROTHERHOOD OF ELECTRICAL WORKERS / IBEW",
                    2,
                    750.0
                ],
                [
                    86,
                    "PLUMBERS & PIPEFITTERS LOCAL 597 from UNITED ASSOCIATION OF JOURNEYMEN & APPRENTICES OF THE PLUMBING & PIPE FITTING INDUSTRY OF THE UNITED STATES & CANADA / UA",
                    1,
                    20000.0
                ],
                [
                    87,
                    "NORTHWESTERN ILLINOIS BUILDING & CONSTRUCTION TRADES COUNCIL from NATIONAL AFL-CIO",
                    1,
                    1000.0
                ],
                [
                    88,
                    "ILLINOIS AFSCME COUNCIL 31 from No value",
                    2,
                    57500.0
                ],
                [
                    89,
                    "TEAMSTERS LOCAL 731 from INTERNATIONAL BROTHERHOOD OF TEAMSTERS / IBT",
                    1,
                    5000.0
                ],
                [
                    90,
                    "OPERATING ENGINEERS LOCAL 520 from INTERNATIONAL UNION OF OPERATING ENGINEERS / IUOE",
                    2,
                    1000.0
                ],
                [
                    91,
                    "LABORERS LOCAL 32 from LABORERS INTERNATIONAL UNION OF NORTH AMERICA / LIUNA",
                    1,
                    1000.0
                ],
                [
                    92,
                    "SERVICE EMPLOYEES INTERNATIONAL UNION / SEIU from No value",
                    1,
                    500000.0
                ],
                [
                    93,
                    "ILLINOIS TROOPERS LODGE 41 from No value",
                    1,
                    1000.0
                ],
                [
                    94,
                    "SOUTHERN & CENTRAL ILLINOIS LABORERS DISTRICT COUNCIL from LABORERS INTERNATIONAL UNION OF NORTH AMERICA / LIUNA",
                    2,
                    15000.0
                ],
                [
                    95,
                    "TEAMSTERS LOCAL 727 from INTERNATIONAL BROTHERHOOD OF TEAMSTERS / IBT",
                    1,
                    10000.0
                ],
                [
                    96,
                    "AIR LINE PILOTS ASSOCIATION / ALPA from No value",
                    1,
                    2500.0
                ],
                [
                    97,
                    "BOILERMAKERS LOCAL 363 from INTERNATIONAL BROTHERHOOD OF BOILERMAKERS IRON SHIP BUILDERS BLACKSMITHS FORGERS & HELPERS / IBB",
                    1,
                    250.0
                ],
                [
                    98,
                    "WEST SUBURBAN TEACHERS LOCAL 571 from No value",
                    1,
                    4000.0
                ],
                [
                    99,
                    "COMMUNICATIONS WORKERS DISTRICT 4 from COMMUNICATIONS WORKERS OF AMERICA / CWA",
                    1,
                    2500.0
                ],
                [
                    100,
                    "UNITED AUTOMOBILE AEROSPACE & AGRICULTURAL IMPLEMENT WORKERS OF AMERICA / UAW from UNITED AUTOMOBILE AEROSPACE & AGRICULTURAL IMPLEMENT WORKERS OF AMERICA / UAW",
                    1,
                    250000.0
                ],
                [
                    101,
                    "S T I P E N D LOCAL 710 from INTERNATIONAL BROTHERHOOD OF TEAMSTERS / IBT",
                    1,
                    500.0
                ],
                [
                    102,
                    "ELECTRICAL WORKERS LOCAL 364 from INTERNATIONAL BROTHERHOOD OF ELECTRICAL WORKERS / IBEW",
                    1,
                    5000.0
                ],
                [
                    103,
                    "ELECTRICAL WORKERS LOCAL 146 from INTERNATIONAL BROTHERHOOD OF ELECTRICAL WORKERS / IBEW",
                    3,
                    3750.0
                ],
                [
                    104,
                    "ELECTRICAL WORKERS LOCAL 34 from INTERNATIONAL BROTHERHOOD OF ELECTRICAL WORKERS / IBEW",
                    1,
                    5000.0
                ],
                [
                    105,
                    "PLUMBERS & GASFITTERS LOCAL 360 from UNITED ASSOCIATION OF JOURNEYMEN & APPRENTICES OF THE PLUMBING & PIPE FITTING INDUSTRY OF THE UNITED STATES & CANADA / UA",
                    1,
                    200.0
                ],
                [
                    106,
                    "WILL-GRUNDY COUNTIES CENTRAL TRADES & LABOR COUNCIL from No value",
                    1,
                    500.0
                ],
                [
                    107,
                    "PLUMBERS & PIPEFITTERS LOCAL 533 from UNITED ASSOCIATION OF JOURNEYMEN & APPRENTICES OF THE PLUMBING & PIPE FITTING INDUSTRY OF THE UNITED STATES & CANADA / UA",
                    1,
                    500.0
                ],
                [
                    108,
                    "CARPENTERS & JOINERS LOCAL 790 from UNITED BROTHERHOOD OF CARPENTERS & JOINERS / UBC",
                    1,
                    500.0
                ],
                [
                    109,
                    "INTERNATIONAL ASSOCIATION OF HEAT & FROST INSULATORS & ALLIED WORKERS / HFIAW from No value",
                    1,
                    10600.0
                ],
                [
                    110,
                    "CARPENTERS & JOINERS LOCAL 272 from UNITED BROTHERHOOD OF CARPENTERS & JOINERS / UBC",
                    1,
                    250.0
                ],
                [
                    111,
                    "ILLINOIS PREVAILING WAGE COUNCIL from No value",
                    2,
                    6000.0
                ],
                [
                    112,
                    "IRONWORKERS LOCAL 498 from INTERNATIONAL ASSOCIATION OF BRIDGE STRUCTURAL ORNAMENTAL & REINFORCING IRON WORKERS",
                    1,
                    500.0
                ],
                [
                    113,
                    "LIQUOR & WINE SALES REPS & ALLIED WORKERS LOCAL 3 from No value",
                    1,
                    500.0
                ],
                [
                    114,
                    "TEAMSTERS LOCAL 627 from INTERNATIONAL BROTHERHOOD OF TEAMSTERS / IBT",
                    1,
                    500.0
                ],
                [
                    115,
                    "HEAT & FROST INSULATORS LOCAL 17 from INTERNATIONAL ASSOCIATION OF HEAT & FROST INSULATORS & ALLIED WORKERS / HFIAW",
                    1,
                    5000.0
                ],
                [
                    116,
                    "PLUMBERS & PIPEFITTERS (UNIDENTIFIABLE) from No value",
                    1,
                    1000.0
                ],
                [
                    117,
                    "ROOFERS & WATERPROOFERS LOCAL 11 from UNITED UNION OF ROOFERS WATERPROOFERS & ALLIED WORKERS",
                    1,
                    200.0
                ],
                [
                    118,
                    "TEAMSTERS LOCAL 700 from INTERNATIONAL BROTHERHOOD OF TEAMSTERS / IBT",
                    1,
                    25000.0
                ]
            ],
            "score": 0.2288323268911608,
            "units": {
                "results": [
                    "Contributor",
                    "Contributor",
                    "Contribution",
                    "dollar"
                ],
                "score": "no units"
            }
        }
        urllink = make_results_url(self.url, self.ringid, self.versionid, "Contribution", "analysis", search_opts)

        resp = requests.get(urllink, headers=self.headers, json=analysis_opts)
        results = json.loads(resp.content.decode('utf-8'))
        self.assertEqual(expected_results, results)

    def test_distribution_sum_instate_timeseries(self):
        # ground truth verified
        search_opts = {}
        analysis_opts = {
            "target": {
                "entity":"Contribution",
                "field": "amount",
                "op": "sum"
            },
            "op": "distribution",
            "over": {
                "entity": "Contribution",
                "field": "inState"
            },
            "timeSeries": {
                "entity": "Contribution",
                "field": "electionYear"
            },
            "relationships": []
        }
        expected_results = {'counts': {'Contribution//id': 200}, 
        'fieldNames': [{'entity': 'Contribution', 'field': 'inState'}, {'entity': 'Contribution', 'field': 'electionYear'}, {'entity': 'Contribution', 'field': 'amount', 'op': 'sum'}], 
        'length': 5, 
        'results': [[False, 2010.0, 0.33150984682713347], [False, 2014.0, 0.43538488568535255], [True, 2010.0, 0.6684901531728665], [True, 2014.0, 0.5646151143146475], [True, 2018.0, 1.0]],
         'units': {'results': ['In State Contribution Status', 'Election Year', 'percentage of dollar']}}

        urllink = make_results_url(self.url, self.ringid, self.versionid, "Contribution", "analysis", search_opts)

        resp = requests.get(urllink, headers=self.headers, json=analysis_opts)
        results = json.loads(resp.content.decode('utf-8'))

        self.assertEqual(expected_results, results)

    def test_filter_year_distribution_sum_contributor_timeseries(self):
        # ground truth verified
        search_opts = {"electionYear": "2014"}
        analysis_opts = {
            "target": {
                "entity":"Contribution",
                "field": "amount",
                "op": "sum"
            },
            "op": "distribution",
            "over": {
                "entity": "Contributor",
                "field": "id"
            },
            "groupBy": [{
                "entity": "Contribution",
                "field": "electionYear"
            }],
            "relationships": ["ContribToContributor"]
        }
        expected_results = {
            "counts": {
                "Contribution//id": 66,
                "Contributor//id": 47
            },
            "fieldNames": [
                {
                    "entity": "Contribution",
                    "field": "electionYear"
                },
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
                    2014.0,
                    1,
                    "INTERNATIONAL BROTHERHOOD OF ELECTRICAL WORKERS / IBEW from NATIONAL AFL-CIO",
                    0.01868892367397271
                ],
                [
                    2014.0,
                    6,
                    "INTERNATIONAL ASSOCIATION OF SHEET METAL AIR RAIL & TRANSPORTATION WORKERS / SMART from No value",
                    0.0005105942433664745
                ],
                [
                    2014.0,
                    8,
                    "ELECTRICAL WORKERS LOCAL 145 from INTERNATIONAL BROTHERHOOD OF ELECTRICAL WORKERS / IBEW",
                    0.00012764856084161862
                ],
                [
                    2014.0,
                    9,
                    "IRONWORKERS LOCAL 63 from INTERNATIONAL ASSOCIATION OF BRIDGE STRUCTURAL ORNAMENTAL & REINFORCING IRON WORKERS",
                    0.002552971216832372
                ],
                [
                    2014.0,
                    12,
                    "12TH CONGRESSIONAL DISTRICT OF ILLINOIS AFL-CIO from ILLINOIS AFL-CIO",
                    0.002706149489842315
                ],
                [
                    2014.0,
                    13,
                    "FOOD & COMMERCIAL WORKERS LOCAL 881 from UNITED FOOD & COMMERCIAL WORKERS INTERNATIONAL UNION / UFCW",
                    0.04299203529145715
                ],
                [
                    2014.0,
                    18,
                    "CHICAGO FRATERNAL ORDER OF POLICE LODGE 7 from FRATERNAL ORDER OF POLICE ASSOCIATES / FOP",
                    0.001276485608416186
                ],
                [
                    2014.0,
                    20,
                    "TEAMSTERS LOCAL 705 from INTERNATIONAL BROTHERHOOD OF TEAMSTERS / IBT",
                    0.012764856084161861
                ],
                [
                    2014.0,
                    21,
                    "ILLINOIS AFL-CIO from NATIONAL AFL-CIO",
                    0.002552971216832372
                ],
                [
                    2014.0,
                    23,
                    "STATE UNIVERSITIES ANNUITANTS ASSOCIATION from No value",
                    0.00010211884867329489
                ],
                [
                    2014.0,
                    24,
                    "UAW REGION 4 from UNITED AUTOMOBILE AEROSPACE & AGRICULTURAL IMPLEMENT WORKERS OF AMERICA / UAW",
                    0.02578500929000696
                ],
                [
                    2014.0,
                    27,
                    "TEAMSTERS LOCAL 777 from INTERNATIONAL BROTHERHOOD OF TEAMSTERS / IBT",
                    0.00025529712168323725
                ],
                [
                    2014.0,
                    32,
                    "THEATRICAL STAGE EMPLOYEES LOCAL 2 from INTERNATIONAL ALLIANCE OF THEATRICAL STAGE EMPLOYEES / IATSE",
                    0.002552971216832372
                ],
                [
                    2014.0,
                    33,
                    "BROTHERHOOD OF LOCOMOTIVE ENGINEERS & TRAINMEN / BLET from INTERNATIONAL BROTHERHOOD OF TEAMSTERS / IBT",
                    0.0005105942433664745
                ],
                [
                    2014.0,
                    36,
                    "CHICAGO & COOK COUNTY BUILDING & CONSTRUCTION TRADES COUNCIL from No value",
                    0.0005105942433664745
                ],
                [
                    2014.0,
                    37,
                    "TEAMSTERS JOINT COUNCIL 25 from INTERNATIONAL BROTHERHOOD OF TEAMSTERS / IBT",
                    0.001276485608416186
                ],
                [
                    2014.0,
                    41,
                    "SHEET METAL WORKERS LOCAL 265 from INTERNATIONAL ASSOCIATION OF SHEET METAL AIR RAIL & TRANSPORTATION WORKERS / SMART",
                    0.0005105942433664745
                ],
                [
                    2014.0,
                    47,
                    "IRONWORKERS DISTRICT COUNCIL OF CHICAGO & VICINITY from INTERNATIONAL ASSOCIATION OF BRIDGE STRUCTURAL ORNAMENTAL & REINFORCING IRON WORKERS",
                    0.010211884867329489
                ],
                [
                    2014.0,
                    48,
                    "BROTHERHOOD OF RAILROAD SIGNALMEN / BRS from No value",
                    0.0005105942433664745
                ],
                [
                    2014.0,
                    49,
                    "UNITED ASSOCIATION OF JOURNEYMEN & APPRENTICES OF THE PLUMBING & PIPE FITTING INDUSTRY OF THE UNITED STATES & CANADA / UA from No value",
                    0.051059424336647445
                ],
                [
                    2014.0,
                    51,
                    "TEAMSTERS LOCAL 50 from INTERNATIONAL BROTHERHOOD OF TEAMSTERS / IBT",
                    0.0017870798517826607
                ],
                [
                    2014.0,
                    52,
                    "SEIU HCII from SERVICE EMPLOYEES INTERNATIONAL UNION / SEIU",
                    0.38294568252485583
                ],
                [
                    2014.0,
                    58,
                    "SHEET METAL WORKERS LOCAL 268 from INTERNATIONAL ASSOCIATION OF SHEET METAL AIR RAIL & TRANSPORTATION WORKERS / SMART",
                    0.00025529712168323725
                ],
                [
                    2014.0,
                    59,
                    "LABORERS INTERNATIONAL UNION OF NORTH AMERICA / LIUNA from LABORERS INTERNATIONAL UNION OF NORTH AMERICA / LIUNA",
                    0.10211884867329489
                ],
                [
                    2014.0,
                    60,
                    "FOOD & COMMERCIAL WORKERS LOCAL 1546 from UNITED FOOD & COMMERCIAL WORKERS INTERNATIONAL UNION / UFCW",
                    0.001276485608416186
                ],
                [
                    2014.0,
                    62,
                    "ILLINOIS PIPE TRADES ASSOCIATION from UNITED ASSOCIATION OF JOURNEYMEN & APPRENTICES OF THE PLUMBING & PIPE FITTING INDUSTRY OF THE UNITED STATES & CANADA / UA",
                    0.03829456825248558
                ],
                [
                    2014.0,
                    63,
                    "PLUMBERS & PIPEFITTERS LOCAL 99 from UNITED ASSOCIATION OF JOURNEYMEN & APPRENTICES OF THE PLUMBING & PIPE FITTING INDUSTRY OF THE UNITED STATES & CANADA / UA",
                    0.002859327762852257
                ],
                [
                    2014.0,
                    64,
                    "CHICAGO & NORTHEASTERN ILLINOIS DISTRICT COUNCIL OF CARPENTERS from UNITED BROTHERHOOD OF CARPENTERS & JOINERS / UBC",
                    0.002552971216832372
                ],
                [
                    2014.0,
                    65,
                    "PEORIA FIRE FIGHTERS LOCAL 50 from INTERNATIONAL ASSOCIATION OF FIRE FIGHTERS / IAFF",
                    0.001276485608416186
                ],
                [
                    2014.0,
                    66,
                    "ILLINOIS EDUCATION ASSOCIATION from NATIONAL EDUCATION ASSOCIATION / NEA",
                    0.0005105942433664745
                ],
                [
                    2014.0,
                    69,
                    "UNITED FOOD & COMMERCIAL WORKERS INTERNATIONAL UNION / UFCW from No value",
                    0.1276485608416186
                ],
                [
                    2014.0,
                    72,
                    "THEATRICAL STAGE EMPLOYEES LOCAL 476 from INTERNATIONAL ALLIANCE OF THEATRICAL STAGE EMPLOYEES / IATSE",
                    0.002042376973465898
                ],
                [
                    2014.0,
                    76,
                    "525 POLITICAL CLUB from No value",
                    0.00038294568252485585
                ],
                [
                    2014.0,
                    78,
                    "DUPAGE COUNTY BUILDING & CONSTRUCTION TRADES COUNCIL from NATIONAL AFL-CIO",
                    0.0005105942433664745
                ],
                [
                    2014.0,
                    81,
                    "THEATRICAL STAGE EMPLOYEES LOCAL 750 from INTERNATIONAL ALLIANCE OF THEATRICAL STAGE EMPLOYEES / IATSE",
                    0.0005105942433664745
                ],
                [
                    2014.0,
                    82,
                    "ILLINOIS BROTHERHOOD OF LOCOMOTIVE ENGINEERS & TRAINMEN from BROTHERHOOD OF LOCOMOTIVE ENGINEERS & TRAINMEN / BLET",
                    0.00012764856084161862
                ],
                [
                    2014.0,
                    86,
                    "PLUMBERS & PIPEFITTERS LOCAL 597 from UNITED ASSOCIATION OF JOURNEYMEN & APPRENTICES OF THE PLUMBING & PIPE FITTING INDUSTRY OF THE UNITED STATES & CANADA / UA",
                    0.010211884867329489
                ],
                [
                    2014.0,
                    95,
                    "TEAMSTERS LOCAL 727 from INTERNATIONAL BROTHERHOOD OF TEAMSTERS / IBT",
                    0.005105942433664744
                ],
                [
                    2014.0,
                    99,
                    "COMMUNICATIONS WORKERS DISTRICT 4 from COMMUNICATIONS WORKERS OF AMERICA / CWA",
                    0.001276485608416186
                ],
                [
                    2014.0,
                    100,
                    "UNITED AUTOMOBILE AEROSPACE & AGRICULTURAL IMPLEMENT WORKERS OF AMERICA / UAW from UNITED AUTOMOBILE AEROSPACE & AGRICULTURAL IMPLEMENT WORKERS OF AMERICA / UAW",
                    0.1276485608416186
                ],
                [
                    2014.0,
                    102,
                    "ELECTRICAL WORKERS LOCAL 364 from INTERNATIONAL BROTHERHOOD OF ELECTRICAL WORKERS / IBEW",
                    0.002552971216832372
                ],
                [
                    2014.0,
                    103,
                    "ELECTRICAL WORKERS LOCAL 146 from INTERNATIONAL BROTHERHOOD OF ELECTRICAL WORKERS / IBEW",
                    0.0019147284126242792
                ],
                [
                    2014.0,
                    104,
                    "ELECTRICAL WORKERS LOCAL 34 from INTERNATIONAL BROTHERHOOD OF ELECTRICAL WORKERS / IBEW",
                    0.002552971216832372
                ],
                [
                    2014.0,
                    106,
                    "WILL-GRUNDY COUNTIES CENTRAL TRADES & LABOR COUNCIL from No value",
                    0.00025529712168323725
                ],
                [
                    2014.0,
                    108,
                    "CARPENTERS & JOINERS LOCAL 790 from UNITED BROTHERHOOD OF CARPENTERS & JOINERS / UBC",
                    0.00025529712168323725
                ],
                [
                    2014.0,
                    109,
                    "INTERNATIONAL ASSOCIATION OF HEAT & FROST INSULATORS & ALLIED WORKERS / HFIAW from No value",
                    0.00541229897968463
                ],
                [
                    2014.0,
                    114,
                    "TEAMSTERS LOCAL 627 from INTERNATIONAL BROTHERHOOD OF TEAMSTERS / IBT",
                    0.00025529712168323725
                ]
            ],
            "units": {
                "results": [
                    "Election Year",
                    "Contributor",
                    "Contributor",
                    "percentage of dollar"
                ]
            }
        }
        urllink = make_results_url(self.url, self.ringid, self.versionid, "Contribution", "analysis", search_opts)

        resp = requests.get(urllink, headers=self.headers, json=analysis_opts)
        results = json.loads(resp.content.decode('utf-8'))
        self.assertEqual(expected_results, results)

    def test_comparison_empty(self):
        # ground truth verified
        search_opts = {"electionYear": "273823728"}
        analysis_opts = {
            "target1": {
                "entity":"Contribution",
                "field": "id",
                "op": "count"
            },
            "target2": {
                "entity":"Contribution",
                "field": "amount",
                "op": "sum"
            },
            "op": "comparison",
            "group": {
                "entity": "Contribution",
                "field": "inState"
            },
            "relationships": []
        }
        expected_results = {
            "counts": {
                "Contribution//id": 0
            },
            "fieldNames": [
                {
                    "entity": "Contribution",
                    "field": "inState"
                },
                {
                    "entity": "Contribution",
                    "field": "id",
                    "op": "count"
                },
                {
                    "entity": "Contribution",
                    "field": "amount",
                    "op": "sum"
                }
            ],
            "length": 0,
            "results": [],
            "units":  [
                    "In State Contribution Status",
                    "Contribution",
                    "dollar"
                ]
        }
        urllink = make_results_url(self.url, self.ringid, self.versionid, "Contribution", "analysis", search_opts)

        resp = requests.get(urllink, headers=self.headers, json=analysis_opts)
        results = json.loads(resp.content.decode('utf-8'))
        self.assertEqual(expected_results, results)

    def test_correlation_empty(self):
        # ground truth verified
        search_opts = {"electionYear": "1234"}
        analysis_opts = {
            "target1": {
                "entity":"Contribution",
                "field": "id",
                "op": "count"
            },
            "target2": {
                "entity":"Contribution",
                "field": "amount",
                "op": "sum"
            },
            "op": "correlation",
            "group": {
                "entity": "Contributor",
                "field": "id"
            },
            "relationships": ["ContribToContributor"]
        }
        expected_results = {
            "counts": {
                "Contribution//id": 0,
                "Contributor//id": 0
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
                    "field": "id",
                    "op": "count"
                },
                {
                    "entity": "Contribution",
                    "field": "amount",
                    "op": "sum"
                }
            ],
            "length": 0,
            "results": [],
            "score": 0,
            "units": {
                "results": [
                    "Contributor",
                    "Contributor",
                    "Contribution",
                    "dollar"
                ],
                "score": "no units"
            }
        }
        urllink = make_results_url(self.url, self.ringid, self.versionid, "Contribution", "analysis", search_opts)

        resp = requests.get(urllink, headers=self.headers, json=analysis_opts)
        results = json.loads(resp.content.decode('utf-8'))
        self.assertEqual(expected_results, results)

    def test_distribution_empty(self):
        # ground truth verified
        search_opts = {"electionYear": "1234"}
        analysis_opts = {
            "target": {
                "entity":"Contribution",
                "field": "amount",
                "op": "sum"
            },
            "op": "distribution",
            "over": {
                "entity": "Contribution",
                "field": "inState"
            },
            "timeSeries": {
                "entity": "Contribution",
                "field": "electionYear"
            },
            "relationships": []
        }
        expected_results = {
            "counts": {
                "Contribution//id": 0
            },
            "fieldNames": [
                {'entity': 'Contribution', 'field': 'inState'}, {'entity': 'Contribution', 'field': 'electionYear'}, {'entity': 'Contribution', 'field': 'amount', 'op': 'sum'}
            ],
            "length": 0,
            "results": [],
            "units": {
                "results": ['In State Contribution Status', 'Election Year', 'percentage of dollar']
            }
        }
        urllink = make_results_url(self.url, self.ringid, self.versionid, "Contribution", "analysis", search_opts)

        resp = requests.get(urllink, headers=self.headers, json=analysis_opts)
        results = json.loads(resp.content.decode('utf-8'))
        self.assertEqual(expected_results, results)

if __name__ == '__main__':
    unittest.main()