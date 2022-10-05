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
import os
import json
import pandas as pd

credsPath = os.environ["c3creds"]


from jumbodb.core import Jumbo

from sqlalchemy import func
jdb = Jumbo(strictMode=True)
db = jdb.scales

from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import sessionmaker

from sqlalchemy.sql.expression import case, extract
from sqlalchemy import create_engine
from sqlalchemy import distinct


jumbo_cred = "postgresql://doadmin:AVNS_ZU5d8ihFeYz7IcW@scalesdb-do-user-7032257-0.b.db.ondigitalocean.com:25060/scalesdb?sslmode=require"

engine = create_engine(jumbo_cred, convert_unicode=True)
js = scoped_session(sessionmaker(autocommit=False,
                                         autoflush=False,
                                         bind=engine))

print("--------------------test_filter_year--------------------")
query = js.query(db.Case.case_id)
query = query.filter(db.Case.year == 2016)
my_results = query.count()
print(my_results)

print("--------------------test_filter_case_pacer_id--------------------")
query = js.query(db.Case.case_id)
query = query.filter(db.Case.case_pacer_id == "222255")
my_results = query.count()
print(my_results)

print("--------------------test_filter_terminating_date--------------------")
query = js.query(db.Case.case_id)
query = query.filter(db.Case.terminating_date == "2018-08-23")
my_results = query.count()
print(my_results)

print("--------------------test_filter_case_type--------------------")
print("did this with postico")

print("--------------------test_max_cost_groupby_jurisdiction--------------------")
query = js.query(func.sum(db.Case.monetary_demand), db.Case.is_multi)
my_results = query.group_by(db.Case.is_multi).distinct().all()
print(my_results)

print("--------------------entity count--------------------")
query = js.query(db.Case.case_id)
print(query.distinct(db.Case.case_id).count())









