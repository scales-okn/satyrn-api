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

DB_MODE = os.environ.get("DB_MODE", "mongo")

if DB_MODE == "sql":
    from .satyrnBundler import app
    with app.app_context():
      from .api.views import api
      app.register_blueprint(api, url_prefix="/api")
elif DB_MODE == "sparql":
    from .sparqlBundler import app
    with app.app_context():
      from .api.sparql_views import api
      app.register_blueprint(api, url_prefix="/api")
elif DB_MODE == "mongo":
    from .mongo.mongoBundler import app
    with app.app_context():
      from .mongo.api.views import api
      app.register_blueprint(api, url_prefix="/api")
else:
    raise Exception("Invalid DB_MODE: {DB_MODE}")