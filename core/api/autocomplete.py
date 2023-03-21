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

from sqlalchemy import func, String
from sqlalchemy.sql.expression import cast
from . import utils

def runAutocomplete(db, theType, config, extractor, targetEntity, opts={"query": None}):
    # opts["type"] = config["acType"] if "acType" in config else theType
    # TODO: come back and make this work with multi hop joins
    opts["model"] = config["model"]
    opts["vals"] = config["fields"] if "fields" in config else None
    if not opts["query"] or not opts["vals"]:
        return None
    opts["limit"] = opts["limit"] if "limit" in opts else 1000
    opts["format"] = opts["format"] if "format" in config else ("{} "*len(opts["vals"])).strip()
    sess = db.Session()
    return getDedupedBundle(db, extractor, targetEntity, theType, opts, sess)

# a helper for deduping types that have same names
# this helper is not memoized, though the functions that call into should be
def getDedupedBundle(db, extractor, targetEntity, theType, opts={"query": None}, sess=None):
    if not sess:
        return None

    # targetModel = opts["model"]
    # put together the query
    field, name, joins = utils._get(extractor, targetEntity, theType, db)
    if 'ontology' in name: # when time is of the essence, I'm not above some hardcoded ugliness!
        return ['answer', 'arbitration motion', 'arrest', 'bench trial', 'bilateral', 'brief', 'complaint', 'consent decree order', 'consent decree', 'consent judgment', 'default judgment resolution', 'dismiss with prejudice', 'dismiss without prejudice', 'dismissing motion', 'error', 'findings of fact', 'granting motion for summary judgment', 'granting motion to dismiss', 'habeas corpus ad prosequendum', 'indictment', 'information', 'judgment', 'jury trial', 'minute entry', 'motion for default judgment', 'motion for habeas corpus', 'motion for judgment as a matter of law', 'motion for judgment on the pleadings', 'motion for judgment', 'motion for settlement', 'motion for summary judgment', 'motion for time extension', 'motion to certify class', 'motion to dismiss', 'motion to remand', 'motion to seal', 'motion', 'notice of appeal', 'notice of consent', 'notice of dismissal', 'notice of motion', 'notice of removal', 'notice of settlement', 'notice of voluntary dismissal', 'notice', 'opening - complaint', 'opening - indictment', 'opening - information', 'opening - notice of removal', 'order', 'outcome - bench trial', 'outcome - consent decree', 'outcome - default judgment', 'outcome - jury trial', 'outcome - party resolution', 'outcome - remand', 'outcome - rule 12b', 'outcome - rule 68', 'outcome - settlement', 'outcome - summary judgment', 'outcome - transfer', 'outcome - trial', 'outcome - voluntary dismissal', 'party resolution', 'petition for habeas corpus', 'petition', 'proposed', 'remand resolution', 'response', 'rule 68 resolution', 'settlement agreement', 'stipulation for judgment', 'stipulation for settlement', 'stipulation for voluntary dismissal', 'stipulation of dismissal', 'stipulation', 'summons', 'transfer', 'transferred entry', 'trial', 'unopposed', 'verdict', 'waiver of indictment', 'waiver', 'warrant']

    query = sess.query(field)
    if opts["query"]:
        query = query.filter(cast(field, String).ilike(f'%{opts["query"].lower()}%')).limit(opts["limit"])

    # at one point, a comment suggested that rules would be needed to join the potential multiple values in each tuple in query.all;
    # however, as of commit 9b9b7e, sess.query only ever pulls a single field, so the below line should be fine
    output = list(set([item[0] for item in query.all()]))
    output.sort()
    return output
