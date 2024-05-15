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
import json
from sqlalchemy import func, String, text
from sqlalchemy.sql.expression import cast
from . import utils


# 7/20/23 - Danny O'Neal: This is a helper class for deduping (__hash__) autocomplete results.
# This was created because the set function was used prior to this change. I am assuming the database return dups.
class AutocompleteRecord:
    def __init__(self, value, label):
        self.value = value
        self.label = label

    def to_dict(self):
        return { "value": self.value, "label": self.label }

    def __hash__(self):
        return hash(self.value)

    def __eq__(self, other):
        return self.value == other.value

def runAutocomplete(db, theType, config, extractor, targetEntity, opts={"query": None}):
    # opts["type"] = config["acType"] if "acType" in config else theType
    # TODO: come back and make this work with multi hop joins
    opts["model"] = config["model"]
    opts["vals"] = config["fields"] if "fields" in config else None
    # not opts["query"] or
    if not opts["vals"]:
        return None
    opts["limit"] = opts["limit"] if "limit" in opts else 1000
    opts["format"] = opts["format"] if "format" in config else ("{} "*len(opts["vals"])).strip()
    with db.Session() as sess:
        return getDedupedBundle(db, extractor, targetEntity, theType, opts, sess)

# a helper for deduping types that have same names
# this helper is not memoized, though the functions that call into should be
  
def prepare_output(sess, column_name, field, opts, default_limit, label_template):
  query = sess.query(field, text(column_name))
  if opts["query"]:
      query = query.filter(cast(field, String).ilike(f'%{opts["query"].lower()}%')).limit(opts["limit"])
  else:
      query = query.limit(default_limit)

  output = [
      ac_rec.to_dict() 
      for ac_rec in set(
          AutocompleteRecord(value=item[0], label=label_template.format(item[1], item[0])) 
          for item in query.all()
      )
  ]
  return output

'''
    7/20/23: Danny O'Neal
    re: The autocomplete options for NoS filters should have the corresponding numbers in them, in case someone wants to search by number
    The dictionary includes a label for the front end to render and a value for the backend to use. Label and value are equal if the filters 
    do not include additional view information.
'''
def getDedupedBundle(db, extractor, targetEntity, theType, opts={"query": None}, sess=None):
    if not sess:
        return None

    # targetModel = opts["model"]
    # put together the query
    field, name, joins = utils._get(extractor, targetEntity, theType, db)
    default_limit = 20
    
    # when time is of the essence, I'm not above some hardcoded ugliness!
    if 'ontology_labels' in name:
        # ontology_labels = ['answer', 'arbitration motion', 'arrest', 'bench trial', 'bilateral', 'brief', 'complaint', 'consent decree order', 'consent decree', 'consent judgment', 'default judgment resolution', 'dismiss with prejudice', 'dismiss without prejudice', 'dismissing motion', 'error', 'findings of fact', 'granting motion for summary judgment', 'granting motion to dismiss', 'habeas corpus ad prosequendum', 'indictment', 'information', 'judgment', 'jury trial', 'minute entry', 'motion for default judgment', 'motion for habeas corpus', 'motion for judgment as a matter of law', 'motion for judgment on the pleadings', 'motion for judgment', 'motion for settlement', 'motion for summary judgment', 'motion for time extension', 'motion to certify class', 'motion to dismiss', 'motion to remand', 'motion to seal', 'motion', 'notice of appeal', 'notice of consent', 'notice of dismissal', 'notice of motion', 'notice of removal', 'notice of settlement', 'notice of voluntary dismissal', 'notice', 'opening - complaint', 'opening - indictment', 'opening - information', 'opening - notice of removal', 'order', 'outcome - bench trial', 'outcome - consent decree', 'outcome - default judgment', 'outcome - jury trial', 'outcome - party resolution', 'outcome - remand', 'outcome - rule 12b', 'outcome - rule 68', 'outcome - settlement', 'outcome - summary judgment', 'outcome - transfer', 'outcome - trial', 'outcome - voluntary dismissal', 'party resolution', 'petition for habeas corpus', 'petition', 'proposed', 'remand resolution', 'response', 'rule 68 resolution', 'settlement agreement', 'stipulation for judgment', 'stipulation for settlement', 'stipulation for voluntary dismissal', 'stipulation of dismissal', 'stipulation', 'summons', 'transfer', 'transferred entry', 'trial', 'unopposed', 'verdict', 'waiver of indictment', 'waiver', 'warrant']
        ontology_labels = ['agreement reached', 'answer', 'arbitration motion', 'arrest', 'bench trial', 'bilateral', 'brief', 'case dismissed', 'case opened in error', 'complaint', 'consent decree', 'consent decree resolution', 'consent judgment', 'consent judgment resolution', 'default judgment resolution', 'dismiss with prejudice', 'dismiss without prejudice', 'dismissing motion', 'error', 'findings of fact', 'granting motion for summary judgment', 'granting motion to dismiss', 'guilty plea', 'ifp - application', 'ifp - deny', 'ifp - grant', 'inbound transfer', 'indictment', 'information', 'judgment', 'jury trial', 'minute entry', 'motion', 'motion for default judgment', 'motion for habeas corpus', 'motion for judgment', 'motion for judgment as a matter of law', 'motion for judgment on the pleadings', 'motion for settlement', 'motion for summary judgment', 'motion for time extension', 'motion for voluntary dismissal', 'motion to certify class', 'motion to dismiss', 'motion to remand', 'motion to seal', 'not guilty plea', 'notice', 'notice of appeal', 'notice of consent', 'notice of dismissal', 'notice of motion', 'notice of removal', 'notice of settlement', 'notice of voluntary dismissal', 'opening - complaint', 'opening - inbound transfer', 'opening - indictment', 'opening - information', 'opening - notice of removal', 'opening - petition', 'order', 'outbound transfer', 'outcome - admin closing', 'outcome - bench trial', 'outcome - case dismissed', 'outcome - consent decree', 'outcome - default judgment', 'outcome - jury trial', 'outcome - other trial', 'outcome - outbound transfer', 'outcome - remand', 'outcome - rule 12b', 'outcome - rule 68', 'outcome - sentence', 'outcome - settlement', 'outcome - summary judgment', 'outcome - voluntary dismissal', 'outcome - voluntary dismissal (settlement)', 'petition', 'petition for habeas corpus', 'plea', 'plea agreement', 'proposed', 'remand resolution', 'response', 'rule 68 resolution', 'sentence', 'settlement agreement', 'settlement reached', 'stipulation', 'stipulation for judgment', 'stipulation for settlement', 'stipulation of dismissal', 'summons', 'transfer', 'transferred entry', 'trial', 'unopposed', 'verdict', 'voluntary dismissal resolution', 'waiver', 'waiver of indictment', 'warrant']
        return [{ 'value': x, 'label': x } for x in ontology_labels]
    elif 'case_type' in name:
        return [{ 'value': x, 'label': x } for x in ('civil', 'criminal')]
    elif 'caseHTML' in name:
        # the front end should no longer be sending this, but just in case; currently causes db to crash
        return []
    elif 'case_NOS' in name:
        output = prepare_output(sess, 'nature_suit.number', field, opts, default_limit, "{} - {}")
    elif 'case_id' in name:
        output = prepare_output(sess, 'case_id', field, opts, default_limit, "{}")

    else:
        subquery = sess.query(field).distinct()
        query = sess.query(subquery.subquery()).distinct()
        if opts["query"]:
            query = query.filter(cast(field, String).ilike(f'%{opts["query"].lower()}%'))
            limit = opts["limit"]
        else:
            limit = default_limit

        query = query.limit(limit)

        output = [
            AutocompleteRecord(value=item[0], label=item[0]).to_dict()
            for item in query.all()
        ]

    return sorted(output, key=lambda x: x["value"])
