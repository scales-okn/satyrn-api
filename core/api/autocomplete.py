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
from sqlalchemy import func, String, text, Integer, or_
from sqlalchemy.sql.expression import cast
from . import utils


# 7/20/23 - Danny O'Neal: This is a helper class for deduping (__hash__) autocomplete results.
# This was created because the set function was used prior to this change. I am assuming the database return dups.
class AutocompleteRecord:
    def __init__(self, value, label):
        self.value = value
        self.label = label

    def to_dict(self):
        return {"value": self.value, "label": self.label}

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
    opts["format"] = opts["format"] if "format" in config else ("{} " * len(opts["vals"])).strip()
    with db.Session() as sess:
        return getDedupedBundle(db, extractor, targetEntity, theType, opts, sess)


# a helper for deduping types that have same names
# this helper is not memoized, though the functions that call into should be

def prepare_output(sess, column_name, field, opts, default_limit, label_template):
    query = sess.query(field, text(column_name))
    if opts["query"]:
        if column_name == "nature_suit.number":
            query = query.filter(
                or_(
                    cast(field, String).ilike(f'%{opts["query"].lower()}%'),
                    text(f"CAST(nature_suit.number AS VARCHAR) ILIKE '%{opts['query'].lower()}%'")
                    # Add your additional field here
                )
            ).limit(opts["limit"])
        else:
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
        ontology_labels = ['answer', 'arrest', 'brief', 'complaint', 'findings_of_fact', 'ifp - application',
                           'ifp - deny', 'ifp - grant', 'indictment', 'information', 'judgment', 'minute_entry',
                           'motion', 'notice', 'order', 'petition', 'plea', 'plea_agreement', 'removal', 'response',
                           'sentence', 'settlement', 'stipulation', 'summons', 'trial', 'verdict', 'waiver', 'warrant',
                           'attribute_admin_closing', 'attribute_bilateral_unopposed', 'attribute_case_opened_in_error',
                           'attribute_default_judgment', 'attribute_dismiss_with_prejudice',
                           'attribute_dismiss_without_prejudice', 'attribute_dismissal_other', 'attribute_dispositive',
                           'attribute_error', 'attribute_granting_motion_for_summary_judgment',
                           'attribute_granting_motion_to_dismiss', 'attribute_motion_for_arbitration',
                           'attribute_motion_for_default_judgment', 'attribute_motion_for_dismissal_other',
                           'attribute_motion_for_habeas_corpus', 'attribute_motion_for_judgment_as_a_matter_of_law',
                           'attribute_motion_for_judgment_on_the_pleadings', 'attribute_motion_for_judgment_other',
                           'attribute_motion_for_settlement', 'attribute_motion_for_summary_judgment',
                           'attribute_motion_for_time_extension', 'attribute_motion_for_voluntary_dismissal',
                           'attribute_motion_to_certify_class', 'attribute_motion_to_dismiss',
                           'attribute_motion_to_remand', 'attribute_motion_to_seal', 'attribute_notice_of_appeal',
                           'attribute_notice_of_consent', 'attribute_notice_of_dismissal_other',
                           'attribute_notice_of_motion', 'attribute_notice_of_settlement',
                           'attribute_notice_of_voluntary_dismissal', 'attribute_opening',
                           'attribute_petition_for_habeas_corpus', 'attribute_plea_guilty', 'attribute_plea_not_guilty',
                           'attribute_proposed', 'attribute_remand', 'attribute_settlement_consent_decree',
                           'attribute_settlement_rule_68', 'attribute_stipulation_for_judgment',
                           'attribute_stipulation_for_settlement', 'attribute_stipulation_of_dismissal',
                           'attribute_transfer_inbound', 'attribute_transfer_outbound', 'attribute_transfer_unknown',
                           'attribute_transferred_entry', 'attribute_trial_bench', 'attribute_trial_jury',
                           'attribute_trial_other', 'attribute_voluntary_dismissal', 'attribute_waiver_of_indictment']
        return [{'value': x, 'label': x.replace('attribute_', '').replace('_',
                                                                          ' ') + ' (attribute)' if 'attribute' in x else x.replace(
            '_', ' ')} for x in ontology_labels]
    elif 'case_type' in name:
        return [{'value': x, 'label': x} for x in ('civil', 'criminal')]
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
