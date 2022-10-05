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


def make_results_url(url, ringid, versionid, entity, urltype, filters):
	urllink = f"{url}api/{urltype}/{ringid}/{versionid}/{entity}/"

	if not filters:
		return urllink
	else:
		return add_filters(urllink, filters)


def add_filters(urllink, filters):
	urllink = urllink + "?"
	for key, val in filters.items():
		urllink += f"{key}={val}&"
	urllink = urllink[:-1]

	return urllink	
