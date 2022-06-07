


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
