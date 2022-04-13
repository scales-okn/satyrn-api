# Testing

Hello whoever is reading this. So, to run tests, you simply need to run the command
`python -m unittest`
inside the `tests` folder (i.e. wherever this README.md is).

You should activate the virtual environment and everything before running that command.

Currently, the only tests available are for the `satyrn-templates/basic` satyrn ring.
Before running the tests, make sure to change your `SATYRN_CONFIG` variable to point to that ring


Note:
- You can also run individual test files by running commandas such as 
	- `python test_basic_plugins.py` (if you're inside the `tests` folder)
	- `python .\tests\test_basic_plugins.py` (if you wanna run it at the top level directory)
- You can also run specific tests inside each file by running commands such as 
	- `python test_basic_transforms.py TestAnalysis.test_count_contribs_groupby_amount_bucket` (if you're inside the `tests` folder)
	- `python .\tests\test_basic_transforms.py TestAnalysis.test_count_contribs_groupby_amount_bucket`(if you wanna run it at the top level directory)
	- The general format of these commands is `python <TEST_FILE_PATH> <TEST_CLASS_OBJECT_NAME>.<TEST_FUNCTION_NAME>`

## Known errors

- Caching might be producing some weird errors. This happened to me (Andong) when I ran all tests together; I ended up failing "test_averagecount_contribution_contributor" in "test_analysis_newbasic".
To avoid caching issues, it might be better to run each test file individually, e.g.
``python -m unittest tests/test_analysis_newbasic.py``
- Equality of floats in not well handled right now, so if your solution is different by the slightest margin, it'll consider that to be wrong.