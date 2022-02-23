# Testing

Hello whoever is reading this. So, to run tests, you simply need to run the command
`python -m unittest`
from the same directory level that you would run `flask run`

You should activate the virtual environment and everything before running that command.

Currently, the only tests available are for the `satyrn-templates/basic` satyrn ring.
Before running the tests, make sure to change your `SATYRN_CONFIG` variable to point to that ring



## Known errors

- Caching might be producing some weird errors. This happened to me (Andong) when I ran all tests together; I ended up failing "test_averagecount_contribution_contributor" in "test_analysis_newbasic".
To avoid caching issues, it might be better to run each test file individually, e.g.
``python -m unittest tests/test_analysis_newbasic.py``
- Equality of floats in not well handled right now, so if your solution is different by the slightest margin, it'll consider that to be wrong.