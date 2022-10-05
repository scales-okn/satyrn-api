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


# a convenience only for dev
# run directly: python dev.py
# bootstraps the app and db elements in context for CLI exploration
import IPython

from core.satyrnBundler import app

with app.app_context():
    print("\033[92m=============================================")
    print(" - You're about to be loaded into an app context. The app var contains an instance of the Satyrn app.")
    print("=============================================\033[0m")

    IPython.embed()
