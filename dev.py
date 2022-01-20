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
