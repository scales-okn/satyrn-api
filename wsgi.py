try:
    from core import app
except:
    from .core import app

if __name__ == "__main__":
    app.run()
