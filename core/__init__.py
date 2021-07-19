from .satyrnBundler import app

with app.app_context():
    from .api.views import api
    app.register_blueprint(api, url_prefix="/api")
