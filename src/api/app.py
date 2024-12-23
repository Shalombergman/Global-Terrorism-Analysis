from flask import Flask
from src.api.routes.analysis_route import analysis_bp

app = Flask(__name__)
app.register_blueprint(analysis_bp)

if __name__ == '__main__':
    app.run(debug=True) 