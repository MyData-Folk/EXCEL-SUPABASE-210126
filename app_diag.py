from flask import Flask, jsonify
import os
import sys
import traceback

app = Flask(__name__)

@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def diagnostic(path):
    try:
        # Tenter d'importer les librairies une par une pour voir laquelle casse
        report = {"status": "diagnostic", "imports": {}}
        
        try:
            import pandas
            report["imports"]["pandas"] = "ok"
        except Exception as e:
            report["imports"]["pandas"] = str(e)
            
        try:
            import supabase
            report["imports"]["supabase"] = "ok"
        except Exception as e:
            report["imports"]["supabase"] = str(e)

        try:
            from utils import snake_case
            report["imports"]["utils"] = "ok"
        except Exception as e:
            report["imports"]["utils"] = str(e)
            
        return jsonify(report)
    except Exception as e:
        return jsonify({"error": str(e), "traceback": traceback.format_exc()}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
