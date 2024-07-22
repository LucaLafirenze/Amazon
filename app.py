from flask import Flask, request, redirect, render_template, url_for
import json
# import amazon as data
# import sys
# import os

# external_module_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'backend'))
# if external_module_path not in sys.path:
#     sys.path.append(external_module_path)

app = Flask(__name__)

# @app.route('/products')
# def products():
#     categories = data.get_categories()
#     return render_template('products.html', categories=categories)

@app.route('/')
def index():
    return render_template('index.html')

# # Carica i dati dei prodotti dal file JSON
# with open('static/products.json') as f:
#     products = json.load(f)

# @app.route('/search', methods=['GET'])
# def search():
#     product_id = request.args.get('product_id')
#     product = next((p for p in products if p['product_id'] == product_id), None)
#     if product:
#         return redirect(product['link'])
#     else:
#         return "Product not found", 404

if __name__ == '__main__':
    app.run(debug=True)        