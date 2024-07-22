from flask import Flask, request, redirect, render_template, url_for
import json
from config import Config
import mysql.connector

app = Flask(__name__)

def connect_to_database():
    connection = mysql.connector.connect(
        host=Config.DATABASE_HOST,
        user=Config.DATABASE_USER,
        password=Config.DATABASE_PASSWORD,
        database=Config.DATABASE_NAME
    )
    return connection
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