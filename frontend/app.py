from flask import Flask, request, redirect, render_template, url_for, session
import json
from backend import amazon as data
import mysql.connector
import Amazon.frontend.backend.Database_Luca_Definitivo as Luca


# SELECT p.*, r.rating FROM product p JOIN rating r ON p.product_ID = r.product_ID

# import sys
# import os

# external_module_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'backend'))
# if external_module_path not in sys.path:
#     sys.path.append(external_module_path)

app = Flask(__name__)
db = Luca.connect_database("localhost", "root", "", "amazon")


@app.route('/products')
def products():
    products_list = []
    rating_list = []
    rating = request.args.get('rating')
    category = request.args.get('category')
    filtro = []

    for elem in data.get_products(db):
        products_list.append(elem)

    for elem in data.get_rating(db):
        rating_list.append(elem)

    if rating:
        risultato = []
        for t1, t2 in zip(rating_list, products_list):
            nuova_tupla = t1 + t2
            if float(t1[0]) > float(rating):
                risultato.append(nuova_tupla) 
    else:
        risultato = []
        for t1, t2 in zip(rating_list, products_list):
            nuova_tupla = t1 + t2
            risultato.append(nuova_tupla)

    categories = data.get_categories(db)
    return render_template('products.html', categories=categories, rating_list=rating_list, products=risultato, rating=rating, filtro=filtro)


@app.route('/like/<int:product_id>', methods=['POST'])
def like(product_id):
    liked_products = session.get('liked_products', [])
    if product_id in liked_products:
        liked_products.remove(product_id)
    else:
        liked_products.append(product_id)
    session['liked_products'] = liked_products
    return '', 204  # Nessun contenuto da restituire


"""@app.route('/products2')
def products2():
    rating = request.args.get('rating')
    category = request.args.get('category')
    filtro = []

    # Establishing the database connection
    cnx = mysql.connector.connect(user='root', password='',
                                  host='localhost', database='amazon')
    cursor = cnx.cursor()

    # Base query
    query = "SELECT p.*, r.rating FROM product p JOIN rating r ON p.product_ID = r.product_ID"
    conditions = []
    params = []

    # Adding conditions based on query parameters
    if rating:
        conditions.append("r.rating > %s")
        params.append(rating)
        filtro.append(f"Rating: {rating}")

    if category:
        conditions.append("p.category = %s")
        params.append(category)
        filtro.append(f"Category: {category}")

    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    cursor.execute(query, params)
    risultato = cursor.fetchall()

    # Fetching all categories for the filter dropdown
    cursor.execute("SELECT DISTINCT category FROM product")
    categories = cursor.fetchall()

    cursor.close()
    cnx.close()

    return render_template('products.html', categories=categories, products=risultato, rating=rating, filtro=filtro)"""


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

@app.route('/login')
def login():
    return render_template('login.html')


if __name__ == '__main__':
    app.run(debug=True) 
        