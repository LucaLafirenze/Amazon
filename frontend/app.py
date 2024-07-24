from flask import Flask, request, redirect, render_template, url_for, session
import json
from Amazon.frontend.backend.amazon import login_signup, check_user_credentials
from backend import amazon as data
import mysql.connector
import Amazon.frontend.backend.Database_Luca_Definitivo as Luca


app = Flask(__name__)
app.secret_key = 's3cr3t_k3y'
db = Luca.connect_database("localhost", "root", "", "amazon")


@app.route('/products')
def products():
    products_list = []
    rating_list = []
    rating = request.args.get('rating')
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


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/login')
def login():
    return render_template('login.html')


@app.route('/signup')
def signup():
    return render_template('signup.html')


@app.route('/do_login', methods=['POST'])
def do_login():
    username = request.form['username']
    password = request.form['password']
    user = check_user_credentials(db, username, password)
    if user:
        session['utente_id'] = user[0]
        session['username'] = user[1]
        return redirect(url_for('index'))

    return render_template('login.html')


@app.route('/do_signup', methods=['POST'])
def do_signup():
    username = request.form["username"]
    password = request.form["password"]
    login_signup(db, username, password)

    return render_template('login.html')


if __name__ == '__main__':
    app.run(debug=True) 
        