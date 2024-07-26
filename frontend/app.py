from flask import Flask, request, redirect, render_template, url_for, session
from backend.amazon import login_signup, check_user_credentials
from backend import amazon as data
import backend.Database_Luca_Amazon as Luca
import os

app = Flask(__name__)
app.secret_key = os.urandom(24) #Modifica And

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


#Modifica And in piu vedi su products.html righe 10 il link e le righe dal 118-123
@app.route('/like/<string:product_id>', methods=['POST'])
def like(product_id):
    likes_diz = {}
    liked_products = session.get('liked_products', [])
    print(liked_products)
    if product_id in liked_products:
        liked_products.remove(product_id)
        likes_diz[product_id] = session['utente_id']
        Luca.delete_likes(db, "likes", "product_ID, utente_ID", elem_diz=likes_diz)
    else:
        liked_products.append(product_id)
        likes_diz[product_id] = session['utente_id']
        Luca.insert_likes(db, "likes", "product_ID, utente_ID", elem_diz=likes_diz)
    session['liked_products'] = liked_products
    return redirect(url_for('products'))


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


@app.route('/add_to_cart/<string:product_id>', methods=['POST'])
def add_to_cart(product_id):
    print(product_id)
    product_diz = {}
    username = session.get('utente_id')
    print(username)
    """if 'cart' not in session:
        session['cart'] = []

    cart = session['cart']
    
    for item in cart:
        if item['id'] == product_id:
            item['quantity'] += 1
            break
    else:
        cart.append({'id': product_id, 'price': product_price, 'quantity': 1})
    
    session['cart'] = cart"""
    return redirect(url_for('products'))


@app.route('/cart')
def cart():
    cart = session.get('cart', [])
    total = sum(item['price'] * item['quantity'] for item in cart)
    return render_template('cart.html', cart=cart, total=total)


@app.route('/categorie')
def categories():
    categories = data.get_categories(db)
    return render_template('categories.html', categories=categories)


if __name__ == '__main__':
    app.run(debug=True) 
        