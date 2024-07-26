@app.route('/like/<string:product_id>', methods=['POST'])
def like(product_id):
    likes_diz = {}
    liked_products = session.get('liked_products', [])
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