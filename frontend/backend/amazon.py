import csv
import re

import mysql.connector

import backend.Database_Luca_Amazon as Luca


# input_path = 'C:/Users/Luca/OneDrive/Documenti/Data_Engineer/Francesco/lezione file csv/amazon.csv'
input_path = r'C:\Users\rames\Documents\GitHub\Amazon\frontend\static\amazon.csv'

with open(input_path, encoding="utf-8") as f:
    lettura = csv.reader(f, delimiter=",")
    next(f)
    categorie = []
    category_set = set()
    lista_completa = []
    price_list = []
    product_list = []
    rating_list = []
    i = 0
    for elem in lettura:
        i += 1
        sub_price_list = []
        sub_product_list = []
        sub_rating_list = []
        try:
            elem[3] = elem[3].replace("₹", "")
            elem[3] = float(elem[3].replace(",", ""))
        except ValueError:
            break
        try:
            elem[4] = elem[4].replace("₹", "")
            elem[4] = float(elem[4].replace(",", ""))
        except ValueError:
            break
        try:
            elem[5] = elem[5].replace("%", "")
        except ValueError:
            break
        try:
            float(elem[6])
        except ValueError:
            elem[6] = 0.0
        try:
            elem[7] = int(elem[7].replace(",", ""))
        except ValueError:
            elem[7] = 0

        categorie.append(elem[2].split("|"))
        sub_price_list.append(elem[3])
        sub_price_list.append(elem[4])
        sub_price_list.append(elem[5])
        price_list.append(sub_price_list)

        sub_product_list.append(elem[0])
        sub_product_list.append(elem[1])
        sub_product_list.append(elem[8])
        sub_product_list.append(elem[14])
        sub_product_list.append(elem[15])
        sub_product_list.append(i)
        product_list.append(sub_product_list)

        sub_rating_list.append(elem[6])
        sub_rating_list.append(elem[7])
        sub_rating_list.append(elem[0])
        rating_list.append(sub_rating_list)

        lista_completa.append(elem)


for elem in categorie:
    for i in elem:
        category_set.add(i)


def get_categories(db):
    cursor = db.cursor()
    cursor.execute("SELECT category_names FROM category")
    categories = [row[0] for row in cursor.fetchall()]
    cursor.close()
    return categories


def get_products_name(db):
    cursor = db.cursor()
    cursor.execute("SELECT product_name FROM product")
    product_name = cursor.fetchall()
    cursor.close()
    return product_name


def get_rating(db):
    cursor = db.cursor()
    cursor.execute("SELECT rating FROM rating")
    products = cursor.fetchall()
    cursor.close()
    return products


def get_product_img_link(db):
    cursor = db.cursor()
    cursor.execute("SELECT img_link FROM product")
    product_img_link = cursor.fetchall()
    cursor.close()
    return product_img_link


def get_product_description(db):
    cursor = db.cursor()
    cursor.execute("SELECT description FROM product")
    product_description = cursor.fetchall()
    cursor.close()
    return product_description


def get_products(db):
    products = []
    cursor = db.cursor()
    try:
        cursor.execute("SELECT * FROM product")
        products = cursor.fetchall()
    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        cursor.close()
    return products

def get_products_in_utente_product(db):
    products = []
    cursor = db.cursor()
    try:
        cursor.execute("SELECT * FROM utente_product JOIN product ON utente_product.product_ID = product.product_ID")
        products = cursor.fetchall()
    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        cursor.close()
    return products

def get_price_in_utente_product(db):
    products = []
    cursor = db.cursor()
    try:
        cursor.execute("SELECT * FROM utente_product JOIN product ON utente_product.product_ID = product.product_ID JOIN price ON product.price_ID = price.price_ID")
        products = cursor.fetchall()
    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        cursor.close()
    return products


def validate_password(password):
    if len(password) < 8:
        return False, "Password più lunga pl0x"
    if not re.search(r"[A-Z]", password):
        return False, "Password maiuscola pl0x"
    if not re.search(r"[a-z]", password):
        return False, "Password minuscola pl0x"
    if not re.search(r"[0-9]", password):
        return False, "Password numerosa pl0x"
    if not re.search(r"[@$!%*?_&]", password):
        return False, "Password carattere disabile pl0x"
    return True, "password valida"


def login_signup(db, username, password):
    cursor = db.cursor()
    queryselect = "SELECT username FROM utente_amazon"
    cursor.execute(queryselect)
    values = cursor.fetchall()
    usernames = [value[0] for value in values]

    if username not in usernames:
        is_valid, message = validate_password(password)
        if is_valid:
            query = "INSERT INTO utente_amazon (username, password) VALUES (%s, %s)"
            data = (username, password)
            cursor.execute(query, data)
            db.commit()
            cursor.close()
        else:
            return {"status": "error", "message": message}
    else:
        return {"status": "error", "message": "Username già presente nel database"}


def check_user_credentials(db, username, password):
    cursor = db.cursor()
    query = "SELECT * FROM utente_amazon WHERE username = %s AND password = %s"
    cursor.execute(query, (username, password))
    user = cursor.fetchone()
    cursor.close()
    return user


if __name__ == "__main__":
    Luca.create_database("localhost", "root", "", "amazon")

    db = Luca.connect_database("localhost", "root", "", "amazon")

    c = {
        "category_names": "VARCHAR"
    }
    Luca.crea_tabelle(db, "category", "category_ID", colonne_aggiuntive=c, Auto_I=True)
    c = {
        "discount_price": "FLOAT",
        "actual_price": "FLOAT",
        "discount_percentage": "INT"
    }
    Luca.crea_tabelle(db, "price", "price_ID", colonne_aggiuntive=c, Auto_I=True)
    c = {
        "product_name": "VARCHAR",
        "description": "VARCHAR",
        "img_link": "TEXT",
        "product_link": "TEXT"
    }

    c_fk = {
        "price_ID": ("INT", "price", "price_ID")
    }
    Luca.crea_tabelle(db, "product", "product_ID", colonne_FK=c_fk, colonne_aggiuntive=c, tipo_ID="VARCHAR")

    c = {
        "rating": "FLOAT",
        "rating_count": "INT"

    }
    c_fk = {
        "product_ID": ("VARCHAR", "product", "product_ID")
    }

    Luca.crea_tabelle(db, "rating", "rating_ID", colonne_FK=c_fk, colonne_aggiuntive=c, Auto_I=True)

    c_fk = {
        "product_ID": ("VARCHAR", "product", "product_ID"),
        "category_ID": ("INT", "category", "category_ID")
    }

    Luca.crea_tabelle(db, "category_products", "cat_product_ID", colonne_FK=c_fk, Auto_I=True)
    colonne = {
        "username": "VARCHAR",
        "password": "VARCHAR"
    }
    Luca.crea_tabelle(db, "utente_amazon", "utente_ID", colonne_aggiuntive=colonne, Auto_I=True)

    colonne_fk = {
        "product_ID": ("VARCHAR", "product", "product_ID"),
        "utente_ID": ("INT", "utente_amazon", "utente_ID")

    }
    Luca.crea_tabelle(db, "likes", "likes_ID", colonne_FK=colonne_fk, Auto_I=True)

    colonne_fk = {
        "product_ID": ("VARCHAR", "product", "product_ID"),
        "utente_ID": ("INT", "utente_amazon", "utente_ID")

    }
    Luca.crea_tabelle(db, "utente_product", "utente_product_ID", colonne_FK=colonne_fk, Auto_I=True)

    insert_tuple = tuple(category_set)
    Luca.insert_query(db, "category", "category_names", insert_tuple)
    Luca.insert_query(db, "price", "discount_price, actual_price, discount_percentage", price_list)
    Luca.insert_query(db, "product", "product_ID, product_name, description, img_link, product_link, price_ID", product_list)

    diz_category_DB = dict(Luca.select_query(db, "category", "category_names, category_ID"))
    diz_product_DB = dict(Luca.select_query(db, "product", "product_name, product_ID"))

    Luca.insert_N_N(db, "category_products", "product_ID, category_ID", lista_completa, diz_category_DB, 2, diff_value=True)
    Luca.insert_query(db, "rating", "rating, rating_count, product_ID", rating_list)

