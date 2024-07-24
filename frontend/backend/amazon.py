import csv

import mysql.connector

import Database_Luca_Definitivo as Luca

input_path = r"C:\Users\Luca\OneDrive\Documenti\Data_Engineer\Francesco\lezione file csv\amazon.csv"
#input_path = r'C:\Users\rames\Documents\GitHub\Amazon\frontend\static\amazon.csv'

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
    cursor.execute("SELECT DISTINCT category_names FROM category")
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


def login_signup(db, username, password):
    cursor = db.cursor()
    queryselect = "SELECT username, password FROM utente_amazon"
    cursor.execute(queryselect)
    values = cursor.fetchall()
    print(values)

    try:
        query = "INSERT INTO utente_amazon (username, password) VALUES (%s, %s)"
        data = (username, password)
        cursor.execute(query, data)
        db.commit()
    except mysql.connector.Error as e:
        print(e)
    finally:
        cursor.close()



"""def login_signup(db, username, password):
    data = username, password
    Luca.insert_query(db, "utente_amazon", "username, password", data)"""


if __name__ == "__main__":
    Luca.create_database("localhost", "root", "", "amazon")

    db = Luca.connect_database("localhost", "root", "", "amazon")

    c = {
        "category_names": "VARCHAR"
    }
    Luca.crea_tabelle(db, "category", "category_ID", colonne_aggiuntive=c)
    c = {
        "discount_price": "FLOAT",
        "actual_price": "FLOAT",
        "discount_percentage": "INT"
    }
    Luca.crea_tabelle(db, "price", "price_ID", colonne_aggiuntive=c)
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

    Luca.crea_tabelle(db, "rating", "rating_ID", colonne_FK=c_fk, colonne_aggiuntive=c)

    c_fk = {
        "product_ID": ("VARCHAR", "product", "product_ID"),
        "category_ID": ("INT", "category", "category_ID")
    }

    Luca.crea_tabelle(db, "category_products", "cat_product_ID", colonne_FK=c_fk)
    colonne = {
        "username": "VARCHAR",
        "password": "VARCHAR"
    }
    Luca.crea_tabelle(db, "utente_amazon", "utente_ID", colonne_aggiuntive=colonne)
    colonne = {
        "like_boolean": "BOOLEAN"
    }
    Luca.crea_tabelle(db, "likes", "like_ID", colonne_aggiuntive=colonne)

    colonne_fk = {
        "utente_ID": ("INT", "utente_amazon", "utente_ID"),
        "like_ID": ("INT", "likes", "like_ID")

    }
    Luca.crea_tabelle(db, "utente_likes", "utente_likes_ID", colonne_FK=colonne_fk)

    colonne_fk = {
        "utente_ID": ("INT", "utente_amazon", "utente_ID"),
        "product_ID": ("VARCHAR", "product", "product_ID")

    }
    Luca.crea_tabelle(db, "utente_product", "utente_product_ID", colonne_FK=colonne_fk)


    insert_tuple = tuple(category_set)
    Luca.insert_query(db, "category", "category_names", insert_tuple)
    Luca.insert_query(db, "price", "discount_price, actual_price, discount_percentage", price_list)
    Luca.insert_query(db, "product", "product_ID, product_name, description, img_link, product_link, price_ID", product_list)

    diz_category_DB = dict(Luca.select_query(db, "category", "category_names, category_ID"))
    diz_product_DB = dict(Luca.select_query(db, "product", "product_name, product_ID"))

    Luca.insert_N_N(db, "category_products", "product_ID, category_ID", lista_completa, diz_category_DB, 2, diff_value=True)
    Luca.insert_query(db, "rating", "rating, rating_count, product_ID", rating_list)
