import csv
from tkinter import messagebox
import mysql.connector


def create_database(host, user, password, db_name):
    try:
        # Connessione al server MySQL
        db = mysql.connector.connect(
        host=host,
        user=user,
        password=password
        )

        # Droppo il database se esiste
        cursor = db.cursor()
        cursor.execute(f"DROP DATABASE IF EXISTS {db_name}")
        db.commit()
        cursor.close()

        # Creo il database se non esiste
        cursor = db.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        db.commit()
        cursor.close()
        db.close()
    
    except mysql.connector.Error:
        messagebox.showerror("errore creazione DataBase", "Il sistema non è riuscito a creare\n"
                                                          "o a connettersi al database")
        
def connect_database(host, user, password, db_name):
    db = mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=db_name
    )
    return db

def crea_tabelle(db, tabella_name, colonna_ID, colonne_aggiuntive=None, tipo_ID=None, Auto_I=None):
    cursor = db.cursor()

    if tipo_ID == "VARCHAR":
        query = f"""
            CREATE TABLE IF NOT EXISTS {tabella_name} (
            {colonna_ID} {tipo_ID}(255) PRIMARY KEY
            """
    else:
        tipo_ID = "INT"
        query = f"""
            CREATE TABLE IF NOT EXISTS {tabella_name} (
            {colonna_ID} {tipo_ID} PRIMARY KEY
            """

    if Auto_I is not None:
        tipo_ID = "INT"
        query = f"""
            CREATE TABLE IF NOT EXISTS {tabella_name} (
            {colonna_ID} {tipo_ID} AUTO_INCREMENT PRIMARY KEY
            """


    if colonne_aggiuntive:
        for colonna, tipo in colonne_aggiuntive.items():
            if tipo == 'INT':
                query += f", {colonna} INT NOT NULL"
            elif tipo == 'FLOAT':
                query += f", {colonna} FLOAT NOT NULL"
            elif tipo == 'VARCHAR':
                query += f", {colonna} VARCHAR(255) NOT NULL"
            elif tipo == 'DATE':
                query += f", {colonna} DATE NOT NULL"
            elif tipo == 'TINYTEXT':
                query += f", {colonna} TINYTEXT NOT NULL"
            elif tipo == 'TEXT':
                query += f", {colonna} TEXT NOT NULL"
            elif tipo == 'CHAR':
                query += f", {colonna} CHAR(1) NOT NULL"

    query += ")"

    cursor.execute(query)
    db.commit()
    cursor.close()

def truncate(db, tabella_name):
    cursor = db.cursor()
    query = f""" TRUNCATE {tabella_name} """
    cursor.execute(query)
    db.commit()
    cursor.close()

def crea_tabelle_FK(db, tabella_name, colonna_ID, colonne_FK, colonne_aggiuntive=None, tipo_ID=None, Auto_I=None):
    cursor = db.cursor()

    if tipo_ID == "VARCHAR":
        query = f"""
            CREATE TABLE IF NOT EXISTS {tabella_name} (
            {colonna_ID} {tipo_ID}(255) PRIMARY KEY
            """
    else:
        tipo_ID = "INT"
        query = f"""
            CREATE TABLE IF NOT EXISTS {tabella_name} (
            {colonna_ID} {tipo_ID} PRIMARY KEY
            """
        
    if Auto_I is not None:
        tipo_ID = "INT"
        query = f"""
            CREATE TABLE IF NOT EXISTS {tabella_name} (
            {colonna_ID} {tipo_ID} AUTO_INCREMENT PRIMARY KEY
            """

    if colonne_aggiuntive:
        for colonna, tipo in colonne_aggiuntive.items():
            if tipo == 'INT':
                query += f", {colonna} INT NOT NULL"
            elif tipo == 'FLOAT':
                query += f", {colonna} FLOAT NOT NULL"
            elif tipo == 'VARCHAR':
                query += f", {colonna} VARCHAR(255) NOT NULL"
            elif tipo == 'DATE':
                query += f", {colonna} DATE NOT NULL"
            elif tipo == 'TINYTEXT':
                query += f", {colonna} TINYTEXT NOT NULL"
            elif tipo == 'TEXT':
                query += f", {colonna} TEXT NOT NULL"

    if colonne_FK:
        for chiave, valore in colonne_FK.items():
            if valore[0] == "INT":
                query += f", {chiave} {valore[0]} NOT NULL"
                query += f", FOREIGN KEY ({chiave}) REFERENCES {valore[1]}({valore[2]}) ON UPDATE CASCADE"
            elif valore[0] == "VARCHAR":
                query += f", {chiave} {valore[0]}(255) NOT NULL"
                query += f", FOREIGN KEY ({chiave}) REFERENCES {valore[1]}({valore[2]}) ON UPDATE CASCADE"
    query += ")"
    cursor.execute(query)
    db.commit()
    cursor.close()

def insert_query(db, table_name, columns, values):
    cursor = db.cursor()
    
    placeholders = ', '.join(['%s'] * len(columns.split(', ')))
    query_insert = f"INSERT IGNORE INTO {table_name} ({columns}) VALUES ({placeholders})"
    
    # Ensure values are formatted as a list of tuples
    if not all(isinstance(v, (list, tuple)) for v in values):
        values = [(v,) for v in values]

    try:
        cursor.executemany(query_insert, values)
        db.commit()
        print(f"{cursor.rowcount} rows were inserted.")
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        db.rollback()
    finally:
        cursor.close()

def select_query(db, tabella_name, colonne):
    cursor = db.cursor()
    query_select = f"""SELECT {colonne} FROM {tabella_name}"""
    cursor.execute(query_select)
    result = cursor.fetchall()
    cursor.close()
    return result

def insert_N_N(db, tabella_name, colonne, lista, elem_dict, n, diff_value=None):
    sub_tuple_elem = []
    for row in lista:
        if row[n]:
            elem_divisi = row[n].split("|")
            for elem_solo in elem_divisi:
                elem = elem_solo.strip()

                if elem in elem_dict:
                    if diff_value:
                        sub_tuple_elem.append((row[0], elem_dict[elem]))
                    else:
                        sub_tuple_elem.append((elem_dict[elem], row[0]))

    insert_query(db, tabella_name, colonne, sub_tuple_elem)


def insert_images(db, tabella_name, values):

    cursor = db.cursor()

    for row in values:
        image_ID = (row[0])
        urls = row[1]

        try:
            query = f"""INSERT INTO {tabella_name} (image_ID, image)
                        VALUES (%s)"""

            data = (image_ID, urls[0])
            cursor.execute(query, data)

        except:
            del row
    db.commit()

    cursor.close()


def fk_disable(db):
    cursor = db.cursor()
    query = """SET FOREIGN_KEY_CHECKS=0;"""
    cursor.execute(query)
    cursor.close()


def alter_table_unique(db, tabella_name, colonne):
    cursor = db.cursor()
    query = f"""ALTER TABLE {tabella_name} ADD UNIQUE ({colonne})"""
    cursor.execute(query)
    cursor.close()

def get_categories():
    # Connect to your database
    db = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="amazon"
    )
    cursor = db.cursor()

    # Query to get categories
    cursor.execute("SELECT DISTINCT category_names FROM category")
    categories = [row[0] for row in cursor.fetchall()]

    # Close the db
    cursor.close()
    db.close()

    return categories

def get_products_name():
    # Connect to your database
    db = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="amazon"
    )
    cursor = db.cursor()

    # Query to get products
    cursor.execute("SELECT product_name FROM product")
    product_name = cursor.fetchall()

    # Close the db
    cursor.close()
    db.close()

    return product_name

def get_rating():
    # Connect to your database
    db = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="amazon"
    )
    cursor = db.cursor()

    # Query to get products
    cursor.execute("SELECT rating FROM rating")
    products = cursor.fetchall()

    # Close the db
    cursor.close()
    db.close()

    return products

def get_product_img_link():
    # Connect to your database
    db = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="amazon"
    )
    cursor = db.cursor()

    # Query to get products
    cursor.execute("SELECT img_link FROM product")
    product_img_link = cursor.fetchall()

    # Close the db
    cursor.close()
    db.close()

    return product_img_link

def get_product_description():
    # Connect to your database
    db = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="amazon"
    )
    cursor = db.cursor()

    # Query to get products
    cursor.execute("SELECT description FROM product")
    product_description = cursor.fetchall()

    # Close the db
    cursor.close()
    db.close()

    return product_description

def get_products():
    # Connect to your database
    db = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="amazon"
    )
    cursor = db.cursor()

    # Query to get products
    cursor.execute("SELECT * FROM product")
    products = cursor.fetchall()

    # Close the db
    cursor.close()
    db.close()

    return products







if __name__ == "__main__":


    input_path = r'C:\Users\rames\Desktop\Lab11\docs\amazon.csv'


    categorie = []
    category_set = set()
    lista_completa = []
    price_list = []
    product_list = []
    rating_list = []
    with open(input_path, encoding="utf-8") as f:
        lettura = csv.reader(f, delimiter=",")
        next(f)
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

    create_database("localhost", "root", "", "amazon")

    db = connect_database("localhost", "root", "", "amazon")

    c = {
        "category_names": "VARCHAR"
    }
    crea_tabelle(db, "category", "category_ID", colonne_aggiuntive=c, Auto_I=True)

    c = {
        "discount_price": "FLOAT",
        "actual_price": "FLOAT",
        "discount_percentage": "VARCHAR"
    }
    crea_tabelle(db, "price", "price_ID", colonne_aggiuntive=c, Auto_I=True)

    c = {
        "product_name": "VARCHAR",
        "description": "VARCHAR",
        "img_link": "TEXT",
        "product_link": "TEXT"
    }
    c_fk = {
        "price_ID": ("INT", "price", "price_ID")
    }
    crea_tabelle_FK(db, "product", "product_ID", colonne_FK=c_fk, colonne_aggiuntive=c, tipo_ID="VARCHAR")

    c = {
        "rating": "FLOAT",
        "rating_count": "INT"
    }
    c_fk = {
        "product_ID": ("VARCHAR", "product", "product_ID")
    }
    crea_tabelle_FK(db, "rating", "rating_ID", colonne_FK=c_fk, colonne_aggiuntive=c, Auto_I=True)

    c_fk = {
        "product_ID": ("VARCHAR", "product", "product_ID"),
        "category_ID": ("INT", "category", "category_ID")
    }
    crea_tabelle_FK(db, "category_products", "cat_product_ID", colonne_FK=c_fk, Auto_I=True) 


    insert_query(db, "category", "category_names", list(category_set))

    insert_query(db, "price", "discount_price, actual_price, discount_percentage", price_list)

    insert_query(db, "product", "product_ID, product_name, description, img_link, product_link, price_ID", product_list)

    diz_category_DB = dict(select_query(db, "category", "category_names, category_ID"))
    diz_product_DB = dict(select_query(db, "product", "product_name, product_ID"))

    insert_N_N(db, "category_products", "product_ID, category_ID", lista_completa, diz_category_DB, 2, diff_value=True)
    insert_query(db, "rating", "rating, rating_count, product_ID", rating_list)      
