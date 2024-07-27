from tkinter import messagebox
import mysql.connector
from mysql.connector import errorcode


def create_database(host, user, password, database_name):
    try:
        # Connessione al server MySQL XAMPP localhost
        db = mysql.connector.connect(
            host=host,
            user=user,
            password=password
        )

        cursor = db.cursor()
        query = f"""DROP DATABASE IF EXISTS {database_name}"""

        cursor.execute(query)
        db.commit()
        cursor.close()

        # Creazione del cursore DB
        cursor = db.cursor()

        # Creazione del database se non esiste, utilizzerà i parametri forniti sopra
        cursor.execute("CREATE DATABASE IF NOT EXISTS {}".format(database_name))

        # Chiudo il cursore e la connessione al DB
        cursor.close()
        db.close()
    except mysql.connector.Error:
        messagebox.showerror("errore creazione DataBase", "Il sistema non è riuscito a creare\n"
                                                          "o a connettersi al database")


def connect_database(host, user, password, database):
    try:
        db = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        return db
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("dati non corretti")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database non esiste")
        else:
            print(err)
        return None


def truncate(db, tabella_name):
    cursor = db.cursor()
    query = f""" TRUNCATE {tabella_name} """
    cursor.execute(query)
    db.commit()
    cursor.close()


def crea_tabelle(db, tabella_name, colonna_ID, colonne_FK=None, colonne_aggiuntive=None, tipo_ID=None, Auto_I=None):
    cursor = db.cursor()

    if tipo_ID == "VARCHAR":
        query = f"""
            CREATE TABLE IF NOT EXISTS {tabella_name} (
            {colonna_ID} {tipo_ID}(255) PRIMARY KEY
            """
    else:
        tipo_ID = "INT"
        if Auto_I is None:
            query = f"""
                CREATE TABLE IF NOT EXISTS {tabella_name} (
                {colonna_ID} {tipo_ID} PRIMARY KEY
                """
        else:
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
            elif tipo == 'BOOLEAN':
                query += f", {colonna} BOOLEAN NOT NULL"

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


def insert_query(db, tabella_name, colonne, values):
    cursor = db.cursor()

    percentuali_esse = ', '.join(['%s'] * (len(colonne.split(', '))))
    query_insert = f"""
        INSERT IGNORE INTO {tabella_name} ({colonne})
        VALUES ({percentuali_esse}) 
        """

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


#SELECT product_ID, utente_ID FROM likes WHERE product_ID = "B002PD61Y4" AND utente_ID = 1
def select_query_WHERE(db, tabella_name, colonne, valori):
    cursor = db.cursor()
    where = " AND ".join([f"{col} = %s" for col in valori.keys()])
    try:
        query_select = f"SELECT {colonne} FROM {tabella_name} WHERE {where}"
    except mysql.connector.Error:
        print(query_select)

    cursor.execute(query_select, tuple(valori.values()))
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


def insert_likes(db, tabella_name, colonne, elem_diz):
    cursor = db.cursor()
    percentuali_esse = ', '.join(['%s'] * (len(colonne.split(', '))))
    query = f"INSERT INTO {tabella_name} ({colonne}) VALUES ({percentuali_esse})"
    data = [(utente_id, product_id) for utente_id, product_id in elem_diz.items()]
    try:
        cursor.executemany(query, data)
        db.commit()
    except mysql.connector.Error as err:
        print(f"Error  e: {err}")
    finally:
        cursor.close()


def delete_likes(db, tabella_name, colonne, elem_diz, unique_id):
    cursor = db.cursor()

    data = select_query(db, tabella_name, colonne)
    likes_tuple = list(elem_diz.items())[0]

    if likes_tuple in data:
        conditions = {f"product_ID": f"{likes_tuple[0]}", "utente_ID": f"{likes_tuple[1]}"}
        id_eliminare_tupla = select_query_WHERE(db, tabella_name, unique_id + ", " + colonne, conditions)
        id_eliminare = ([num[0] for num in id_eliminare_tupla])
        cursor = db.cursor()
        query = f"DELETE FROM {tabella_name} WHERE {unique_id} = %s"
        cursor.execute(query, (id_eliminare[0],))

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


def insert_cart(db, tabella_name, colonne, elem_diz):
    cursor = db.cursor()
    percentuali_esse = ', '.join(['%s'] * (len(colonne.split(', '))))
    query = f"INSERT INTO {tabella_name} ({colonne}) VALUES ({percentuali_esse})"
    data = [(utente_id, product_id) for utente_id, product_id in elem_diz.items()]
    try:
        cursor.executemany(query, data)
        db.commit()
    except mysql.connector.Error as err:
        print(f"Error  e: {err}")
    finally:
        cursor.close()

