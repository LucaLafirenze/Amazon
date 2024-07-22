import csv
import pprint
import Database_Luca as Luca

input_path = 'C:/Users/Luca/OneDrive/Documenti/Data_Engineer/Francesco/lezione file csv/amazon.csv'

with open(input_path, encoding="utf-8") as f:
    lettura = csv.reader(f, delimiter=",")
    next(f)
    categorie = []
    category_set = set()
    lista_completa = []
    price_list = []
    for elem in lettura:
        sub_price_list = []
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

print(price_list)

for elem in categorie:
    for i in elem:
        category_set.add(i)


Luca.create_database("localhost", "root", "", "amazon_PROVE")

db = Luca.connect_database("localhost", "root", "", "amazon_PROVE")

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
    "category_ID": ("INT", "category", "category_ID"),
    "price_ID": ("INT", "price", "price_ID")
}
Luca.crea_tabelle_FK(db, "product", "product_ID", colonne_FK=c_fk, colonne_aggiuntive=c, tipo_ID="VARCHAR")

c = {
    "rating_count": "INT",
    "rating": "FLOAT",
}
c_fk = {
    "product_ID": ("VARCHAR", "product", "product_ID")
}

Luca.crea_tabelle_FK(db, "rating", "rating_ID", colonne_FK=c_fk, colonne_aggiuntive=c)

c = {
    "user_name": "VARCHAR"
}
Luca.crea_tabelle(db, "utente", "utente_ID", colonne_aggiuntive=c, tipo_ID="VARCHAR")

c = {
    "review_title": "VARCHAR",
    "review_content": "TEXT",
}
c_fk = {
    "product_ID": ("VARCHAR", "product", "product_ID"),
    "utente_ID": ("VARCHAR", "utente", "utente_ID")
}

Luca.crea_tabelle_FK(db, "review", "review_ID", colonne_FK=c_fk, colonne_aggiuntive=c, tipo_ID="VARCHAR")

insert_tuple = tuple(category_set)
Luca.insert_query(db, "category", "category_names", insert_tuple)
Luca.insert_query(db, "price", "discount_price, actual_price, discount_percentage", price_list)

diz_category_DB = dict(Luca.select_query(db, "category", "category_names, category_ID"))





