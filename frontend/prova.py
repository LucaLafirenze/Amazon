from backend import amazon as data




for elem in data.get_products_name():
    print(type(elem))

lista_completa = []
for elem in data.get_products():
    lista_completa.append(elem)

rating = []
for elem in data.get_rating():
    rating.append(elem)

risultato = []
for t1, t2 in zip(rating, lista_completa):
    nuova_tupla = t1 + t2
    risultato.append(nuova_tupla)

print(risultato)

