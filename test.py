

import csv


with open('u.item', encoding = "latin1") as g:
    reader2 = csv.reader(g, delimiter='|')
    item2 = []
    for row in reader2:
        item2.append(row)

print(item2)
