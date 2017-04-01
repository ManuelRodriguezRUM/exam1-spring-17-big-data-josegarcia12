#Code here
x = sc.textFile('escuelasPR.csv')
temp = x.map(lambda m: m.split(','))
res = temp.filter(lambda k: k[2]=='Ponce' or k[2]=='Cabo Rojo' or k[2]=='Dorado')
res.collect()
