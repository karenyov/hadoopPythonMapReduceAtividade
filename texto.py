
import mincemeat
import glob
import csv

text_files = glob.glob('trab\\*')

def file_contents(file_name):
    f = open(file_name, encoding="utf8")
    try:
        return f.read()
    finally:
        f.close()

source = dict((file_name, file_contents(file_name)) for file_name in text_files)

def mapfn(k, v):
    print('map ' + k)
    from stopwords import allStopWords
    words = dict()


    contagem = dict()
    for line in v.splitlines():

        title = line.split(':::')[-1]
        autores = line.split(':::')[1:-1]

        if '::' in autores[0]:
            autores = autores[0].split('::')
        
        for autor in autores:

            if autor not in 'Grzegorz Rozenberg' and autor not in 'Philip S. Yu':
                continue
            
            if (autor not in contagem):
                contagem.update({autor: dict()})

            for word in title.split():
                newWord = word.lower().replace('.','').replace('"','')
            
                if (newWord not in contagem[autor] and newWord not in allStopWords):
                    contagem[autor].update({newWord: 1})
                    yield autor + ':' + newWord ,1

                
def reducefn(k, v):
    
    return sum(v)

s = mincemeat.Server()

# A fonte de dados pode ser qualquer objeto do tipo dicionário
s.datasource = source
s.mapfn = mapfn
s.reducefn = reducefn

results = s.run_server(password = "changeme")
w = csv.writer(open("RESULT.csv", "w"))
for k,v in results.items():
    w.writerow([k, v])
