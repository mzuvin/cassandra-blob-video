# Handles file uploads in Python Tornado: http://tornadoweb.org/

import tornado.web
import logging
import os
import uuid
from cassandra_model import Cassandra,fileModel,fileChunk
import os,time,re,sys,random,threading
cassandra = Cassandra()
import codecs
from app import BaseHandler

def utf8len(s):
    return len(s)

def uuid_naming_strategy(original_name):
    "File naming strategy that ignores original name and returns an UUID"
    return str(uuid.uuid4())


class Parcala():

    def __init__(self,dosyaYolu,parcaboyut,hash):
        self.dosyaYolu=dosyaYolu
        self.parcaboyut=parcaboyut
        self.hash=hash
    
    def oku(self,boyut,index):
        bas=boyut.split('/')[0]
        son=boyut.split('/')[1]
        encodings = all_encodings()
        #for enc in encodings:
        #try:

        with codecs.open(self.dosyaYolu, 'r', 'ISO-8859-1' ) as ff:
            ff.seek(int(bas))
            #content = ff.read()
            lines=ff.read(int(son)).splitlines()
            data=cassandra.write(fileChunk,chunk_id=index,file_id=self.hash,content=bytes(lines[0], 'ISO-8859-1'))
            print(data.chunk_id)

        """
        with open(self.dosyaYolu,'r', encoding="utf-8") as f:
            # print the encoding and the first 500 characters
            #print(enc, f.read(500))
            f.seek(int(bas))
            lines=f.read(int(son)).splitlines()
            print("-------------")
            sys.stdout.write(str(lines[0])+"--------------\r")
            sys.stdout.flush()
            
            data=cassandra.write(fileChunk,chunk_id=index,file_id=self.hash,content=lines[0].decode("utf-8"))
            print(data)
            f.close()
        """
        #except Exception as e:
        #    print(e)
        #f=open(self.dosyaYolu,"r",encoding='cp1250')
        
        #lines[0]
        
    def isInt(self,x):
        if x%1 == 0:
            return False#tam bölünüyor
        else:
            return True


    """
    Dosyayı belirtilen parcalara gore bir diziye aktarıyoruz.
    Daha basitte yapılabilir.
    ornek vermek gerekirse
    0/100
    101/200
    201/300
    """
    def boyutListHazirla(self,boyut,parca):
        bol=float(boyut)/float(parca)
        sayilar=[]
        print ("toplam parca"+str(bol))
        print ("toplam mb: "+str(boyut/1024*1024))
        t=0
        for i in range(0,int(bol)):
            if i==0:
                j=i
            b=parca
            t=b*(i+1)
            s=str(j)+"/"+str(t)
            #print s
            sayilar.append(s)
            j=t+1
        if(self.isInt(bol)):
            isi=str(t+1)+"/"+str(boyut)
            sayilar.append(isi)

        return sayilar



    def run(self):
        boyut = os.path.getsize(self.dosyaYolu)
        boyutlist=self.boyutListHazirla(boyut,int(self.parcaboyut)*1024*1024)#50*1024*102450 mb lik parcalar.
        
        for i in range(0,len(boyutlist)):
            self.oku(boyutlist[i],i)
        """
        #ayni anda en fazla 5 islem yapiyor.
        maxthreads = 5
        for i in range(0,len(boyutlist)):
            while threading.activeCount() >= maxthreads:
                time.sleep(0.2)
                sys.stdout.write(str(i)+ " Bekliyor.\r")
                sys.stdout.flush()
            sys.stdout.write(str(i)+ " Araniyor.\r")
            sys.stdout.flush()
            threading.Thread(target = self.oku, args=(boyutlist[i],i)).start()
        """
        return True

class UploadHandler(BaseHandler):
    "Handle file uploads."

    def initialize(self, upload_path, naming_strategy):
        """Initialize with given upload path and naming strategy.
        :keyword upload_path: The upload path.
        :type upload_path: str
        :keyword naming_strategy: File naming strategy.
        :type naming_strategy: (str) -> str function
        """
        self.upload_path = upload_path
        if naming_strategy is None:
            naming_strategy = uuid_naming_strategy
        self.naming_strategy = naming_strategy

    def post(self):
        fileinfo = self.request.files['filearg'][0]
        filehash = self.naming_strategy(fileinfo['filename'])
        filename = fileinfo['filename']
        filesize = utf8len(fileinfo['body'])
        userid = tornado.escape.xhtml_escape(self.current_user)
        
        try:
            """
            
            yüklenen videoyu kaydedip, bilgilerini cassandra ya yazıyoruz. 
            Bize daha sonra dosyanın hash'i lazım. file_chunk'a yazabilmek için. 
            
            """
            with open(os.path.join(self.upload_path, filename), 'wb') as fh:
                data=cassandra.write(fileModel,id=filehash,name=filename,content_length=filesize,userid=userid)
                fh.write(fileinfo['body'])
            
            """
            Parcala class'ı 
            Parcala(dosyaYolu, dosyaParcaBoyutuMB, hash)

            dosyaParcaBoyutu 1 ise 1mb olur.

            """
            p=Parcala(self.upload_path+filename,1,data.id)
            
            #çalıştır.
            p.run()

            logging.info("%s uploaded %s, saved as %s",
                         str(self.request.remote_ip),
                         str(fileinfo['filename']),
                         filename)
        except IOError as e:
            logging.error("Failed to write file due to IOError %s", str(e))