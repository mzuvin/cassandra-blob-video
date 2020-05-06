import requests,os,uuid as u
from uuid import UUID
from cassandra_model import Cassandra,userModel,fileModel,fileChunk
cassandra = Cassandra()


def registerWeb(username,password,email):
	url="http://127.0.0.1:8888/"
	headers = {
		'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.122 Safari/537.36',
	}

	data = {
		'email': email,
		'username': username,
		'password': password
	}
	response = requests.post(url+'user', headers=headers,data=data)

	print(response.text)

#registerWeb('username','password','email@hotmail.com')

def userModelTest(username,password,email):
	username = username
	email = email
	password = password
	
	data = cassandra.write(userModel,username=username,password=username,email=email)
	if data.id.hex:
		print('Token %s'.format(data.id.hex))

class DosyaTesti:
	def __init__(self,name,dosyaYolu,usertoken):
		self.dosyaYolu=dosyaYolu
		self.filehash=self.hash()
		self.name=name
		self.usertoken=usertoken

	def hash(self):
		return str(u.uuid4())

	def oku(self,boyut,index):
		bas=boyut.split('/')[0]
		son=boyut.split('/')[1]
		#for enc in encodings:
		#try:

		with open(self.dosyaYolu, "rb") as ff:
			
			"""
			ff.seek(self.parcaboyut) # omit if you don't want to skip this chunk
			chunk = ff.read(self.parcaboyut)
			print("len"+str(len(chunk)))
			data = cassandra.write(fileChunk,chunk_id=index,file_id=self.filehash,content=chunk)
			print(len(data.content))
			while data:
				chunk = ff.read(self.parcaboyut)
				data = cassandra.write(fileChunk,chunk_id=index,file_id=self.filehash,content=chunk)
				print(len(data.content))
			"""
			#--------------------------------
			
			ff.seek(int(bas))
			#content = ff.read()
			lines=ff.read(int(son))#.splitlines()
			#print(len(lines))
			#sondata=b''.join(lines)
			print("son data {}".format(len(lines)))
			data=cassandra.write(fileChunk,chunk_id=index,file_id=self.filehash,content=bytes(lines))
			print(data.chunk_id)
			print("cass data {}".format(len(data.content)))
			

	def isInt(self,x):
		if x%1 == 0:
			return False#tam bölünüyor
		else:
			return True

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

	def run(self,parcaboyut):
		self.parcaboyut=parcaboyut
		self.boyut = os.path.getsize(self.dosyaYolu)
		boyutlist=self.boyutListHazirla(self.boyut,int(parcaboyut)*1024*1024)#50*1024*1024 50 mb lik parcalar.
		print(boyutlist)
		data=cassandra.write(fileModel,id=self.filehash,name=filename,content_length=self.boyut,userid=self.usertoken)
		print(data.id)
		for i in range(0,len(boyutlist)):
			self.oku(boyutlist[i],i)

	def checkFile(self):
		q=fileChunk.objects.allow_filtering()#
		all_objects=q.filter(file_id=UUID(str(self.filehash)))
		bytearr=[]
		for i in all_objects:
			bytearr.insert(int(i['chunk_id']),bytes(i['content']))
		#array to bytes join
		data=b"".join(bytearr)
		cassandrafilebyte=len(data)
		dosyabyte=self.boyut

		print('Cassandra\'dan gelen datanın byte uzunluğu {}'.format(len(data)))
		print('Yüklenen dosyanın byte uzunluğu {}'.format(self.boyut))
		print('Dosyalar arasındaki fark {}'.format(dosyabyte-cassandrafilebyte))


filename='video.mp4'
dt=DosyaTesti(filename,filename,'54cb638c6851423889f34fc55b13cf24')
dt.run(1)
dt.checkFile()


"""
toplam parca0.4350414276123047
toplam mb: 456174.0
daf36f3a-36f1-4661-9c0d-2489bdbf356d
0
Cassandra'dan gelen datanın byte uzunluğu 452778
Yüklenen dosyanın byte uzunluğu 456174
Dosyalar arasındaki fark 3396
"""