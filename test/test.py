import requests,os,uuid as u
from uuid import UUID
from cassandra_model import Cassandra,userModel,fileModel,fileChunk
cassandra = Cassandra()
import time
from functools import partial
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


	def chunked(self,file, chunk_size):
		return iter(lambda: file.read(chunk_size), '')

	def oku2(self):
		with open(self.dosyaYolu, "rb") as ff:
			index=0
			for data in iter(partial(ff.read, self.parcaboyut), b''):
				
				print(index)
				print("data uzunluğu {}".format(len(data)))
				data=cassandra.write(fileChunk,chunk_id=index,file_id=self.filehash,content=bytes(data))
				print(data.chunk_id)
				print("cass data uzunluğu {}".format(len(data.content)))
				index=index+1
				
				#for data in self.chunked(ff, 1*1024*1024):
				
				#print("son data {}".format(len(data)))
				#data=cassandra.write(fileChunk,chunk_id=index,file_id=self.filehash,content=bytes(data))
				#print(data.chunk_id)
				#print("cass data {}".format(len(data.content)))

	
	def run(self,parcaboyut):
		self.parcaboyut=parcaboyut
		self.boyut = os.path.getsize(self.dosyaYolu)
		
		data=cassandra.write(fileModel,id=self.filehash,name=filename,content_length=self.boyut,userid=self.usertoken)
		print(data.id)
		self.oku2()
		#for i in range(0,len(boyutlist)):
		#	self.oku(boyutlist[i],i)

	def checkFile(self):
		q=fileChunk.objects.allow_filtering()#
		all_objects=q.filter(file_id=UUID(str('8808708c-b75e-4997-b8c5-d645cb983b07')))
		bytearr=[]
		for i in all_objects:
			print(len(i['content']))
			bytearr.insert(int(i['chunk_id']),bytes(i['content']))
		#array to bytes join
		print(len(bytearr))
		data=b"".join(bytearr)
		cassandrafilebyte=len(data)
		dosyabyte=self.boyut
		"""
		1017221
		1048576
		2065797
		toplam parca2.9700984954833984
		toplam mb: 3114374.0
		['0/1048576', '1048577/2097152', '2097153/3114374']
		8808708c-b75e-4997-b8c5-d645cb983b07
		1017220
		cass data 1048576
		cass data 2065797
		cass data 1017221
		"""
		print('Cassandra\'dan gelen datanın byte uzunluğu {}'.format(len(data)))
		print('Yüklenen dosyanın byte uzunluğu {}'.format(self.boyut))
		print('Dosyalar arasındaki fark {}'.format(dosyabyte-cassandrafilebyte))


filename='son.mp4'
dt=DosyaTesti(filename,filename,'54cb638c6851423889f34fc55b13cf24')
dt.run(1*1024*1024)
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