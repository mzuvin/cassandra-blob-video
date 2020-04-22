import tornado.ioloop
import tornado.web
from cassandra_model import Cassandra,userModel
import os,json
import json
from uuid import UUID
import logging
from uploadhandler import uploadhandler




class UUIDEncoder(json.JSONEncoder):
	"""
	JSON da UUİD hatasını çözmek için UUİD objesini stringe dönüştürüyoruz. JSON dumps edilebiliyor.
	"""
    def default(self, obj):
        if isinstance(obj, UUID):
            # Eğer obje uuid ise, uuid objesini str e çevir.
            return obj.hex
        return json.JSONEncoder.default(self, obj)

class BaseHandler(tornado.web.RequestHandler):
	"""
	Base Request Handler, Diğer sınıflar buradan miras alıyor.

	get_current_user() session oturum yönetimi için bir getter.
	"""
    def get_current_user(self):
        return self.get_secure_cookie("user")

class UploadForm(BaseHandler):
	"""
	
	Dosya yüklemek için templates'i render ediyoruz.

	"""
    def get(self):
        self.render("upload.html")

class MainHandler(BaseHandler):

	"""
	Ana sayfa kullanıcı giriş yapmışsa token ve link gözüküyor.
	Eğer kullanıcı giriş yapmadıysa token alması için kayıt sayfasına yönlendiriliyor.
	
	token cerezlere set edilen tokeni alıyor.
	"""

	def get(self):
		if not self.current_user:
			self.redirect("/user")
			return
		token = tornado.escape.xhtml_escape(self.current_user)
		self.render("home.html",token=token,title="Home")

class UserHandler(BaseHandler):

	"""
	Token alma sayfası 
	.............................................
	.cassandra.write(modelClass,**kwargs)
	.
	.->	modelClass : oluşturulan modelin referansı
	.->	**kwargs : model bulanan database'deki colonların değişkenleri 
	"""

	def get(self):
		self.render("register.html", title="Kayıt ol")
	def post(self):
		#data = json.loads(self.request.body)
		username = self.get_argument("username")
		email = self.get_argument("email")
		password = self.get_argument('password')
		#self.write(username)
		cassandra = Cassandra()

		#q = userModel.objects.filter(email=email)
		#self.write(json.dumps(q.first(),cls=UUIDEncoder))
		data = cassandra.write(userModel,username=username,password=username,email=email)
		
		self.set_secure_cookie("user", data.id.hex)
		self.redirect("/")
		#self.write(json.dumps(obj, cls=UUIDEncoder))
		#self.write('{"token":'+str(data.id)+'}')



def make_app():

	"""
	tornado için genel ayarlar..



	"""
	settings = dict(
	site_title=u"Cassandra Video Yönetimi",
	template_path=os.path.join(os.path.dirname(__file__), "templates"),
	static_path=os.path.join(os.path.dirname(__file__), "static"),
	xsrf_cookies=False,
	cookie_secret="kmalksdmdklasmdksandlnaskln",
	debug=True,
	autoreload=True
	)

	#request route patları ayarlıyoruz.
	return tornado.web.Application([
		(r"/", MainHandler),
		(r"/user", UserHandler),
		(r"/upload", UploadForm),
        (r"/uploadHandler", uploadhandler.UploadHandler,
             dict(upload_path="tmp/video/", naming_strategy=None))
	],**settings)

if __name__ == "__main__":
    app = make_app()
    app.listen(8888)
    tornado.ioloop.IOLoop.current().start()