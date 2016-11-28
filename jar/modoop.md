#**Instalación de JAR Mongo – Hive**

1.	Validar que la conexión con MongoDB se acepte desde el host de HIVE
		* Utilizar comando > telnet <hosto/to/mongodb> <port mongodb 27017>
2.	Validar lectura de colecciones en mongo
		* mongo host –u <user name> -p <user pwd>
		* use <data base name, en SIMO es test>
		* hacer consulta a collection siguiente commando: db.SIT_CONSULTA_EXTERNA.find({},{“classname”:1});
3.	Integrar HIVE to Mongo
		* Descargar el driver de Mongo-Hive de https://github.com/radianv/hive-xml/tree/master/jar
		* Copiar los archivos JAR to hadoop lib environment
			i.	sudo cp mongo-hadoop-core-1.3.0.jar /usr/lib/hadoop/lib/.
			ii.	sudo cp mongo-hadoop-hive-1.3.0.jar /usr/lib/hadoop/lib/.
			iii.	sudo cp mongo-java-driver-3.2.2.jar /usr/lib/hadoop/lib/.
		* Realizar lecture	 de collection on Mongodb de acuerdo a el siguiente EJEMPLO (no corresponde a alguna colección de Mongo): 
					CREATE TABLE <nombre de tabla>
					( 
						id INT,
						name STRING,
						age INT,
						work STRUCT<title:STRING, hours:INT>
					)
					STORED BY 'com.mongodb.hadoop.hive.MongoStorageHandler'
					WITH SERDEPROPERTIES('mongo.columns.mapping'='<coleccion de mongo SIMO>')
					TBLPROPERTIES('mongo.uri'='mongodb://<host/to/mongo>:27017/< db.coleccion/a/consultar >');
		* Realizar select de table external hive.

NOTA: Para este ejercicio me cambie a mi instancia local de mongo ya que por los permisos no puede leer y validar que funcionara el JAR, recomiendo hacer el intento con la instancia SIMO y validar que tenemos los problemas de permisos y nombres de campos con minusculasMayusculas.
En caso de no avanzar te recomiendo generar una collection nueva en una instancia nueva y hacer el ejercicio ()
 

