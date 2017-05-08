from sqlalchemy import create_engine


from sqlalchemy.orm import sessionmaker
from feeds_model import *

import timeit

engine = create_engine("mysql+pymysql://root:33422516@localhost/ads?charset=utf8mb4")
Session = sessionmaker(bind=engine)
session = Session()





feed_in = session.query(FeedIn).filter_by( id = 491 ).one()

xml = """
    <ad>
  <property_type><![CDATA[Departamento]]></property_type>
  <id><![CDATA[742t]]></id>
  <url><![CDATA[https://www.easybroker.com/ar/inmueble/742-departamento-en-capital-federal-recoleta?utm_source=Trovit]]></url>
  <title><![CDATA[Juncal y Guido]]></title>
  <content><![CDATA[EasyBroker ID: 742. Departamento en Argentina, Capital Federal, Recoleta. En exclusiva ubicacion lindísimo departamento de 2 1/2 ambientes en atractivo edificio.
El living comedor tiene una mesa redonda con 4 sillas, un sofa de 2 cuerpos (cama de 1&amp;1/2 plaza), una mesa ratona y equipo split frio calor. 
La cocina está completamente equipada. Tiene heladera, cocina con horno y cuatro hornallas, microondas, cafetera eléctrica, tostadora eléctrica, alacenas..
El dormitorio tiene dos camas de una plaza (que puede ser una cama doble) con mesa de luz y amplio placard tipo vestidor, percha valet, Tv  LCD pantalla plana 25 ", cable, internet banda ancha,  WiFi reproductor de DVD, teléfono. 
Area de escritorio. 
El baño es completo con ducha y bañera, secador de pelo, toallero electrico. Ropa de cama (sabanas frazada y edredon) y toallas incluídas.]]></content>
  <type><![CDATA[Short term rentals]]></type>
  <agency><![CDATA[Luz Ocampo Negocios Inmobiliarios]]></agency>
  <floor_area unit="meters"><![CDATA[45]]></floor_area>
  <rooms><![CDATA[1]]></rooms>
  <bathrooms><![CDATA[1]]></bathrooms>
  <city><![CDATA[Capital Federal]]></city>
  <city_area><![CDATA[Recoleta]]></city_area>
  <postcode><![CDATA[1062]]></postcode>
  <region><![CDATA[Buenos Aires]]></region>
  <longitude><![CDATA[-58.3869675]]></longitude>
  <latitude><![CDATA[-34.5928203]]></latitude>
  <pictures>
    <picture>
      <picture_url><![CDATA[http://s3.amazonaws.com/assets.moveglobally.com/property_images/742/15903/Living.JPG?1485443019]]></picture_url>
    </picture>
    <picture>
      <picture_url><![CDATA[http://s3.amazonaws.com/assets.moveglobally.com/property_images/742/15907/baño.jpg?1485443019]]></picture_url>
    </picture>
    <picture>
      <picture_url><![CDATA[http://s3.amazonaws.com/assets.moveglobally.com/property_images/742/15911/Comedor2.jpg?1485443019]]></picture_url>
    </picture>
    <picture>
      <picture_url><![CDATA[http://s3.amazonaws.com/assets.moveglobally.com/property_images/742/15913/escritorio.JPG?1485443019]]></picture_url>
    </picture>
    <picture>
      <picture_url><![CDATA[http://s3.amazonaws.com/assets.moveglobally.com/property_images/742/15915/Living2.JPG?1485443019]]></picture_url>
    </picture>
    <picture>
      <picture_url><![CDATA[http://s3.amazonaws.com/assets.moveglobally.com/property_images/742/15917/placardjuncal.jpg?1485443019]]></picture_url>
    </picture>
  </pictures>
  <date><![CDATA[26/01/2017]]></date>
  <time><![CDATA[15:03]]></time>
  <floor_number><![CDATA[2]]></floor_number>
  <year><![CDATA[1965]]></year>
  <is_new><![CDATA[0]]></is_new>
</ad>

    """
ad_mapped = feed_in.feed_type.feed_mapper.map(xml)


print("------")
xml2 = """
<ad>
    <property_type>
        <![CDATA[Departamento]]>
    </property_type>
    <id>
        <![CDATA[826t]]>
    </id>
    <url>
        <![CDATA[https://www.easybroker.com/ar/inmueble/826-departamento-en-capital-federal-recoleta?utm_source=Trovit]]>
    </url>
    <title>
        <![CDATA[Ayacucho y Melo]]>
    </title>
    <content>
        <![CDATA[EasyBroker ID: 826. Departamento en Argentina, Capital Federal, Recoleta. Luminoso y tranquilo estudio, con lindísima vista a excelente pulmón verde. Se encuentra ubicado en una muy buena zona de la ciudad: a pasos del Village Recoleta -con sus numerosos cines-, en una zona rodeada de restaurantes, bares, discos, galerías de arte y centros culturales. 
        
        
        
        El apartamento ofrece un área de estar con escritorio 3 butacones y 1 pouf, una cocina equipada con barra desayunador, un baño completo con bañera, y una cama dos plazas. Cuenta con TV con cable e Internet. Está completamente equipado: TV, heladera con freezer, tostadora, vajilla, maquina de café, microondas, plancha, aire acondicionado frío/calor. 
        
        La tarifa incluye: expensas, impuestos, cable, y los básicos de luz, gas y teléfono.]]>
    </content>
    <type>
        <![CDATA[Short term rentals]]>
    </type>
    <agency>
        <![CDATA[Luz Ocampo Negocios Inmobiliarios]]>
    </agency>
    <price currency="USD" period="monthly">
        <![CDATA[750]]>
    </price>
    <floor_area unit="meters">
        <![CDATA[30]]>
    </floor_area>
    <rooms>
        <![CDATA[1]]>
    </rooms>
    <bathrooms>
        <![CDATA[1]]>
    </bathrooms>
    <city>
        <![CDATA[Capital Federal]]>
    </city>
    <city_area>
        <![CDATA[Recoleta]]>
    </city_area>
    <region>
        <![CDATA[Buenos Aires]]>
    </region>
    <longitude>
        <![CDATA[-58.39429]]>
    </longitude>
    <latitude>
        <![CDATA[-34.5921138]]>
    </latitude>
    <pictures>
        <picture>
            <picture_url>
                <![CDATA[http://s3.amazonaws.com/assets.moveglobally.com/property_images/826/19353/living.jpg]]>
            </picture_url>
        </picture>
        <picture>
            <picture_url>
                <![CDATA[http://s3.amazonaws.com/assets.moveglobally.com/property_images/826/19357/cocina.jpg]]>
            </picture_url>
        </picture>
        <picture>
            <picture_url>
                <![CDATA[http://s3.amazonaws.com/assets.moveglobally.com/property_images/826/19361/escritorio.jpg]]>
            </picture_url>
        </picture>
        <picture>
            <picture_url>
                <![CDATA[http://s3.amazonaws.com/assets.moveglobally.com/property_images/826/19359/dormitorio.jpg]]>
            </picture_url>
        </picture>
        <picture>
            <picture_url>
                <![CDATA[http://s3.amazonaws.com/assets.moveglobally.com/property_images/826/19355/baño.jpg]]>
            </picture_url>
        </picture>
        <picture>
            <picture_url>
                <![CDATA[http://s3.amazonaws.com/assets.moveglobally.com/property_images/826/46207/cfte.jpg]]>
            </picture_url>
        </picture>
    </pictures>
    <date>
        <![CDATA[26/01/2017]]>
    </date>
    <time>
        <![CDATA[15:00]]>
    </time>
    <floor_number>
        <![CDATA[5]]>
    </floor_number>
    <year>
        <![CDATA[1980]]>
    </year>
    <is_new>
        <![CDATA[0]]>
    </is_new>
</ad>
"""

ad_mapped2 =  feed_in.feed_type.feed_mapper.map(xml2)

print("---------------")
print(ad_mapped)
print(ad_mapped2)
print("---------------")

#print("Hola", type(feed_in.feed_type.additional_params))
