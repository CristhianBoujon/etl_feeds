from lxml import etree
from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound
from sqlalchemy.orm.session import object_session
from sqlalchemy.sql.expression import func
from feed_process.tools.cleaner import slugify, clear_file
from feed_process.models.db import DBSession, Session
from feed_process.models.model import *
from feed_process.translation import Translator
from datetime import datetime as dtt
from datetime import date as dt
from string import Formatter
import os
import re

class FeedMappingException(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message

class FeedParseException(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message

class MapMethod:
    def __init__(self, **kwargs):
        self.additional_args = kwargs

    def map(self, *args, **kwargs):
        raise NotImplementedError("La función map() no está implementada para " + type(self).__name__)

class DescriptionMapMethod(MapMethod):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.formatter = Formatter()
        self.translator = Translator()

    def map(self, *args, **kwargs):
        try:
            template = self.additional_args["template"]
            locale = kwargs["raw_ad"].feed_in.locale

            # We take all keywords (no positional) and we translate them and then we create a dict
            # eg: 
            #     template = "That is an example {SALARY}: $ {0} in {COMPANY}: {1}"
            #     locale = "es_ar"
            #     translated_words = {"SALARY": "Salario", "COMPANY": "Compañía"}
            # 
            translated_words = {
                parse[1]: self.translator.translate(locale, parse[1], "addesc_label") 
                for parse in self.formatter.parse(template) if type(parse[1]) == str and not parse[1].isdigit()
            }

            addesc = self.formatter.format(template, *args, **translated_words)

            return {"addesc": addesc}
        except KeyError:
            type_id = kwargs["raw_ad"].feed_in.feed_type.id if kwargs.get("raw_ad", None) else ""
            msg = "El parámetro 'template' no se encuentra definido para " + type_id
            raise FeedMappingException( msg )

class TitleMapMethod(MapMethod):
    def map(self, *args, **kwargs):
        return {"adtitle": ", ".join(args)}

class UrlMapMethod(MapMethod):
    def map(self, *args, **kwargs):
        return {'link': args[0]}

class LocationMapMethod(MapMethod):
    def map(self, *args, **kwargs):

        slug_location_name = ",".join([slugify(arg, separator = "-") for arg in args if arg])
        session = Session()
        country_id = kwargs["raw_ad"].feed_in.country_id if kwargs.get("raw_ad", None) else 0
        
        query = session.query(FeedInLocation).filter_by(slug_location_name = slug_location_name, country_id = country_id)
        
        try:
            feed_in_location = query.one()
            result = self.__result(feed_in_location)
        except NoResultFound:

            feed_in_location = FeedInLocation(slug_location_name = slug_location_name, country_id = country_id)
            session.add(feed_in_location)
            session.commit()
            result = self.__result(feed_in_location)

        except MultipleResultsFound:
            feed_in_location = query.first()
            result = self.__result(feed_in_location)
        finally:
            session.close()

        return result

    def __result(self, feed_in_location):
        return {
            "cityid": feed_in_location.state_id, 
            "area": feed_in_location.location_name,
            "location_id": feed_in_location.location_id,
            "_feed_in_location_id": feed_in_location.id # If feed_in_location is pending yet we return the FeedInLocation instance 
                                                        # sinse we will update ad later when moderator fill the location data 
        }

class SubCategoryMapMethod(MapMethod):
    def map(self, *args, **kwargs):
        slug_subcat_name = ",".join([slugify(arg) for arg in args if arg])
        session = Session()
        query = session.query(FeedInSubcategory).\
                    filter_by(  slug_subcat_name = slug_subcat_name, 
                                catid = kwargs["raw_ad"].feed_in.category_id)
        
        try:
            feed_in_subcat = query.one()
            result = self.__result(feed_in_subcat)
        except NoResultFound:
            
            feed_in_subcat = FeedInSubcategory(
                slug_subcat_name = slug_subcat_name, 
                catid = kwargs["raw_ad"].feed_in.category_id)
            
            session.add(feed_in_subcat)
            session.commit()
            result = self.__result(feed_in_subcat)

        except MultipleResultsFound:
            feed_in_subcat = query.first()
            result = self.__result(feed_in_subcat)
        finally:
            session.close()

        return result

    def __result(self, feed_in_subcat):
        return {
            "subcatid": feed_in_subcat.subcategory_id, # subcatid that matches with slug_subcat_name
            "_feed_in_subcat_id": feed_in_subcat.id # If feed_in_subcats is pending yet we return the FeedInLocation instance 
                                                                # sinse we will update ad later when moderator fill the location data 
        }

class PriceMapMethod(MapMethod):
    def map(self, *args, **kwargs):
        if type(args[0]) == str and args[0] == "":
            price = 0  
        elif type(args[0]) == str:
            price = float(args[0].replace(",", "."))
        else:
            price = float(args[0])

        return {"price": price}


class CurrencyMapMethod(MapMethod):
    def map(self, *args, **kwargs):
        return {"currency": args[0]}

class IdMapMethod(MapMethod):
    """ Return the id found into the feed """
    def map(self, *args, **kwargs):
        return {"_id_in_feed": args[0]}

class DateMapMethod(MapMethod):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # we create a regex patter in order to match time sinse sometimes dates contains time data
        # time_regex matchs hours:minutes, hours:minutes:seconds and PM/AM
        self.time_regex = re.compile('(\d{1,2}:\d{1,2}(:\d{1,2})*)( )*(PM|AM)*')

    def map(self, *args, **kwargs):

        # if args[0] is integer it will assume timestamp 
        if(args[0].isdigit()):
            date = dt.fromtimestamp(int(args[0]))
        else:
            # Date or datetime as string type
            str_input_date = args[0]

            # if it contains time, we remove it
            str_input_date = self.time_regex.sub('', str_input_date).strip() 

            date = dtt.strptime(str_input_date, kwargs["raw_ad"].feed_in.format_date).date()
        
        # This line was introduced sinse strftime() method was restricted to years >= 1000
        # it spcified https://docs.python.org/3/library/datetime.html#strftime-strptime-behavior 
        date = dt(1000, 1, 1) if date.year < 1000 else date

        str_output_date = date.strftime(self.additional_args["output_format"])

        return {"date": str_output_date, "_format": self.additional_args["output_format"]}

class ImageMapMethod(MapMethod):
    def map(self, *args, **kwargs):
        if type(args[0]) == str and args[0]:
            return {"_images": [args[0]]}
                
        return {"_images": args[0]}

MAP_METHODS = {
    'DESCRIPCION': DescriptionMapMethod,
    'CATEGORIA': SubCategoryMapMethod,
    'ID': IdMapMethod,
    'MONEDA': CurrencyMapMethod,
    'PRECIO': PriceMapMethod,
    'TITULO': TitleMapMethod,
    'UBICACION': LocationMapMethod,
    'URL': UrlMapMethod,
    'FECHA': DateMapMethod,
    'IMAGEN': ImageMapMethod
}

class AdMapper:
    def map(self, raw_ad):
        raise NotImplementedError("El método map() no está implementado para " + type(self).__name__)        
    def iter_from_file(self, file):
        """ returns a iterator from file """
        raise NotImplementedError("El método iter_from_file() no está implementado para " + type(self).__name__)

    def get_raw_content(self):
        """ Must returns a string with a ad information in a specific format format. Eg: json, xml, csv, etc """
        raise NotImplementedError("El método get_raw_content() no está implementado para " + type(self).__name__)


class XmlAdMapper(AdMapper):

    def __init__(self, feed_type):
        self.map_methods = {}
        self.feed_type = feed_type
        self.db_session = DBSession()
        try:
            self.root = feed_type.mapping.filter_by(method = "ROOT").one().field

        except NoResultFound:
            raise FeedMappingException("No se ha creado ROOT para el mapeo de {0}".format(feed_type.id))

        except MultipleResultsFound:
            raise FeedMappingException("Se ha creado más de un ROOT para el mapeo de {0}".format(feed_type.id))

    def iter_from_file(self, file):
        self.__file_path = file
        self.__iteration_count = 0
        self.__file_was_cleaned = False # Flag determines if xml file was cleaned to avoid infite loops
        self.__xml_parser = etree.iterparse(self.__file_path, tag = self.root)

    def get_raw_content(self):
        """ Returns a string with a ad information in xml format """

        """
            How does it work?
            Function iterate over each element next(self.__xml_parser). If a XML syntax error was found
            it will raise a XMLSyntaxError exception. If the file was not cleaned yet (self.__file_was_cleaned is False)
            we will try clean it/fix it calling clear_file().
            After then we restart the iterator and jump the elements thas has been returned and call get_raw_content again.
            If XMLSyntaxError is raised again (self.__file_was_cleaned is True) means that clear_file() couldn't fix it 
            so raise the excepcion and avoid infinite recursion
        """
        try:
            event, element = next(self.__xml_parser)
            raw_content = etree.tostring(element, encoding = "utf-8").decode("utf-8")
            element.clear()
            
            self.__iteration_count += 1

            return raw_content
        except etree.XMLSyntaxError as e:
            # if there is an invalid Token OR invalid entity AND file wasn't cleaned yet
            if not self.__file_was_cleaned: 
                clear_file(self.__file_path) # Removes invalid characters from file
                self.__file_was_cleaned = True
                self.__xml_parser = etree.iterparse(self.__file_path, tag = self.root) # Restart iterator
                # Jump elements self.__iteration_count times
                for i in range(self.__iteration_count):
                    next(self.__xml_parser)

                return self.get_raw_content()
            else:
                raise FeedParseException("El XML no se pudo reparar. " + type(e).__name__ + str(e))


    def __load_map_methods(self):
        """ Loads in memory all map methods and it params"""
  
        # @WARNING: Para que funcione ésta query, es necesario que esté 
        # desactivada la opción ONLY_FULL_GROUP_BY de mysql activada como default a partir de la versión 5.7.5
        # Información técnica: https://dev.mysql.com/doc/refman/5.7/en/sql-mode.html#sqlmode_only_full_group_by
        # Solución: https://www.sitepoint.com/quick-tip-how-to-permanently-change-sql-mode-in-mysql/
        methods = self.db_session.query(
                    FeedTypeMapping.method, 
                    func.group_concat(FeedTypeMapping.field.op('ORDER BY')(FeedTypeMapping.param_order)),
                    func.group_concat(FeedTypeMapping.default_value.op('ORDER BY')(FeedTypeMapping.param_order))                    
                ).filter(FeedTypeMapping.feed_type == self.feed_type, FeedTypeMapping.method.in_(MAP_METHODS.keys())).group_by(FeedTypeMapping.method).all()

        for method_name, xpaths, default_values in methods:
            addional_params = dict([(param.name, param.value) for param in self.feed_type.additional_params.filter_by(method = method_name)])
            
            map_method = MAP_METHODS[method_name](**addional_params)

            # With the below line we get
            # {"DESCRIPCION": (DescriptionMapMethod(template = "..."), [content/text(), bathrooms/text()])}
            self.map_methods[method_name] = (map_method, xpaths.split(","), default_values.split(","))

        return self

    def map(self, raw_ad):

        if not self.map_methods:
            self.__load_map_methods()

        xml = etree.fromstring(raw_ad.raw_content)

        mapped_properties = {}
        temp_ad = TempAd()
        
        for method_name, (map_method, xpaths, default_values) in self.map_methods.items():
            args = [self.extract(xml, xpath) or default_value for xpath, default_value in zip(xpaths, default_values)]
            
            mapped_properties.update(map_method.map(*args, raw_ad = raw_ad))

        temp_ad.set_properties(mapped_properties)

        return temp_ad

    def exec_method(self, method_name, raw_ad):
        if not self.map_methods:
            self.__load_map_methods()

        xml = etree.fromstring(raw_ad.raw_content)
        map_method = self.map_methods[method_name][0]
        xpaths = self.map_methods[method_name][1]

        args = [self.extract(xml, xpath) for xpath in xpaths]
        
        return map_method.map(*args, raw_ad = raw_ad)

    def extract(self, xml, xpath):
        """ Extract data from xml based on it xpath """
        data = xml.xpath(xpath)
        if len(data) == 1:
            data = data[0].strip()
        elif len(data) == 0:
            data = ""
        
        return data