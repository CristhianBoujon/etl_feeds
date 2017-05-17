from lxml import etree
from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound
from sqlalchemy.orm.session import object_session
from sqlalchemy.sql.expression import func
from tools.cleaner import slugify
from db import DBSession, Session
from feeds_model import *
import os

class FeedMappingException(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message


class MapMethod:
    def __init__(self, feed_type = None, **kwargs):
        self.feed_type = feed_type
        self.additional_args = kwargs

    def map(self, *args):
        raise NotImplementedError("La función map() no está implementada para " + type(self).__name__)

class DescripcionMapMethod(MapMethod):
    def map(self, *args):
        try:
            return {"addesc": self.additional_args["template"].format(*args)}
        except KeyError:
            msg = "El parámetro 'template' no se encuentra definido para " + self.feed_type.id
            raise FeedMappingException( msg )

class TitleMapMethod(MapMethod):
    def map(self, *args):
        return {"adtitle": ", ".join(args)}

class UrlMapMethod(MapMethod):
    def map(self, *args):
        return {'url': args[0]}

class LocationMapMethod(MapMethod):
    def map(self, *args):

        slug_location_name = ",".join([slugify(arg, separator = "-") for arg in args if arg])
        session = Session()
        query = session.query(FeedInLocation).filter_by(slug_location_name = slug_location_name)
        
        try:
            feed_in_location = query.one()
            result = self.__result(feed_in_location)
        except NoResultFound:            
            feed_in_location = FeedInLocation(slug_location_name = slug_location_name)
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
            "countryid": feed_in_location.country_id, 
            "stateid": feed_in_location.state_id, 
            "location_name": feed_in_location.location_name,
            "feed_in_location_id": feed_in_location.id # If feed_in_location is pending yet we return the FeedInLocation instance 
                                                                # sinse we will update ad later when moderator fill the location data 
        }

class SubCategoryMapMethod(MapMethod):
    def map(self, *args):
        slug_subcat_name = ",".join([slugify(arg) for arg in args if arg])
        session = Session()
        query = session.query(FeedInSubcategory).filter_by(slug_subcat_name = slug_subcat_name)
        
        try:
            feed_in_subcat = query.one()
            result = self.__result(feed_in_subcat)
        except NoResultFound:
            feed_in_subcat = FeedInSubcategory(slug_subcat_name = slug_subcat_name)
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
            "feed_in_subcat_id": feed_in_subcat.id # If feed_in_subcats is pending yet we return the FeedInLocation instance 
                                                                # sinse we will update ad later when moderator fill the location data 
        }

class PriceMapMethod(MapMethod):
    def map(self, *args):
        if type(args[0]) == str and args[0] == "":
            price = 0  
        elif type(args[0]) == str:
            price = float(args[0].replace(",", "."))
        else:
            price = float(args[0])

        return {"price": price}


class CurrencyMapMethod(MapMethod):
    def map(self, *args):
        return {"currency": args[0]}

class IdMapMethod(MapMethod):
    def map(self, *args):
        return {"site_id": args[0]}

MAP_METHODS = {
    'DESCRIPCION': DescripcionMapMethod,
    'CATEGORIA': SubCategoryMapMethod,
    'ID': IdMapMethod,
    'MONEDA': CurrencyMapMethod,
    'PRECIO': PriceMapMethod,
    'TITULO': TitleMapMethod,
    'UBICACION': LocationMapMethod,
    'URL': UrlMapMethod    
}


class XmlAdMapper:

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
        xml_parser = etree.iterparse(file, tag = self.root)

    def __load_methods(self):
        """ Loads in memory all map methods and it params"""


        
        # @WARNING: Para que funcione ésta query, es necesario que esté 
        # desactivada la opción ONLY_FULL_GROUP_BY de mysql activada como default a partir de la versión 5.7.5
        # Información técnica: https://dev.mysql.com/doc/refman/5.7/en/sql-mode.html#sqlmode_only_full_group_by
        # Solución: https://www.sitepoint.com/quick-tip-how-to-permanently-change-sql-mode-in-mysql/
        methods = self.db_session.query(
                    FeedTypeMapping.method, 
                    func.group_concat(FeedTypeMapping.field.op('ORDER BY')(FeedTypeMapping.param_order))
                ).filter(FeedTypeMapping.feed_type == self.feed_type, FeedTypeMapping.field != self.root).group_by(FeedTypeMapping.method).all()

        for method_name, xpaths in methods:
            addional_params = dict([(param.name, param.value) for param in self.feed_type.additional_params.filter_by(method = method_name)])
            
            map_method = MAP_METHODS[method_name](feed_type = self.feed_type, **addional_params)

            # With the below line we get
            # {"DESCRIPCION": (DescriptionMapMethod(template = "..."), [content/text(), bathrooms/text()])}
            self.map_methods[method_name] = (map_method, xpaths.split(","))

        return self

    def map(self, str_xml):

        if not self.map_methods:
#            print(os.getpid(), DBSession(), self.feed_type, self.feed_type.id)
            self.__load_methods()
#        else:
#            print("--", os.getpid(), DBSession(), self.feed_type, self.feed_type.id)        

        xml = etree.fromstring(str_xml)

        mapped_properties = {}
        temp_ad = TempAd()
        
        for method_name, (map_method, xpaths) in self.map_methods.items():
            args = [self.extract(xml, xpath) for xpath in xpaths]
            
            mapped_properties.update(map_method.map(*args))

        temp_ad.set_properties(mapped_properties)

        return temp_ad

    def extract(self, xml, xpath):
        """ Extract data from xml based on it xpath """
        try:
            data = xml.xpath(xpath)[0].strip()
        except: # If xpath doesn't match elements
            data = ""
        return data

    def bulk_insert(self, file, feed_in):
        xml_parser = etree.iterparse(file, tag = self.root)
        file_was_cleaned = False # Flag determines if xml file was cleaned to avoid infite loops

        max_pending = 10000 # Max INSERTs pending to commit
        current_pending = 0   # count the number of ads processing from the xml
        skip_ads = 0 # Ads to skip in case that we need to "restart" the iterator
        inserted_ads = 0

        info = {'status': None, 'file': file, 'inserted': None, 'e_msg': None}
        pending_raw_ads = []
        while True:
            try:
                event, element = next(xml_parser)

                if skip_ads == 0:

                    pending_raw_ads.append(
                        {   "raw_ad": etree.tostring(element, encoding = "utf-8").decode("utf-8"),
                            "feed_in_id": feed_in.id
                        })

                    current_pending += 1

                    element.clear()
                    if( current_pending == max_pending):
                        self.db_session.execute(RawAd.__table__.insert(), pending_raw_ads)
                        self.db_session.commit()    
                        del pending_raw_ads[:] 
                        inserted_ads += current_pending
                        current_pending = 0

                elif skip_ads != 0:
                    skip_ads -= 1

            except StopIteration:
                if(current_pending != 0):
                    self.db_session.execute(RawAd.__table__.insert(), pending_raw_ads)
                    self.db_session.commit()
                    inserted_ads += current_pending
                    current_pending = 0

                info['status'] = 'ok'
                info['inserted'] = inserted_ads
                
                return info

            except etree.ParseError as e:
                # https://docs.python.org/3/library/xml.etree.elementtree.html#exceptions
                # if there is an invalid Token OR invalid entity AND file wasn't cleaned yet
                if (e.code == 4 or e.code == 11) and not file_was_cleaned: 
                    skip_ads = inserted_ads + current_pending
                    #current_pending = 0
                    cleaner.clear_file(file) # Removes invalid characters from file
                    xml_parser = etree.iterparse(file)
                else:
                    info['status'] = type(e).__name__
                    info['inserted'] = inserted_ads
                    info['e_msg'] = str(e)
                    return info
            except Exception as e:
                info['status'] = type(e).__name__
                info['inserted'] = inserted_ads
                info['e_msg'] = str(e)
                return info
