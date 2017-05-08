from lxml import etree
from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound
from sqlalchemy.orm.session import object_session
from sqlalchemy.sql.expression import func
from slugify import slugify
from db import DBSession
from feeds_model import *


class FeedMappingException(Exception):
    def __init__(self, message):
        self.message = message


class MapMethod:
    def __init__(self, **kwargs):
        self.additional_args = kwargs

    def map(self, *args):
        raise NotImplementedError("La función map() no está implementada para " + self.class.__name__)

class DescripcionMapMethod(MapMethod):
    def map(self, *args):
        return {"content": self.additional_args["template"].format(*args)}

class TitleMapMethod(MapMethod):
    def map(self, *args):
        return {"adtitle": ", ".join(args)}

class UrlMapMethod(MapMethod):
    def map(self, *args):
        return args[0]

class LocationMapMethod(MapMethod):
    def map(self, *args):
        location_name = ",".join([slugify(arg) for arg in args])
        query = location_ids = DBSession.query(
                FeedInLocation.country_id,
                FeedInLocation.state_id,
                FeedInLocation.location_id, 
            ).filter_by(location_name = location_name)

        try:
            location_ids = query.one()

        except NoResultFound:
            self.db_session.add(FeedInLocation(location_name = location_name))
            location_ids = (None, None, None)

        except MultipleResultsFound:
            location_ids = query.first()

        # Set mapped location ids
        return dict(zip(("countryid", "stateid", "locationid"), location_ids))

# @TODO: Talk with Fernando
class SubCategoryMapMethod(MapMethod):
    def map(self, *args):
        pass

class PriceMapMethod(MapMethod):
    def map(self, *args):
        if type(args[0]) == str and args[0] == "":
            price = 0  
        elif type(args[0]) == str:
            price = float(args[0].replace(",", "."))
        else:
            price = float(args[0])

        return {"price": price}

MAP_METHODS = {
    'DESCRIPCION': "map_description",
    'CATEGORIA': "map_category",
    'ID': "map_id",
    'MONEDA': "map_currency",
    'PRECIO': "map_price",
    'TITULO': "map_title",
    'UBICACION': "map_location",
    'URL': "map_url"    
}


class XmlAdMapper:

    def __init__(self, feed_type):

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

    def map(self, str_xml):
        xml = etree.fromstring(str_xml)
        ad = dict.fromkeys([
            "locationid",
            "stateid",
            "countryid",
            "adtitle",
            "content"], None)

        """
        @WARNING: Para que funcione ésta query, es necesario que esté 
        desactivada la opción ONLY_FULL_GROUP_BY de mysql activada como default a partir de la versión 5.7.5
        Información técnica: https://dev.mysql.com/doc/refman/5.7/en/sql-mode.html#sqlmode_only_full_group_by
        Solución: https://www.sitepoint.com/quick-tip-how-to-permanently-change-sql-mode-in-mysql/
        """
        methods = self.db_session.query(
                    FeedTypeMapping.method, 
                    func.group_concat(FeedTypeMapping.field.op('ORDER BY')(FeedTypeMapping.param_order))
                ).filter(FeedTypeMapping.feed_type == self.feed_type, FeedTypeMapping.field != self.root).group_by(FeedTypeMapping.method).all()

        for method_name, xpaths in methods:
            self_method = MAP_METHODS[method_name]
            args = [self.extract(xml, xpath) for xpath in xpaths.split(",")]
            addional_params = dict([(param.name, param.value) for param in self.feed_type.additional_params.filter_by(method = method_name)])
            result = getattr(self, self_method)(*args, **addional_params)

            ad.update(result)

        self.db_session.commit()

        return ad

    def extract(self, xml, xpath):
        """ Extract data from xml based on it xpath """
        try:
            data = xml.xpath(xpath)[0].strip()
        except: # If xpath doesn't match elements
            data = ""
        return data

    def map_description(self, *args, **kwargs):
        return {"content": kwargs["template"].format(*args)}

    def map_category(self, *args, **kwargs):
        return {}

    def map_id(self, *args, **kwargs):
        return {}
    
    def map_currency(self, *args, **kwargs):
        return {}
    
    def map_price(self, *args, **kwargs):
        if type(args[0]) == str and args[0] == "":
            price = 0  
        elif type(args[0]) == str:
            price = float(args[0].replace(",", "."))
        else:
            price = float(args[0])

        return {"price": price}
    
    def map_title(self, *args, **kwargs):
        return {"adtitle": ", ".join(args)}
    
    def map_location(self, *args, **kwargs):
        location_name = ",".join([slugify(arg) for arg in args])
        query = location_ids = self.db_session.query(
                FeedInLocation.country_id,
                FeedInLocation.state_id,
                FeedInLocation.location_id, 
            ).filter_by(location_name = location_name)

        try:
            location_ids = query.one()

        except NoResultFound:
            self.db_session.add(FeedInLocation(location_name = location_name))
            location_ids = (None, None, None)

        except MultipleResultsFound:
            location_ids = query.first()

        # Set mapped location ids
        return dict(zip(("countryid", "stateid", "locationid"), location_ids))
        
        

    def map_url(self, *args, **kwargs):
        return {'url': args[0]}


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
                    current_pending = 0
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
