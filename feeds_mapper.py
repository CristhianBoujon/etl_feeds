from lxml import etree
from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound
from sqlalchemy.orm.session import object_session
from sqlalchemy.sql.expression import func
from slugify import slugify

from feeds_model import FeedTypeMapping

class FeedMappingException(Exception):
    def __init__(self, message):
        self.message = message

MAP_METHODS = {
    'DESCRIPCION': "map_content",
    'CATEGORIA': "map_category",
    'ID': "map_id",
    'MONEDA': "map_currency",
    'PRECIO': "map_price",
    'TITULO': "map_title",
    'UBICACION': "map_location",
    'URL': "map_url"    
}


class XmlFeedMapper:

    def __init__(self, feed_type):
        self.feed_type = feed_type

        try:
            self.root = feed_type.mapping.filter_by(method = "ROOT").one().field

        except NoResultFound:
            raise FeedMappingException("No se ha creado ROOT para el mapeo de {0}".format(feed_type.id))

        except MultipleResultsFound:
            raise FeedMappingException("Se ha creado más de un ROOT para el mapeo de {0}".format(feed_type.id))

    def iter_from_file(self, file):
        pass

    def map(self, str_xml):
        xml = etree.fromstring(str_xml)
        """
        @WARNING: Para que funcione ésta query, es necesario que esté 
        desactivada la opción ONLY_FULL_GROUP_BY de mysql activada como default a partir de la versión 5.7.5
        Información técnica: https://dev.mysql.com/doc/refman/5.7/en/sql-mode.html#sqlmode_only_full_group_by
        Solución: https://www.sitepoint.com/quick-tip-how-to-permanently-change-sql-mode-in-mysql/
        """
        
        methods = object_session(self.feed_type).query(
                    FeedTypeMapping.method, 
                    func.group_concat(FeedTypeMapping.field.op('ORDER BY')(FeedTypeMapping.param_order))
                ).filter(FeedTypeMapping.feed_type == self.feed_type, FeedTypeMapping.field != self.root).group_by(FeedTypeMapping.method).order_by(FeedTypeMapping.param_order).all()

        for method_name, xpaths in methods:
            self_method = MAP_METHODS[method_name]
            args = [self.extract(xml, xpath) for xpath in xpaths.split(",")]
            getattr(self, self_method)(*args)

    def extract(self, xml, xpath):
        try:
            data = xml.xpath(xpath)[0].strip()
        except: # If xpath doesn't match elements
            data = ""
        return data

    def map_content(self, *args):
        print(args)
    
    def map_category(self, *args):
        print(args)

    def map_id(self, *args):
        print(args)
    
    def map_currency(self, *args):
        print(args)
    
    def map_price(self, *args):
        print(args)
    
    def map_title(self, *args):
        print(args)
    
    def map_location(self, *args):

        print(",".join([slugify(arg) for arg in args]))

    def map_url(self, *args):
        print(args)
