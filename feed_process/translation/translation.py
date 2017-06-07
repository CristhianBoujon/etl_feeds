from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Float, TIMESTAMP, Unicode, UnicodeText, ForeignKey, Boolean
from sqlalchemy.orm import reconstructor, relationship
from sqlalchemy.orm.collections import attribute_mapped_collection
from feed_process.translation.db import DBSession

Base = declarative_base()

class Language(Base):
    __tablename__ = "language"

    id = Column(Integer, primary_key = True)
    locale = Column(String)

class LanguageToken(Base):
    __tablename__ = "language_token"

    id = Column(Integer, primary_key = True)
    token = Column(String)    

class LanguageTranslation(Base):
    __tablename__ = "language_translation"

    id = Column(Integer, primary_key = True)
    language_id = Column(Integer, ForeignKey("language.id"))
    language_token_id = Column("languageToken_id", Integer, ForeignKey("language_token.id"))
    catalog = Column("catalogue", String)
    translation = Column(String)

    lang = relationship("Language")
    token = relationship("LanguageToken")


class Translator:
    def translate(self, locale, token, catalog = "messages"):
        translated_token = DBSession.query(LanguageTranslation).\
                            join(LanguageTranslation.lang).\
                            join(LanguageTranslation.token).\
                            filter(
                                Language.locale == locale, 
                                LanguageToken.token == token,
                                LanguageTranslation.catalog == catalog).first()

        return translated_token.translation if translated_token else ""