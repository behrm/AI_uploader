# -*- coding: iso-8859-15 -*-
'''
Created on 20.03.2017

@author: Bernhard Ehrminger
'''
import io
import json
from logging import Formatter
import logging
import os

import pycurl


class AI_uploader(object):
    '''
    classdocs
    '''

    def __init__(self, environment, logger):
        '''
        Instanziieren des AI_uploader-Objekts

        Aufrufparameter:

        :param environment: zwischen den Umgebungen Entwicklung & Produktion umschalten
        :type environment: str
        :param logger:
        :type logger: logging.logger
        '''
        if environment is 'Entwicklung':
            self.url = 'https://integration.geodienste.ch/data_agg/interlis/import'
            self.username = '******'
            self.password = '******'
        elif environment is 'Produktion':
            self.url = 'https://geodienste.ch/data_agg/interlis/import'
            self.username = '******'
            self.password = '*****'
        else:
            raise Exception('Aufruf mit unbekanntem "environment: {0}'.format(environment))
        self.logger = logger
        return

    def __debug_print(self, debug_type, debug_msg):
        if len(debug_msg) < 300:
            self.logger.debug("({0}): {1}".format(debug_type, debug_msg.strip()))
        return

    def upload(self, interlis_topic, lv95_zip_file, publish=True, replace_all=True):
        '''
        Führt den Upload in die Aggregationsinfrastruktur aus

        :param interlis_topic: der Name des Interlis-Topics
        :type interlis_topic: str
        :param lv95_zip_file: Der Pfad zu der zu übertragenden Interlis-Zip-Datei
        :type lv95_zip_file: str
        :param publish: Publizieren True oder False
        :type publish: bool
        :param replace_all: Vor dem Upload selektives Entladen ausführen True oder False
        :type replace_all: bool
        :returns 'True' - der Upload war erfolgreich; 'False' - der Upload war nicht erfolgreich
        :rtype returnValue: bool
        '''

        if not os.path.exists(lv95_zip_file):
            self.logger.error('das Zipfile {0} exisitiert nicht'.format(self.url))
            raise Exception('das Zipfile {0} exisitiert nicht'.format(self.url))

        output = io.BytesIO()
        c = pycurl.Curl()

        if self.logger.level is logging.DEBUG:
            c.setopt(c.VERBOSE, True)
            c.setopt(c.DEBUGFUNCTION, self.__debug_print)

        c.setopt(c.USERPWD, '{0}:{1}'.format(self.username, self.password))

        c.setopt(c.URL, self.url)
        c.setopt(c.FOLLOWLOCATION, 1)
        c.setopt(c.WRITEFUNCTION, output.write)
        c.setopt(c.CONNECTTIMEOUT, 5)
        c.setopt(c.TIMEOUT, 200) # bei grossen Datensätzen (ab ca. 250Mbyte) könnte dies zuwenig sein!

        # unser Proxie terminiert TLS-Verschlüsselung
        # daher ist keine Sicherheits- resp. Zertifikatüberprüfung möglich
        c.setopt(c.SSL_VERIFYPEER, 0)
        c.setopt(c.SSL_VERIFYHOST, 0)

        c.setopt(c.HTTPPOST, [('topic', interlis_topic),
                              ('lv95_file', (c.FORM_FILE, lv95_zip_file)),
                              ('publish', 'true' if publish else 'false'),
                              ('replace_all', 'true' if replace_all else 'false')])
        try:
            c.perform()
            responseDict = json.loads(output.getvalue())  # json auspacken
        except Exception as e:
            self.logger.exception('pyCurl "Perform a file transfer" failed on {0}'.format(self.url))
            raise e

        if responseDict['success']:
            self.logger.info('\n\tSuccessfully uploaded File {0} \n\tto URL {1}\n\t{2}'.
                             format(lv95_zip_file,
                                    responseDict['url'],
                                    '\n\t'.join(responseDict['outputs'])))

            return True
        elif not responseDict['success']:
            self.logger.error('\n\tFailure uploading File {0} \n\tto URL {1}\n\t{2}'.
                              format(lv95_zip_file,
                                     self.url,
                                     '\n\t'.join(responseDict['exceptions'])))
            return False
        else:
            raise Exception('Sorry, should never run into)')
        return


if __name__ == '__main__':

    # Herstellen und konfiguerien eines Loggers
    logger = logging.getLogger('TEST')
    logger.setLevel('DEBUG')

    # Herstellen eines Formatters
    formater = Formatter('%(asctime)-20s | %(levelname)-10s |' +
                         ' %(name)s | %(message)s', "%d-%m-%Y %H:%M:%S")

    # Herstellen und konfigurieren eines Streamhandlers
    sh = logging.StreamHandler()
    sh.setFormatter(formater)

    # Fertigstellen des Loggers zur Ausgabe der Log-Meldungen auf der Console
    logger.addHandler(sh)

    # das hochzuladende Zip-Archiv
    zipArchiv = r'******\Testdaten_SG' + \
                os.sep + 'SG00107.zip'


    # Herstellen einer Instanz mit Angabe der Zielplatform
    # (Entwicklung oder Produktion) und des loggers
    uploader = AI_uploader(environment='Entwicklung', logger=logger)

    # Ausführen des Uploads
    uploader.upload(interlis_topic='kataster_belasteter_standorte',
                    lv95_zip_file=zipArchiv,
                    replace_all=True,
                    publish=True)
    pass
