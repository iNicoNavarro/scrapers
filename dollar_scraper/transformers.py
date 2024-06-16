import re
import json
import requests
from datetime import datetime
from abc import ABC, abstractmethod
from helpers.utils import to_numeric


class Country(ABC):
    URL: str = None

    def __init__(
            self,
            date_source: datetime.date = None,
            value: float = None,
            unit: str = None,
            reference_currency: str = None,
            country: str = None,
            source: str = None
    ):
        self._date_source = date_source
        self._value = value
        self._unit = unit
        self._reference_currency = reference_currency
        self._country = country
        self._source = source

    @classmethod
    def request_info(cls, url: str = None):
        if url is None:
            url = cls.URL

        response = requests.get(url, verify=False)

        if response.status_code == 200:
            return response.text

    @staticmethod
    def get_content(text: str):
        from bs4 import BeautifulSoup

        content = BeautifulSoup(text, 'html.parser')
        return content

    @abstractmethod
    def transform(self):
        pass

    def to_dict(self):
        return {
            'source_date': self._date_source,
            'value': self._value,
            'unit': self._unit,
            'reference_currency': self._reference_currency,
            'country': self._country,
            'source': self._source
        }


class ColTRM(Country):
    URL = "https://www.datos.gov.co/resource/32sa-8pi3.json"

    def transform(self):
        data = json.loads(
            self.request_info()
        )[0]

        self._value = data['valor']
        self._date_source = data['vigenciadesde']
        self._unit = 'COP'
        self._reference_currency = 'USD'
        self._country = 'COL'
        self._source = 'Datos Abiertos Colombia'


class BraTRM(Country):
    URL = "https://www.bcb.gov.br/api/servico/sitebcb/indicadorCambio"

    def transform(self):
        data = json.loads(
            self.request_info()
        )['conteudo'][0]

        self._value = float(data['valorCompra'])
        self._date_source = data['dataIndicador']
        self._unit = 'BRL'
        self._reference_currency = 'USD'
        self._country = 'BRA'
        self._source = 'Banco Central do Brasil'


class PerTRM(Country):
    URL = "https://www.xe.com/es/currencyconverter/convert/?Amount=1&From=PEN&To=USD"
    
    def transform(self):
        content = self.get_content(
            self.request_info()
        )
        
        trm_current_pe = content.find_all(
            'div', class_='sc-ac62c6d1-0 GwlFu'
        )[0].text.split('=')[1].strip().split()[0]
        
        date_text = content.find_all('div', class_="sc-1c293993-2 cYzQtJ")[0].text
        date_text = re.search(r'\d+ \w+ \d{4}', date_text).group()
        date_source = datetime.strptime(date_text, '%d %b %Y').strftime('%Y-%m-%d')
        
        self._value = to_numeric(trm_current_pe)
        self._date_source = date_source
        self._unit = 'PEN'
        self._reference_currency = 'USD'
        self._country = 'PER'
        self._source = 'www.xe.com'


class MexTRM(Country):
    URL = "https://www.banxico.org.mx/tipcamb/llenarTiposCambioAction.do?idioma=sp&_=1693277260105"

    def transform(self):
        content = self.get_content(
            self.request_info()
        )

        # get date
        current_date = content.find("div", class_="renglonPar").text
        cleaned_date = current_date.strip().replace('\r\n', '').strip()
        date_type = datetime.strptime(cleaned_date, "%d/%m/%Y")

        # get dollar value
        rate_value = content.find_all("td", class_="renglonPar")[4]

        self._value = float(rate_value.find("div").text.strip().replace('\r\n', '').replace(' ', ''))
        self._date_source = date_type.strftime("%Y-%m-%d")
        self._unit = 'MXN'
        self._reference_currency = 'USD'
        self._country = 'MEX'
        self._source = 'Banco de México'
