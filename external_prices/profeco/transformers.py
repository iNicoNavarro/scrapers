import re
import requests


class Request:
    INPUTS = [
        '__VIEWSTATE',
        '__VIEWSTATEGENERATOR',
        '__EVENTVALIDATION'
    ]
    
    def __init__(self, session: requests.Session):
        self._session = session
        self._content = None
        
    @property
    def content(self):
        return self._content
    
    def execute(self, method, url, **kwargs):
        from bs4 import BeautifulSoup

        request = getattr(
            self._session,
            method
        )(
            url,
            **kwargs
        )

        if request.status_code == 200:
            self._content = BeautifulSoup(request.text, 'lxml')
            
            return self
        
        else:
            raise ValueError(request.content)
    
    def get_inputs(self, content=None, inputs: list = INPUTS) -> dict:
        if content is None:
            content = self._content
        
        return {
            item: content.find_all('input', id=item)[0]['value']
            for item in inputs
        }

    def get_options(self, content=None) -> dict:
        if content is None:
            content = self._content
        
        return {
            item['value']: item.text
            for item in content.find_all('option')[1:]
        }
    
    def get_items(self, content=None):
        if content is None:
            content = self._content
            
        items = {}

        for item in content.find_all('td', class_='textos_nodos'):
            a = item.find('a')

            if a is not None:
                code = re.search(
                    r'codigo=(\d+)', 
                    a['href']
                ).group(1)

                items[code] = a.text.strip(' ')

        return items
    
    def get_prices(self, content=None):
        if content is None:
            content = self._content
        
        prices = {}
        
        for item in content.find_all('tr', class_='textos_tablas'):
            tds = item.find_all('td')
            
            if tds is not None:
                prices[tds[-2].text.strip()] = float(tds[-1].text.replace('$', '').replace(',', ''))
        
        return prices


def compile_category_result(prices: dict, code: str, item: str):
    import pandas as pd

    return pd.Series(prices, name='price').to_frame().eval(
        f"""
        category_code = '{code}'
        category = '{item}'
        """
    ).reset_index().rename(
        columns={
            'index': 'name'
        }
    )


def run_all_items(items: dict, session: requests.Session):
    import pandas as pd

    city_results = []

    for code, item in items.items():
        prices = Request(session).execute(
            'get',
            f'https://www.profeco.gob.mx/precios/canasta/listaProdArbol.aspx?codigo={code}'
        ).get_prices()

        city_results.append(
            compile_category_result(prices, code, item)
        )
        
    if city_results:
        return pd.concat(
            city_results
        ).reset_index(
            drop=True
        )
    
    else:
        return pd.DataFrame(
            columns=[
                'category_code',
                'category',
                'name',
                'price'
            ]
        )


def set_session(city_code: str, inputs: dict, session: requests.Session):
    response_zone = Request(session).execute(
        'post',
        'https://www.profeco.gob.mx/precios/canasta/homer.aspx',
        data={
            'cmbCiudad': city_code,
            **inputs
        }
    )
    
    response_set = Request(session).execute(
        'post',
        'https://www.profeco.gob.mx/precios/canasta/homer.aspx',
        data={
            'cmbCiudad': city_code,
            'listaMunicipios': f'{city_code}0',
            'ImageButton1.x': 37,
            'ImageButton1.y': 14,
            '__EVENTTARGET': '',
            '__EVENTARGUMENT': '',
            '__LASTFOCUS': '',
            **response_zone.get_inputs()
        }
    )

    
def get_city_values(city: str, city_code: str, inputs: dict):
    session = requests.session()

    set_session(city_code, inputs, session)
    
    request_items = Request(session).execute(
        'get',
        'https://www.profeco.gob.mx/precios/canasta/arbol_frame.aspx',
        headers={
            'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/116.0',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Origin': 'https://www.profeco.gob.mx',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Referer': 'https://www.profeco.gob.mx/precios/canasta/homer.aspx',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'iframe',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1',
            'Pragma': 'no-cache',
            'Cache-Control': 'no-cache'
        }
    )
    
    return run_all_items(
        request_items.get_items(),
        session=session
    ).eval(
        f"""
        city = '{city}'
        city_code = '{city_code}'
        """
    ).set_index(
        [
            'city_code',
            'city'
        ]
    ).reset_index()


def get_cities() -> dict:
    response = Request(requests.session()).execute(
        'get',
        'https://www.profeco.gob.mx/precios/canasta/homer.aspx'
    )

    return {
        'cities': response.get_options(),
        'inputs': response.get_inputs()
    }
