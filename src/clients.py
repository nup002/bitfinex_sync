import json
import hmac
import hashlib
import time
import requests
import base64
import time

class BitfinexError(Exception):
    pass


class BaseClient(object):
    """
    A base class for the API Client methods that handles interaction with
    the requests library.
    """
    #api_url = 'https://bf1.apiary-mock.com/'
    api_url = 'https://api.bitfinex.com/'
    exception_on_error = True
    lastcall = time.time()
    
    def __init__(self, proxydict=None, *args, **kwargs):
        self.proxydict = proxydict

    def _get(self, *args, **kwargs):
        """
        Make a GET request.
        """
        return self._request(requests.get, *args, **kwargs)

    def _post(self, *args, **kwargs):
        """
        Make a POST request.
        """
        data = self._default_data()
        data.update(kwargs.get('data') or {})
        kwargs['data'] = data
        return self._request(requests.post, *args, **kwargs)

    def _default_data(self):
        """
        Default data for a POST request.
        """
        return {}

    def _request(self, func, url, *args, **kwargs):
        """
        Make a generic request, adding in any proxy defined by the instance.

        Raises a ``requests.HTTPError`` if the response status isn't 200, and
        raises a :class:`BitfinexError` if the response contains a json encoded
        error message.
        
        A timer has been implemented to limit the time between api calls to 1.5 seconds.
        """
        retry = False
        timeout = False
        return_json = kwargs.pop('return_json', False)
        while True:
            if retry:
                print("Retrying API call.")
            nowtime = time.time()
            while self.lastcall + 1.5 > nowtime:
                nowtime = time.time()
                time.sleep(0.1)
                
            fullurl = self.api_url + url
            self.lastcall = time.time()
            try:
                response = func(fullurl, timeout=5, *args, **kwargs)
                timeout=False
            except  requests.exceptions.Timeout:
                print("\nTimeout!")
                timeout=True
                retry=True
                 
            if not timeout:
                if retry:
                    print('Response Code: ' + str(response.status_code))
                    print('Response Header: ' + str(response.headers))
                if 'proxies' not in kwargs:
                    kwargs['proxies'] = self.proxydict
                    
                
                # Check for error, raising an exception if appropriate.
                # If the error code is 429, it is a ddos protection, i.e. too many requests in too short time.
                # In that case we sleep 5sec and try the request again.
                if  response.status_code == 429:
                    retryAfter = int(response.headers['Retry-After'])
                    print("\nDDoS protection. Waiting {} seconds...".format(retryAfter))
                    time.sleep(retryAfter)
                    retry = True
                else:
                    #print("Request status code: {}".format(response.status_code))
                    response.raise_for_status()
                    break
        try:
            json_response = response.json()
        except ValueError:
            json_response = None
        if isinstance(json_response, dict):
            error = json_response.get('error')
            if error:
                raise BitfinexError(error)

        if return_json:
            if json_response is None:
                raise BitfinexError(
                    "Could not decode json for: " + response.text)
            return json_response

        return response


class BitfinexPublic(BaseClient):

    def ticker(self):
        """
        Returns dictionary. 
        
        mid (price): (bid + ask) / 2
        bid (price): Innermost bid.
        ask (price): Innermost ask.
        last_price (price) The price at which the last order executed.
        low (price): Lowest trade price of the last 24 hours
        high (price): Highest trade price of the last 24 hours
        volume (price): Trading volume of the last 24 hours
        timestamp (time) The timestamp at which this information was valid.
        
        """
        return self._get("v2/ticker/tBTCUSD", return_json=True)
    
    def get_last(self):
        """shortcut for last trade"""
        return float(self.ticker()['last_price'])
    
    def get_candlesticks(self, timeframe, symbol, section, **kwargs):
        """
        Returns candlesticks.
        See the bitfinex documentation: https://bitfinex.readme.io/v2/reference#rest-public-candles
        timeframe:  Available values: '1m', '5m', '15m', '30m', '1h', '3h', '6h', '12h', '1D', '7D', '14D', '1M'
        symbol:     The symbol you want information about. 
        section:    Available values: "last", "hist"
            
        MTS     int     millisecond time stamp
        OPEN    float   First execution during the time frame
        CLOSE   float   Last execution during the time frame
        HIGH    float   Highest execution during the time frame
        LOW     float   Lowest execution during the timeframe
        VOLUME  float   Quantity of symbol traded within the timeframe
        """
        timeframe_valids= ['1m', '5m', '15m', '30m', '1h', '3h', '6h', '12h', '1D', '7D', '14D', '1M']
        params = ""
        if kwargs:
            params = "?"
            for key, value in kwargs.items():
                if key=='timeframe':
                    if value not in timeframe_valids:
                        raise RuntimeError("{} is not a valid candlestick timeframe. Valid values are: '1m', '5m', '15m', '30m', '1h', '3h', '6h', '12h', '1D', '7D', '14D', '1M'".format(value))
                params += key + '=' + str(value) + "&"
            params = params[:-1]
            
        result = self._get("v2/candles/trade:{}:{}/{}{}".format(timeframe, symbol, section, params), return_json=True)
        
        if len(result)>0:
            if isinstance(result[0], list):
                for n in range(len(result)):
                    result[n][0] = result[n][0]/1000
            else:
                result[0] = result[0]/1000
                result = [result]
        return result
        
        
class BitfinexTrading(BitfinexPublic):

    def __init__(self, key, secret, *args, **kwargs):
        """
        Stores the username, key, and secret which is used when making POST
        requests to Bitfinex.
        """
        super(Trading, self).__init__(
                 key=key, secret=secret, *args, **kwargs)
        self.key = key
        self.secret = secret

    def _get_nonce(self):
        """
        Get a unique nonce for the bitfinex API.

        This integer must always be increasing, so use the current unix time.
        Every time this variable is requested, it automatically increments to
        allow for more than one API request per second.

        This isn't a thread-safe function however, so you should only rely on a
        single thread if you have a high level of concurrent API requests in
        your application.
        """
        nonce = getattr(self, '_nonce', 0)
        if nonce:
            nonce += 1
        # If the unix time is greater though, use that instead (helps low
        # concurrency multi-threaded apps always call with the largest nonce).
        self._nonce = max(int(time.time()), nonce)
        return self._nonce

    def _default_data(self, *args, **kwargs):
        """
        Generate a one-time signature and other data required to send a secure
        POST request to the Bitfinex API.
        """
        data = {}
        nonce = self._get_nonce()
        data['nonce'] = str(nonce)
        data['request'] = args[0]
        return data

    def _post(self, *args, **kwargs):
        """
        Make a POST request.
        """
        data = kwargs.pop('data', {})
        data.update(self._default_data(*args, **kwargs))
        
        key = self.key
        secret = self.secret
        payload_json = json.dumps(data)
        payload = base64.b64encode(payload_json)
        sig = hmac.new(secret, payload, hashlib.sha384)
        sig = sig.hexdigest()

        headers = {
           'X-BFX-APIKEY' : key,
           'X-BFX-PAYLOAD' : payload,
           'X-BFX-SIGNATURE' : sig
           }
        kwargs['headers'] = headers
        
        #print("headers: " + json.dumps(headers))
        #print("sig: " + sig)
        #print("api_secret: " + secret)
        #print("api_key: " + key)
        #print("payload_json: " + payload_json)
        return self._request(requests.post, *args, **kwargs)

    def account_infos(self):
        """
        Returns dictionary::
        [{"fees":[{"pairs":"BTC","maker_fees":"0.1","taker_fees":"0.2"},
        {"pairs":"LTC","maker_fees":"0.0","taker_fees":"0.1"},
        {"pairs":"DRK","maker_fees":"0.0","taker_fees":"0.1"}]}]
        """
        return self._post("/v2/account_infos", return_json=True)
    
    def balances(self):
        """
        returns a list of balances
        A list of wallet balances:
        type (string): "trading", "deposit" or "exchange".
        currency (string): Currency 
        amount (decimal): How much balance of this currency in this wallet
        available (decimal): How much X there is in this wallet that 
        is available to trade.
        """
        return self._post("/v2/balances",return_json=True)
    
    def new_order(self, amount=0.01, price=1.11, side='buy',
                  order_type='limit', symbol='btcusd'):
        """
        enters a new order onto the orderbook
        
        symbol (string): The name of the symbol (see `/symbols`).
        amount (decimal): Order size: how much to buy or sell.
        price (price): Price to buy or sell at. May omit if a market order.
        exchange (string): "bitfinex".
        side (string): Either "buy" or "sell".
        type (string): Either "market" / "limit" / "stop" / "trailing-stop" / "fill-or-kill" / "exchange market" / "exchange limit" / "exchange stop" / "exchange trailing-stop" / "exchange fill-or-kill". (type starting by "exchange " are exchange orders, others are margin trading orders) 
        is_hidden (bool) true if the order should be hidden. Default is false.
        Response
        
        order_id (int): A randomly generated ID for the order.
        and the information given by /order/status"""
        data = {'symbol': str(symbol),
                'amount': str(amount),
                'price': str(price),
                'exchange': 'bitfinex',
                'side':str(side),
                'type':order_type
                }
        return self._post("/v2/order/new", data=data, return_json=True)

    def orders(self):
        """
        Returns an array of the results of `/order/status` for all
        your live orders.
        """
        return self._post("/v2/orders", return_json=True)

    def cancel_order(self, order_id):
        """
        cancels order with order_id
        """
        data = {'order_id': str(order_id)}
        return self._post("/v2/order/cancel",data, return_json=True)
    
    def cancel_all_orders(self):
        """
        cancels all orders
        """
        req = self._post('/v2/order/cancel/all', return_json=False)
        if req.content == "All orders cancelled":
            return True
        else:
            return False
        
    def positions(self):
        """
        gets positions
        """
        return self._post("/v2/positions", return_json=True)



