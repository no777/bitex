# Import Built-Ins
import logging
import json
import threading
import time

# Import Third-Party
from websocket import create_connection, WebSocketTimeoutException
import requests
# Import Homebrew
from bitex.api.WSS.base import WSSAPI
from datetime import datetime
# Init Logging Facilities
log = logging.getLogger(__name__)


class BinanceWSS(WSSAPI):
    def __init__(self,pair='btcusdt'):
        super(BinanceWSS, self).__init__('wss://stream.binance.com:9443/ws/'+pair.lower()+'@trade', 'Binance')
        self.conn = None

        self.pairs = [pair.upper()]
        self._data_thread = None

    def start(self):
        super(BinanceWSS, self).start()

        self._data_thread = threading.Thread(target=self._process_data)
        self._data_thread.daemon = True
        self._data_thread.start()

    def stop(self):
        if self.running:
            super(BinanceWSS, self).stop()

            if self._data_thread:
                self._data_thread.join()
                self._data_thread = None

    def _process_data(self):
        self.conn = create_connection(self.addr)
        # payload = json.dumps({"op": "subscribe", "args": ['trade:XBTUSD']})
        # self.conn.send(payload)
        while self.running:
            # data = json.loads(self.conn.recv())
            # log.debug(data)
            try:

                data = json.loads(self.conn.recv())
                log.debug(data)
                # {'e': 'trade', 'E': 1543905785462, 's': 'BTCUSDT', 't': 84336794,
                #                        'p': '3948.60000000', 'q': '0.79434400', 'b': 199820805, 'a': 199820804,
                #                        'T': 1543905785459, 'm': False, 'M': True}

                if 'e' in data:
                    type = data['e']
                    # reason = data['reason']

                    if type == 'trade':
                        tradedata = data
                        product_id = tradedata['s']
                        # log.info(product_id)
                        if product_id == self.pairs[0].upper():
                            log.debug(tradedata)
                            amount = float(tradedata['q'])
                            if tradedata['m'] == True:
                                amount = -amount

                            timestamp =  float(tradedata['T'])

                            # print("ts %s" % timestamp)
                            self.data_q.put(('trades',
                                             timestamp, amount, float(tradedata['p']),))


            except (WebSocketTimeoutException, ConnectionResetError):
                log.warning("restarted")
                self._controller_q.put('restart')


        self.conn = None