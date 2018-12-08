# Import Built-Ins
import logging
import json
import threading
import time

# Import Third-Party
from websocket import create_connection, WebSocketTimeoutException,WebSocketConnectionClosedException
import requests
# Import Homebrew
from bitex.api.WSS.base import WSSAPI
from datetime import datetime
# Init Logging Facilities
log = logging.getLogger(__name__)


class BitmexWSS(WSSAPI):
    def __init__(self,pair="XBTUSD"):
        super(BitmexWSS, self).__init__('wss://www.bitmex.com/realtime', 'Bitmex')
        self.conn = None

        self.pairs = [pair.upper()]
        self._data_thread = None

    def start(self):
        super(BitmexWSS, self).start()

        self._data_thread = threading.Thread(target=self._process_data)
        self._data_thread.daemon = True
        self._data_thread.start()

    def stop(self):
        if self.running:
            super(BitmexWSS, self).stop()

            if self._data_thread:
                self._data_thread.join()
                self._data_thread = None

    def _process_data(self):
        self.conn = create_connection(self.addr)
        payload = json.dumps({"op": "subscribe", "args": ['trade:'+self.pairs[0]]})
        self.conn.send(payload)
        while self.running:
            try:
                data = json.loads(self.conn.recv())
                log.debug(data)
            except (WebSocketTimeoutException, ConnectionResetError,WebSocketConnectionClosedException):
                log.warning("restarted")
                self._controller_q.put('restart')
                time.sleep(3)

            # {'table': 'trade', 'action': 'insert', 'data': [
            #     {'timestamp': '2018-12-04T03:26:49.976Z', 'symbol': 'XBTUSD', 'side': 'Buy', 'size': 28999,
            #      'price': 3826.5, 'tickDirection': 'PlusTick', 'trdMatchID': '7d5089d5-486b-37cf-9b4c-0366e76f1ffc',
            #      'grossValue': 757859866, 'homeNotional': 7.57859866, 'foreignNotional': 28999},
            #     {'timestamp': '2018-12-04T03:26:49.976Z', 'symbol': 'XBTUSD', 'side': 'Buy', 'size': 7500,
            #      'price': 3826.5, 'tickDirection': 'ZeroPlusTick', 'trdMatchID': '3a374212-dc3c-4b2b-eb3b-10fc270cfb7a',
            #      'grossValue': 196005000, 'homeNotional': 1.96005, 'foreignNotional': 7500},
            #     {'timestamp': '2018-12-04T03:26:49.976Z', 'symbol': 'XBTUSD', 'side': 'Buy', 'size': 40,
            #      'price': 3826.5, 'tickDirection': 'ZeroPlusTick', 'trdMatchID': 'a586232f-e634-bb4a-db90-4af1f73481c1',
            #      'grossValue': 1045360, 'homeNotional': 0.0104536, 'foreignNotional': 40},
            #     {'timestamp': '2018-12-04T03:26:49.976Z', 'symbol': 'XBTUSD', 'side': 'Buy', 'size': 40,
            #      'price': 3826.5, 'tickDirection': 'ZeroPlusTick', 'trdMatchID': 'f93e0576-5cd2-35a5-0bf6-50d5361f01db',
            #      'grossValue': 1045360, 'homeNotional': 0.0104536, 'foreignNotional': 40},
            #     {'timestamp': '2018-12-04T03:26:49.976Z', 'symbol': 'XBTUSD', 'side': 'Buy', 'size': 10000,
            #      'price': 3826.5, 'tickDirection': 'ZeroPlusTick', 'trdMatchID': '74c8bddb-d264-0fec-ae24-1bbe11263cae',
            #      'grossValue': 261340000, 'homeNotional': 2.6134, 'foreignNotional': 10000},
            #     {'timestamp': '2018-12-04T03:26:49.976Z', 'symbol': 'XBTUSD', 'side': 'Buy', 'size': 36000,
            #      'price': 3826.5, 'tickDirection': 'ZeroPlusTick', 'trdMatchID': '6395d9a1-64cb-4ad4-4710-a60d0db8d770',
            #      'grossValue': 940824000, 'homeNotional': 9.40824, 'foreignNotional': 36000},
            #     {'timestamp': '2018-12-04T03:26:49.976Z', 'symbol': 'XBTUSD', 'side': 'Buy', 'size': 10000,
            #      'price': 3826.5, 'tickDirection': 'ZeroPlusTick', 'trdMatchID': '8816e23f-0d6d-5993-40a8-8aa3f331b806',
            #      'grossValue': 261340000, 'homeNotional': 2.6134, 'foreignNotional': 10000},
            #     {'timestamp': '2018-12-04T03:26:49.976Z', 'symbol': 'XBTUSD', 'side': 'Buy', 'size': 5000,
            #      'price': 3826.5, 'tickDirection': 'ZeroPlusTick', 'trdMatchID': '263cb99d-983d-abc4-05e0-1b337a700495',
            #      'grossValue': 130670000, 'homeNotional': 1.3067, 'foreignNotional': 5000},
            #     {'timestamp': '2018-12-04T03:26:49.976Z', 'symbol': 'XBTUSD', 'side': 'Buy', 'size': 30336,
            #      'price': 3826.5, 'tickDirection': 'ZeroPlusTick', 'trdMatchID': 'a7bf787b-14ad-4778-e3c2-c58a7928a6fe',
            #      'grossValue': 792801024, 'homeNotional': 7.92801024, 'foreignNotional': 30336}]}

            if 'table' in data:
                type = data['table']
                # reason = data['reason']

                if type == 'trade':
                    tradedatas = data['data']
                    for tradedata in tradedatas:
                        product_id = tradedata['symbol']
                        # log.info(product_id)
                        if product_id == self.pairs[0]:
                            log.debug(tradedata)
                            amount = float(tradedata['size'])
                            if tradedata['side'] == "Sell":
                                amount = -amount

                            date_str = (tradedata['timestamp'])
                            # //2018-12-03T14:38:33.665000Z
                            ts = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S.%fZ')
                            timestamp = (ts - datetime(1970, 1, 1)).total_seconds()

                            # print("ts %s" % timestamp)
                            self.data_q.put(('trades',
                                             timestamp, amount, float(tradedata['price']),))

        self.conn = None