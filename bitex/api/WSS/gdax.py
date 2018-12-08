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


class GDAXWSS(WSSAPI):
    def __init__(self):
        super(GDAXWSS, self).__init__('wss://ws-feed.gdax.com', 'GDAX')
        self.conn = None
        r = requests.get('https://api.gdax.com/products').json()
        self.pairs = [x['id'] for x in r]
        self._data_thread = None

    def start(self):
        super(GDAXWSS, self).start()

        self._data_thread = threading.Thread(target=self._process_data)
        self._data_thread.daemon = True
        self._data_thread.start()

    def stop(self):
        if self.running:
            super(GDAXWSS, self).stop()
            if self._data_thread:
                self._data_thread.join()
                self._data_thread = None

    def _process_data(self):
        self.conn = create_connection(self.addr)
        payload = json.dumps({'type': 'subscribe', 'product_ids': self.pairs})
        self.conn.send(payload)
        while self.running:
            try:
                data = json.loads(self.conn.recv())
            except (WebSocketTimeoutException, ConnectionResetError):
                log.warning("restarted")
                self._controller_q.put('restart')

            
            type=data['type']
            # reason = data['reason']

            if type=='match':
                product_id = data['product_id']
                # log.info(product_id) 
                if product_id=='BTC-USD':
                    log.debug(data)
                    amount = float(data['size'])
                    if data['side']=="sell":
                        amount = -amount

                    date_str = (data['time'])
                    # //2018-12-03T14:38:33.665000Z
                    ts = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S.%fZ')
                    timestamp = (ts - datetime(1970, 1, 1)).total_seconds()

                    # print("ts %s" % timestamp)
                    self.data_q.put(('trades',
                                     timestamp,amount,data['price'],))
                


            # if 'product_id' in data:
            #     self.data_q.put(('order_book', data['product_id'],
            #                      data, time.time()))
        self.conn = None