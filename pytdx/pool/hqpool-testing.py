# utf-8
from pytdx.log import DEBUG, log
from functools import partial
import time


## 调用单个接口，重试次数，超过次数则不再重试
DEFAULT_API_CALL_MAX_RETRY_TIMES = 20
## 重试间隔的休眠时间
DEFAULT_API_RETRY_INTERVAL = 0.2

class TdxHqApiCallMaxRetryTimesReachedException(Exception):
    pass

class TdxHqPool_API(object):
    """
    实现一个连接池的机制
    包含：

    1 1个正在进行数据通信的主连接
    2 1个备选连接，备选连接也连接到服务器，通过心跳包维持连接，当主连接通讯出现问题时，备选连接立刻转化为主连接, 原来的主连接返回ip池，并从ip池中选取新的备选连接
    3 m个ip构成的ip池，可以通过某个方法获取列表，列表可以进行排序，如果备选连接缺少的时候，我们根据排序的优先级顺序将其追加到备选连接
    """

    def __init__(self, hq_cls, ippool):
        self.hq_cls = hq_cls
        self.ippool = ippool
        """
        正在通信的客户端连接
        """
        self.api = hq_cls(multithread=True, heartbeat=True)
        """
        备选连接
        """
        self.hot_failover_api = hq_cls(multithread=True, heartbeat=True)

        self.api_call_max_retry_times = DEFAULT_API_CALL_MAX_RETRY_TIMES
        self.api_call_retry_times = 0
        self.api_retry_interval = DEFAULT_API_RETRY_INTERVAL


        # 对hq_cls 里面的get_系列函数进行反射
        log.debug("perform_reflect")
        self.perform_reflect(self.api)

    def perform_reflect(self, api_obj):
        # ref : https://stackoverflow.com/questions/34439/finding-what-methods-an-object-has
        method_names = [attr for attr in dir(api_obj) if callable(getattr(api_obj, attr))]
        for method_name in method_names:
            log.debug("testing attr %s" % method_name)
            if method_name[:3] == 'get' or method_name == "do_heartbeat" or method_name == 'to_df':
                log.debug("set refletion to method: %s", method_name)
                _do_hp_api_call = partial(self.do_hq_api_call, method_name)
                setattr(self, method_name, _do_hp_api_call)

    def do_hq_api_call(self, method_name, *args, **kwargs):
        """
        代理发送请求到实际的客户端
        :param method_name: 调用的方法名称
        :param args: 参数
        :param kwargs: kv参数
        :return: 调用结果
        """
        try:
            result = getattr(self.api, method_name)(*args, **kwargs)
            if result is None:
                log.info("api(%s) call return None" % (method_name,))
        except Exception as e:
            log.info("api(%s) call failed, Exception is %s" % (method_name, str(e)))
            result = None

        # 如果无法获取信息，则进行重试
        if result is None:
            if self.api_call_retry_times >= self.api_call_max_retry_times:
                log.info("(method_name=%s) max retry times(%d) reached" % (method_name, self.api_call_max_retry_times))
                raise TdxHqApiCallMaxRetryTimesReachedException("(method_name=%s) max retry times reached" % method_name)
            old_api_ip = self.api.ip
            new_api_ip = None
            if self.hot_failover_api:
                new_api_ip = self.hot_failover_api.ip
                log.info("api call from init client (ip=%s) err, perform rotate to (ip =%s)..." %(old_api_ip, new_api_ip))
                self.api.disconnect()
                self.api = self.hot_failover_api
            log.info("retry times is " + str(self.api_call_max_retry_times))
            # 从池里再次获取备用ip
            new_ips = self.ippool.get_ips()

            choise_ip = None
            for _test_ip in new_ips:
                if _test_ip[0] == old_api_ip or _test_ip[0] == new_api_ip:
                    continue
                choise_ip = _test_ip
                break

            if choise_ip:
                self.hot_failover_api = self.hq_cls(multithread=True, heartbeat=True)
                self.hot_failover_api.connect(*choise_ip)
            else:
                self.hot_failover_api = None
            # 阻塞0.2秒，然后递归调用自己
            time.sleep(self.api_retry_interval)
            result = self.do_hq_api_call(method_name, *args, **kwargs)
            self.api_call_retry_times += 1

        else:
            self.api_call_retry_times = 0

        return result

    def connect(self, ipandport, hot_failover_ipandport):
        log.debug("setup ip pool")
        self.ippool.setup()
        log.debug("connecting to primary api")
        self.api.connect(*ipandport)
        log.debug("connecting to hot backup api")
        self.hot_failover_api.connect(*hot_failover_ipandport)
        return self

    def disconnect(self):
        log.debug("primary api disconnected")
        self.api.disconnect()
        log.debug("hot backup api  disconnected")
        self.hot_failover_api.disconnect()
        log.debug("ip pool released")
        self.ippool.teardown()

    def close(self):
        """
        disconnect的别名，为了支持 with closing(obj): 语法
        :return:
        """
        self.disconnect()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


if __name__ == '__main__':
    from pytdx.config.hosts import hq_hosts
    import random
    from pytdx.pool.ippool import AvailableIPPool
    from pytdx.hq import TdxHq_API,TDXParams
    import pandas as pd

    def get_tdx_endpoint(available_ip_count=10):
        ips = [(v[1], v[2]) for v in hq_hosts]
        # 获取5个随机ip作为ip池
        random.shuffle(ips)
        ips5 = ips[:available_ip_count]

        ## IP 池对象
        ippool_ = AvailableIPPool(TdxHq_API, ips5)

        ## 选出M, H
        ips = ippool_.sync_get_top_n(available_ip_count)
        ippool_.ips=ips
        return ippool_

    ippool = get_tdx_endpoint()

    api = TdxHqPool_API(TdxHq_API, ippool)
    ip_port1=ippool.ips.pop()
    ip_port2 = ippool.ips.pop()
    with api.connect(ip_port1,ip_port2):
        ret = []
        index = 0
        # while data := api.get_history_transaction_data(TDXParams.MARKET_SH, '110060', 0, 1888, 20230615):
        while data := api.get_minute_time_data(TDXParams.MARKET_SH, '600300'):
            index += 1
            ret.extend(data)
            if len(data) != 1888:
                break
    ret_df = pd.DataFrame(ret)
    print(ret_df)

