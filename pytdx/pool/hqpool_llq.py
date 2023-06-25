# utf-8
from pytdx.log import log
from functools import partial

## 调用单个接口，重试次数，超过次数则不再重试
DEFAULT_API_CALL_MAX_RETRY_TIMES = 2
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
            raise e

        return result

    def connect(self, ipandport):
        log.debug("setup ip pool")
        self.ippool.setup()
        log.debug("connecting to primary api")
        self.api.connect(*ipandport)
        return self

    def disconnect(self):
        log.debug("primary api disconnected")
        self.api.disconnect()
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
    from pytdx.hq import TdxHq_API
    from pytdx.pool.ippool import AvailableIPPool
    from pytdx.config.hosts import hq_hosts
    import random
    import logging
    import pprint

    log.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    # create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    # add formatter to ch
    ch.setFormatter(formatter)
    log.addHandler(ch)

    ips = [(v[1], v[2]) for v in hq_hosts]

    # 获取5个随机ip作为ip池
    random.shuffle(ips)
    ips5 = ips[:5]

    ippool = AvailableIPPool(TdxHq_API, ips5)

    primary_ip = ippool.sync_get_top_n(1)

    print("make pool api")
    api = TdxHqPool_API(TdxHq_API, ippool)
    print("make pool api done")
    print("send api call to primary ip %s " % (str(primary_ip)))
    with api.connect(primary_ip):
        ret = api.get_xdxr_info(0, '000001')
        print("send api call done")
        pprint.pprint(ret)
