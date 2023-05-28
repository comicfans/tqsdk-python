#!/usr/bin/env python
#  -*- coding: utf-8 -*-
__author__ = 'chengzhi'

import asyncio
import time
from datetime import datetime
from asyncio import gather
from inspect import isfunction
from typing import Optional, Union, Callable

from tqsdk import utils
from tqsdk.api import TqApi
from tqsdk.backtest import TqBacktest
from tqsdk.channel import TqChan
from tqsdk.datetime import _is_in_trading_time, _timestamp_nano_to_str
from tqsdk.diff import _get_obj
from tqsdk.lib.utils import _check_volume_limit, _check_direction, _check_offset, _check_volume, _check_price, \
    _check_offset_priority
from tqsdk.tradeable import TqAccount, TqKq, TqSim
import structlog

class TargetPosTaskSingleton(type):
    """
    TargetPosTask 需要保证在每个账户下每个合约只有一个 TargetPosTask 实例。

    当用户多次调用时，应该保证对于同一账户同一合约使用相同的参数构造，否则抛错。

    在修改 TargetPosTask 构造参数时，同时应该修改 TargetPosTaskSingleton.__call__ 方法的参数，要确保其个数、名称、默认值和文档描述一致，\
    这些参数才是实际传给 TargetPosTask.__init__ 方法的参数。

    同时应该在 TargetPosTask 实例运行结束时释放相应的资源，_instances 需要情况对应的引用。
    """

    # key 为 id(account) + '#' + symbol， 值为 TargetPosTask 实例。
    _instances = {}

    def __call__(cls, api, symbol, price="ACTIVE", offset_priority="今昨,开", min_volume=None, max_volume=None,
                 trade_chan=None, trade_objs_chan=None, account: Optional[Union[TqAccount, TqKq, TqSim]]=None, *args, **kwargs):
        target_account = api._account._check_valid(account)
        if target_account is None:
            raise Exception(f"多账户模式下, 需要指定账户实例 account")
        key = api._account._get_account_key(target_account) + "#" + symbol
        if key not in TargetPosTaskSingleton._instances:
            TargetPosTaskSingleton._instances[key] = super(TargetPosTaskSingleton, cls).__call__(api, symbol, price,
                                                                                                 offset_priority,
                                                                                                 min_volume, max_volume,
                                                                                                 trade_chan,
                                                                                                 trade_objs_chan,
                                                                                                 target_account,
                                                                                                 *args, **kwargs)
        else:
            instance = TargetPosTaskSingleton._instances[key]
            if instance._offset_priority != offset_priority:
                raise Exception("您试图用不同的 offset_priority 参数创建两个 %s 调仓任务, offset_priority参数原为 %s, 现为 %s" % (
                    symbol, instance._offset_priority, offset_priority))
            if instance._price != price:
                raise Exception("您试图用不同的 price 参数创建两个 %s 调仓任务, price参数原为 %s, 现为 %s" % (symbol, instance._price, price))
            if instance._min_volume != min_volume:
                raise Exception(f"您试图用不同的 min_volume 参数创建两个 {symbol} 调仓任务, min_volume 参数原为 {instance._min_volume}, 现为 {min_volume}")
            if instance._max_volume != max_volume:
                raise Exception(f"您试图用不同的 max_volume 参数创建两个 {symbol} 调仓任务, max_volume 参数原为 {instance._max_volume}, 现为 {max_volume}")
        return TargetPosTaskSingleton._instances[key]


class TargetPosTask(object, metaclass=TargetPosTaskSingleton):
    """目标持仓 task, 该 task 可以将指定合约调整到目标头寸"""

    def __init__(self, api: TqApi, symbol: str, price: Union[str, Callable[[str], Union[float, int]]] = "ACTIVE",
                 offset_priority: str = "今昨,开", min_volume: Optional[int] = None, max_volume: Optional[int] = None,
                 trade_chan: Optional[TqChan] = None, trade_objs_chan: Optional[TqChan] = None,
                 account: Optional[Union[TqAccount, TqKq, TqSim]] = None) -> None:
        """
        创建目标持仓task实例，负责调整归属于该task的持仓 **(默认为整个账户的该合约净持仓)**.

        **注意:**
            1. TargetPosTask 在 set_target_volume 时并不下单或撤单, 它的下单和撤单动作, 是在之后的每次 wait_update 时执行的. 因此, **需保证 set_target_volume 后还会继续调用wait_update()** 。

            2. 请勿在使用 TargetPosTask 的同时使用 insert_order() 函数, 否则将导致 TargetPosTask 报错或错误下单。

            3. TargetPosTask 如果同时设置 min_volume（每笔最小下单手数），max_volume（每笔最大下单的手数）两个参数，表示采用 **大单拆分模式** 下单。

                在 **大单拆分模式** 下，每次下单的手数为随机生成的正整数，值介于 min_volume、max_volume 之间。

                具体说明：调用 set_target_volume 后，首先会根据目标持仓手数、开平仓顺序计算出，需要平今、平昨、开仓的目标下单手数及顺序。

                + 如果在调整持仓的目标下单手数小于 max_volume，则直接以目标下单手数下单。

                + 如果在调整持仓的目标下单手数大于等于 max_volume，则会以 min_volume、max_volume 之间的随机手数下一笔委托单，手数全部成交后，会接着处理剩余的手数；\
                继续以随机手数下一笔委托单，全部成交后，继续处理剩余的手数，直至剩余手数小于 max_volume 时，直接以剩余手数下单。

                当使用大单拆分模式下单时，必须同时填写 min_volume、max_volume，且需要满足 max_volume >= min_volume > 0。

        Args:
            api (TqApi): TqApi实例，该task依托于指定api下单/撤单

            symbol (str): 负责调整的合约代码

            price (str / Callable): [可选]下单方式, 默认为 "ACTIVE"。
                * "ACTIVE"：对价下单，在持仓调整过程中，若下单方向为买，对价为卖一价；若下单方向为卖，对价为买一价。
                * "PASSIVE"：排队价下单，在持仓调整过程中，若下单方向为买，对价为买一价；若下单方向为卖，对价为卖一价。
                * Callable[[str], Union[float, int]]: 函数参数为下单方向，函数返回值是下单价格。如果返回 nan，程序会抛错。

            offset_priority (str): [可选]开平仓顺序，昨=平昨仓，今=平今仓，开=开仓，逗号=等待之前操作完成

                                   对于下单指令区分平今/昨的交易所(如上期所)，按照今/昨仓的数量计算是否能平今/昨仓
                                   对于下单指令不区分平今/昨的交易所(如中金所)，按照“先平当日新开仓，再平历史仓”的规则计算是否能平今/昨仓，如果这些交易所设置为"昨开"在有当日新开仓和历史仓仓的情况下，会自动跳过平昨仓进入到下一步

                                   * "今昨,开" 表示先平今仓，再平昨仓，等待平仓完成后开仓，对于没有单向大边的品种避免了开仓保证金不足
                                   * "今昨开" 表示先平今仓，再平昨仓，并开仓，所有指令同时发出，适合有单向大边的品种
                                   * "昨开" 表示先平昨仓，再开仓，禁止平今仓，适合股指这样平今手续费较高的品种
                                   * "开" 表示只开仓，不平仓，适合需要进行锁仓操作的品种

            min_volume (int): [可选] **大单拆分模式下** 每笔最小下单的手数，默认不启用 **大单拆分模式**

            max_volume (int): [可选] **大单拆分模式下** 每笔最大下单的手数，默认不启用 **大单拆分模式**

            trade_chan (TqChan): [可选]成交通知channel, 当有成交发生时会将成交手数(多头为正数，空头为负数)发到该channel上

            trade_objs_chan (TqChan): [可选]成交对象通知channel, 当有成交发生时会将成交对象发送到该channel上

            account (TqAccount/TqKq/TqSim): [可选]指定发送下单指令的账户实例, 多账户模式下，该参数必须指定

        **注意**

        当 price 参数为函数类型时，该函数应该返回一个有效的价格值，应该避免返回 nan。以下为 price 参数是函数类型时的示例。

        Example1::

            # ... 用户代码 ...
            quote = api.get_quote("SHFE.cu2012")
            def get_price(direction):
                # 在 BUY 时使用买一价加一档价格，SELL 时使用卖一价减一档价格
                if direction == "BUY":
                    price = quote.bid_price1 + quote.price_tick
                else:
                    price = quote.ask_price1 - quote.price_tick
                # 如果 price 价格是 nan，使用最新价报单
                if price != price:
                    price = quote.last_price
                return price

            target_pos = TargetPosTask(api, "SHFE.cu2012", price=get_price)
            # ... 用户代码 ...


        Example2::

            # ... 用户代码 ...
            quote_list = api.get_quote_list(["SHFE.cu2012","SHFE.au2012"])

            def get_price_by_quote(quote):
                def get_price(direction):
                    # 在 BUY 时使用涨停价，SELL 时使用跌停价
                    if direction == "BUY":
                        price = quote["upper_limit"]
                    else:
                        price = quote.lower_limit
                    # 如果 price 价格是 nan，使用最新价报单
                    if price != price:
                        price = quote.last_price
                    return price
                return get_price

            for quote in quote_list:
                target_pos_active_dict[quote.instrument_id] = TargetPosTask(api, quote.instrument_id, price=get_price_by_quote(quote))

            # ... 用户代码 ...

        Example3::

            # 大单拆分模式用法示例

            from tqsdk import TqApi, TqAuth, TargetPosTask
            api = TqApi(auth=TqAuth("信易账户", "账户密码"))
            position = api.get_position('SHFE.rb2106')

            # 同时设置 min_volume、max_volume 两个参数，表示使用大单拆分模式
            t = TargetPosTask(api, 'SHFE.rb2106', min_volume=2, max_volume=10)
            t.set_target_volume(50)
            while True:
                api.wait_update()
                if position.pos_long == 50:
                    break
            api.close()

            # 说明：
            # 以上代码使用 TqSim 交易，开始时用户没有 SHFE.cu2012 合约的任何持仓，那么在 t.set_target_volume(50) 之后应该开多仓 50 手
            # 根据用户参数，下单使用大单拆分模式，每次下单手数在 2～10 之间，打印出的成交通知可能是这样的：
            # 2021-03-15 11:29:48 -     INFO - 模拟交易成交记录
            # 2021-03-15 11:29:48 -     INFO - 时间: 2021-03-15 11:29:47.516138, 合约: SHFE.rb2106, 开平: OPEN, 方向: BUY, 手数: 7, 价格: 4687.000,手续费: 32.94
            # 2021-03-15 11:29:48 -     INFO - 时间: 2021-03-15 11:29:47.519699, 合约: SHFE.rb2106, 开平: OPEN, 方向: BUY, 手数: 8, 价格: 4687.000,手续费: 37.64
            # 2021-03-15 11:29:48 -     INFO - 时间: 2021-03-15 11:29:47.522848, 合约: SHFE.rb2106, 开平: OPEN, 方向: BUY, 手数: 10, 价格: 4687.000,手续费: 47.05
            # 2021-03-15 11:29:48 -     INFO - 时间: 2021-03-15 11:29:47.525617, 合约: SHFE.rb2106, 开平: OPEN, 方向: BUY, 手数: 8, 价格: 4687.000,手续费: 37.64
            # 2021-03-15 11:29:48 -     INFO - 时间: 2021-03-15 11:29:47.528151, 合约: SHFE.rb2106, 开平: OPEN, 方向: BUY, 手数: 7, 价格: 4687.000,手续费: 32.94
            # 2021-03-15 11:29:48 -     INFO - 时间: 2021-03-15 11:29:47.530930, 合约: SHFE.rb2106, 开平: OPEN, 方向: BUY, 手数: 7, 价格: 4687.000,手续费: 32.94
            # 2021-03-15 11:29:48 -     INFO - 时间: 2021-03-15 11:29:47.533515, 合约: SHFE.rb2106, 开平: OPEN, 方向: BUY, 手数: 3, 价格: 4687.000,手续费: 14.12

        """
        if symbol.startswith("CZCE.CJ"):
            raise Exception("红枣期货不支持创建 targetpostask、twap、vwap 任务，交易所规定该品种最小开仓手数为大于等于 4 手，这些函数还未支持该规则!")
        if symbol.startswith("CZCE.ZC"):
            raise Exception("动力煤期货不支持创建 targetpostask、twap、vwap 任务，交易所规定该品种最小开仓手数为大于等于 4 手，这些函数还未支持该规则!")
        if symbol.startswith("CZCE.WH"):
            raise Exception("强麦期货不支持创建 targetpostask、twap、vwap 任务，交易所规定该品种最小开仓手数为大于等于 10 手，这些函数还未支持该规则!")
        if symbol.startswith("CZCE.PM"):
            raise Exception("普麦期货不支持创建 targetpostask、twap、vwap 任务，交易所规定该品种最小开仓手数为大于等于 10 手，这些函数还未支持该规则!")
        if symbol.startswith("CZCE.RI"):
            raise Exception("早籼稻期货不支持创建 targetpostask、twap、vwap 任务，交易所规定该品种最小开仓手数为大于等于 10 手，这些函数还未支持该规则!")
        if symbol.startswith("CZCE.JR"):
            raise Exception("粳稻期货不支持创建 targetpostask、twap、vwap 任务，交易所规定该品种最小开仓手数为大于等于 10 手，这些函数还未支持该规则!")
        if symbol.startswith("CZCE.LR"):
            raise Exception("晚籼稻期货不支持创建 targetpostask、twap、vwap 任务，交易所规定该品种最小开仓手数为大于等于 10 手，这些函数还未支持该规则!")
        super(TargetPosTask, self).__init__()
        self._api = api
        self._account = api._account._check_valid(account)
        self._symbol = symbol
        self._exchange = symbol.split(".")[0]
        self._offset_priority = _check_offset_priority(offset_priority)
        self._min_volume, self._max_volume = _check_volume_limit(min_volume, max_volume)
        self._price = _check_price(price)
        self._pos = self._account.get_position(self._symbol)
        self._pos_chan = TqChan(self._api, last_only=True)
        self._trade_chan = trade_chan
        self._trade_objs_chan = trade_objs_chan
        self._task = self._api.create_task(self._target_pos_task())
        self._time_update_task = self._api.create_task(self._wrap_update_time_from_md())  # 监听行情更新并记录当时本地时间的task
        self._local_time_record = time.time() - 0.005  # 更新最新行情时间时的本地时间
        self._local_time_record_update_chan = TqChan(self._api, last_only=True)  # 监听 self._local_time_record 更新
        self._log = structlog.get_logger(symbol = symbol,
                                         clazz = "TargetPosTask",
                                         id = id(self),
                                         task = id(self._task),
                                         children = [id(x) for x in [self._pos_chan,
                                                                     self._task,
                                                                     self._time_update_task,
                                                                     self._local_time_record_update_chan]])

        self._log.debug("create",
                        children = [id(x) for x in [self._pos_chan,
                                                                     self._task,
                                                                     self._time_update_task,
                                                                     self._local_time_record_update_chan]])

    def set_target_volume(self, volume: int) -> None:
        """
        设置目标持仓手数

        Args:
            volume (int): 目标持仓手数，正数表示多头，负数表示空头，0表示空仓

        Example1::

            # 设置 rb1810 持仓为多头5手
            from tqsdk import TqApi, TqAuth, TargetPosTask

            api = TqApi(auth=TqAuth("信易账户", "账户密码"))
            target_pos = TargetPosTask(api, "SHFE.rb1810")
            target_pos.set_target_volume(5)
            while True:
                # 需在 set_target_volume 后调用wait_update()以发出指令
                api.wait_update()

        Example2::

            # 多账户模式下使用 TargetPosTask
            from tqsdk import TqApi, TqMultiAccount, TqAuth, TargetPosTask

            account1 = TqAccount("H海通期货", "123456", "123456")
            account2 = TqAccount("H宏源期货", "654321", "123456")
            api = TqApi(TqMultiAccount([account1, account2]), auth=TqAuth("信易账户", "账户密码"))
            symbol1 = "DCE.m2105"
            symbol2 = "DCE.i2101"
            # 多账户模式下, 调仓工具需要指定账户实例
            target_pos1 = TargetPosTask(api, symbol1, account=account1)
            target_pos2 = TargetPosTask(api, symbol2, account=account2)
            target_pos1.set_target_volume(30)
            target_pos2.set_target_volume(80)
            while True:
                api.wait_update()

            api.close()

        """
        if self._task.done():
            raise Exception("已经结束的 TargetPosTask 实例不可以再设置手数。")
        self._pos_chan.send_nowait(int(volume))

    def _get_order(self, offset, vol, pending_frozen):
        """
        根据指定的offset和预期下单手数vol, 返回符合要求的委托单最大报单手数
        :param offset: "昨" / "今" / "开"
        :param vol: int, <0表示SELL, >0表示BUY
        :return: order_offset: "CLOSE"/"CLOSETODAY"/"OPEN"; order_dir: "BUY"/"SELL"; "order_volume": >=0, 报单手数
        """
        if vol > 0:  # 买单(增加净持仓)
            order_dir = "BUY"
            pos_all = self._pos.pos_short
        else:  # 卖单
            order_dir = "SELL"
            pos_all = self._pos.pos_long
        if offset == "昨":
            order_offset = "CLOSE"
            if self._exchange == "SHFE" or self._exchange == "INE":
                if vol > 0:
                    pos_all = self._pos.pos_short_his
                else:
                    pos_all = self._pos.pos_long_his
                frozen_volume = sum([order.volume_left for order in self._pos.orders.values() if
                                     not order.is_dead and order.offset == order_offset and order.direction == order_dir])
            else:
                frozen_volume = pending_frozen + sum([order.volume_left for order in self._pos.orders.values() if
                                                      not order.is_dead and order.offset != "OPEN" and order.direction == order_dir])
                # 判断是否有未冻结的今仓手数: 若有则不平昨仓
                if (self._pos.pos_short_today if vol > 0 else self._pos.pos_long_today) - frozen_volume > 0:
                    pos_all = frozen_volume
            order_volume = min(abs(vol), max(0, pos_all - frozen_volume))
        elif offset == "今":
            if self._exchange == "SHFE" or self._exchange == "INE":
                order_offset = "CLOSETODAY"
                if vol > 0:
                    pos_all = self._pos.pos_short_today
                else:
                    pos_all = self._pos.pos_long_today
                frozen_volume = sum([order.volume_left for order in self._pos.orders.values() if
                                     not order.is_dead and order.offset == order_offset and order.direction == order_dir])
            else:
                order_offset = "CLOSE"
                frozen_volume = pending_frozen + sum([order.volume_left for order in self._pos.orders.values() if
                                                      not order.is_dead and order.offset != "OPEN" and order.direction == order_dir])
                pos_all = self._pos.pos_short_today if vol > 0 else self._pos.pos_long_today
            order_volume = min(abs(vol), max(0, pos_all - frozen_volume))
        elif offset == "开":
            order_offset = "OPEN"
            order_volume = abs(vol)
        else:
            order_offset = ""
            order_volume = 0
        return order_offset, order_dir, order_volume

    async def _wrap_update_time_from_md(self):
        async_log = self._log.bind(current_task = id(asyncio.current_task(self._api._loop)))
        async_log.debug("_update_time_from_md", my_event = "await", depends= [],
                        check_rev = self._api._check_rev,
                        event_rev = self._api._event_rev,
                        wait_update_counter = self._api._wait_update_counter)
        try:
            await self._update_time_from_md()
        finally:
            async_log.debug("_update_time_from_md", my_event = "resume",
                            check_rev = self._api._check_rev,
                            event_rev = self._api._event_rev,
                            wait_update_counter = self._api._wait_update_counter)

    async def _update_time_from_md(self):
        """监听行情更新并记录当时本地时间的task"""
        async_log = self._log.bind(current_task = id(asyncio.current_task(self._api._loop)))
        tag1_written = True
        try:
            chan = TqChan(self._api, last_only=True)
            q = self._api.get_quote(self._symbol)
            async_log.debug("self._quote = await self._api.get_quote(self._symbol)",
                            my_event = "await",
                            depends = [id(q)],
                            check_rev = self._api._check_rev,
                            event_rev = self._api._event_rev,
                            wait_update_counter = self._api._wait_update_counter)
            try:
                self._quote = await q
            finally:
                async_log.debug("self._quote = await self._api.get_quote(self._symbol)",
                                my_event = "resume",
                                check_rev = self._api._check_rev,
                                event_rev = self._api._event_rev,
                                wait_update_counter = self._api._wait_update_counter)

            self._api.register_update_notify(self._quote, chan)  # quote有更新时: 更新记录的时间
            if isinstance(self._api._backtest, TqBacktest):
                # 回测情况下，在收到回测时间有更新的时候，也需要更新记录的时间
                self._api.register_update_notify(_get_obj(self._api._data, ["_tqsdk_backtest"]), chan)

            tag1_written = False
            async_log.debug("async for _ in chan",
                            my_event="await",
                            depends = [id(chan)],
                            check_rev = self._api._check_rev,
                            event_rev = self._api._event_rev,
                            wait_update_counter = self._api._wait_update_counter)

            async for _ in chan:
                async_log.debug("async for _ in chan",
                                my_event="resume",
                                check_rev = self._api._check_rev,
                                event_rev = self._api._event_rev,
                                wait_update_counter = self._api._wait_update_counter)
                tag1_written = True
                self._local_time_record = time.time() - 0.005  # 更新最新行情时间时的本地时间
                self._local_time_record_update_chan.send_nowait(True)  # 通知记录的时间有更新
                async_log.debug("async for _ in chan",
                                my_event="await",
                                depends = [id(chan)],
                                check_rev = self._api._check_rev,
                                event_rev = self._api._event_rev,
                                wait_update_counter = self._api._wait_update_counter)
                tag1_written = False
            async_log.debug("async for _ in chan",
                            my_event="resume",
                            check_rev = self._api._check_rev,
                            event_rev = self._api._event_rev,
                            wait_update_counter = self._api._wait_update_counter)
            tag1_written = True
        finally:
            if not tag1_written:
                async_log.debug("async for _ in chan",
                                my_event="resume",
                                check_rev = self._api._check_rev,
                                event_rev = self._api._event_rev,
                                wait_update_counter = self._api._wait_update_counter)

            async_log.debug("await chan.close()",
                            my_event="await",
                            depends = [id(chan)],
                            check_rev = self._api._check_rev,
                            event_rev = self._api._event_rev,
                            wait_update_counter = self._api._wait_update_counter)
            try:
                await chan.close()
            finally:
                async_log.debug("await chan.close()",
                                my_event="resume",
                                check_rev = self._api._check_rev,
                                event_rev = self._api._event_rev,
                                wait_update_counter = self._api._wait_update_counter)

    async def _target_pos_task(self):
        """负责调整目标持仓的task"""
        all_tasks = []
        async_log = self._log.bind(current_task = id(asyncio.current_task(self._api._loop)))
        tag_written = None
        try:
            q = self._api.get_quote(self._symbol)
            async_log.debug("self._quote = await self._api.get_quote(self._symbol)",
                            my_event="await",
                            depends = [id(q)],
                            external_depends = True,
                            check_rev = self._api._check_rev,
                            event_rev = self._api._event_rev,
                            wait_update_counter = self._api._wait_update_counter)
            try:
                self._quote = await q
            finally:
                async_log.debug("self._quote = await self._api.get_quote(self._symbol)",
                                my_event="resume",
                                check_rev = self._api._check_rev,
                                event_rev = self._api._event_rev,
                                wait_update_counter = self._api._wait_update_counter)

            async_log.debug("async for target_pos in self._pos_chan",
                            my_event="await",
                            depends = [id(self._pos_chan)],
                            check_rev = self._api._check_rev,
                            event_rev = self._api._event_rev,
                            wait_update_counter = self._api._wait_update_counter)
            tag_written = False
            async for target_pos in self._pos_chan:
                async_log.debug("async for target_pos in self._pos_chan",
                                my_event="resume",
                                check_rev = self._api._check_rev,
                                event_rev = self._api._event_rev,
                                wait_update_counter = self._api._wait_update_counter)
                tag_written = True
                # lib 中对于时间判断的方案:
                #   如果当前时间（模拟交易所时间）不在交易时间段内，则：等待直到行情更新
                #   行情更新（即下一交易时段开始）后：获取target_pos最新的目标仓位, 开始调整仓位

                # 如果不在可交易时间段内（回测时用 backtest 下发的时间判断，实盘使用 quote 行情判断）: 等待更新
                while True:
                    if isinstance(self._api._backtest, TqBacktest):
                        cur_timestamp = self._api._data.get("_tqsdk_backtest", {}).get("current_dt", float("nan"))
                        cur_dt = _timestamp_nano_to_str(cur_timestamp)
                        time_record = float("nan")
                    else:
                        cur_dt = self._quote["datetime"]
                        time_record = self._local_time_record
                    if _is_in_trading_time(self._quote, cur_dt, time_record):
                        break
                    async_log.debug("await self._local_time_record_update_chan.recv()",
                                    my_event="await",
                                    depends = [id(self._local_time_record_update_chan)],
                                    check_rev = self._api._check_rev,
                                    event_rev = self._api._event_rev,
                                    wait_update_counter = self._api._wait_update_counter)
                    try:
                        await self._local_time_record_update_chan.recv()
                    finally:
                        async_log.debug("await self._local_time_record_update_chan.recv()",
                                        my_event="resume",
                                        check_rev = self._api._check_rev,
                                        event_rev = self._api._event_rev,
                                        wait_update_counter = self._api._wait_update_counter)

                target_pos = self._pos_chan.recv_latest(target_pos)  # 获取最后一个target_pos目标仓位
                # 确定调仓增减方向
                delta_volume = target_pos - self._pos.pos
                pending_forzen = 0
                for each_priority in self._offset_priority + ",":  # 按不同模式的优先级顺序报出不同的offset单，股指(“昨开”)平昨优先从不平今就先报平昨，原油平今优先("今昨开")就报平今
                    if each_priority == ",":
                        async_log.debug("await gather(*[each._task for each in all_tasks])",
                                        my_event="await",
                                        depends = [id(x._task) for x in all_tasks],
                                        check_rev = self._api._check_rev,
                                        event_rev = self._api._event_rev,
                                        wait_update_counter = self._api._wait_update_counter)
                        try:
                            await gather(*[each._task for each in all_tasks])
                        finally:
                            async_log.debug("await gather(*[each._task for each in all_tasks])",
                                            my_event="resume",
                                            check_rev = self._api._check_rev,
                                            event_rev = self._api._event_rev,
                                            wait_update_counter = self._api._wait_update_counter)
                        pending_forzen = 0
                        all_tasks = []
                        continue
                    order_offset, order_dir, order_volume = self._get_order(each_priority, delta_volume, pending_forzen)
                    if order_volume == 0:  # 如果没有则直接到下一种offset
                        continue
                    elif order_offset != "OPEN":
                        pending_forzen += order_volume
                    order_task = InsertOrderUntilAllTradedTask(self._api, self._symbol, order_dir, offset=order_offset,
                                                               volume=order_volume, min_volume=self._min_volume,
                                                               max_volume=self._max_volume, price=self._price,
                                                               trade_chan=self._trade_chan,
                                                               trade_objs_chan=self._trade_objs_chan,
                                                               account=self._account)
                    all_tasks.append(order_task)
                    delta_volume -= order_volume if order_dir == "BUY" else -order_volume

                tag_written = False
                async_log.debug("async for target_pos in self._pos_chan",
                                my_event="await",
                                depends = [id(self._pos_chan)],
                                check_rev = self._api._check_rev,
                                event_rev = self._api._event_rev,
                                wait_update_counter = self._api._wait_update_counter)

            async_log.debug("async for target_pos in self._pos_chan",
                            my_event="resume",
                            check_rev = self._api._check_rev,
                            event_rev = self._api._event_rev,
                            wait_update_counter = self._api._wait_update_counter)
            tag_written = True
        finally:
            # 执行 task.cancel() 时, 删除掉该 symbol 对应的 TargetPosTask 实例
            # self._account 类型为 TqSim/TqKq/TqAccount，都包括 _account_key 变量
            if tag_written == False:
                async_log.debug("async for target_pos in self._pos_chan",
                                my_event="resume",
                                check_rev = self._api._check_rev,
                                event_rev = self._api._event_rev,
                                wait_update_counter = self._api._wait_update_counter)
            TargetPosTaskSingleton._instances.pop(self._account._account_key + "#" + self._symbol, None)
            async_log.debug("await self._pos_chan.close()",
                            my_event="await",
                            depends = [id(self._pos_chan)],
                            check_rev = self._api._check_rev,
                            event_rev = self._api._event_rev,
                            wait_update_counter = self._api._wait_update_counter)
            try:
                await self._pos_chan.close()
            finally:
                async_log.debug("await self._pos_chan.close()",
                                my_event="resume",
                                check_rev = self._api._check_rev,
                                event_rev = self._api._event_rev,
                                wait_update_counter = self._api._wait_update_counter)
            self._time_update_task.cancel()
            async_log.debug("await asyncio.gather(*([t._task for t in all_tasks] + [self._time_update_task]), return_exceptions=True)",
                            my_event="await",
                            depends = [id(x._task) for x in all_tasks] + [id(self._time_update_task)],
                            check_rev = self._api._check_rev,
                            event_rev = self._api._event_rev,
                            wait_update_counter = self._api._wait_update_counter)
            try:
                await asyncio.gather(*([t._task for t in all_tasks] + [self._time_update_task]), return_exceptions=True)
            finally:
                async_log.debug("await asyncio.gather(*([t._task for t in all_tasks] + [self._time_update_task]), return_exceptions=True)",
                                my_event="resume",
                                check_rev = self._api._check_rev,
                                event_rev = self._api._event_rev,
                                wait_update_counter = self._api._wait_update_counter)

    def cancel(self):
        """
        取消当前 TargetPosTask 实例，会将该实例已经发出但还是未成交的委托单撤单，并且如果后续调用此实例的 set_target_volume 函数会报错。

        任何时刻，每个账户下一个合约只能有一个 TargetPosTask 实例，并且其构造参数不能修改。

        如果对于同一个合约要构造不同参数的 TargetPosTask 实例，需要调用 cancel 方法销毁，才能创建新的 TargetPosTask 实例

        Example1::

            from datetime import datetime, time
            from tqsdk import TqApi, TargetPosTask

            api = TqApi(auth=TqAuth("信易账户", "账户密码"))
            quote = api.get_quote("SHFE.rb2110")
            target_pos_passive = TargetPosTask(api, "SHFE.rb2110", price="PASSIVE")

            while datetime.strptime(quote.datetime, "%Y-%m-%d %H:%M:%S.%f").time() < time(14, 50):
                api.wait_update()
                # ... 策略代码 ...

            # 取消 TargetPosTask 实例
            target_pos_passive.cancel()

            while not target_pos_passive.is_finished():  # 此循环等待 target_pos_passive 处理 cancel 结束
                api.wait_update()  # 调用wait_update()，会对已经发出但还是未成交的委托单撤单

            # 创建新的 TargetPosTask 实例
            target_pos_active = TargetPosTask(api, "SHFE.rb2110", price="ACTIVE")
            target_pos_active.set_target_volume(0)  # 平所有仓位

            while True:
                api.wait_update()
                # ... 策略代码 ...

            api.close()

        """
        self._task.cancel()

    def is_finished(self) -> bool:
        """
        返回当前 TargetPosTask 实例是否已经结束。即如果后续调用此实例的 set_target_volume 函数会报错，此实例不会再下单或者撤单。

        Returns:
            bool: 当前 TargetPosTask 实例是否已经结束
        """
        return self._task.done()


class InsertOrderUntilAllTradedTask(object):
    """追价下单task, 该task会在行情变化后自动撤单重下，直到全部成交
     （注：此类主要在tqsdk内部使用，并非简单用法，不建议用户使用）"""

    def __init__(self, api, symbol, direction, offset, volume, min_volume: Optional[int] = None,
                 max_volume: Optional[int] = None, price: Union[str, Callable[[str], Union[float, int]]] = "ACTIVE",
                 trade_chan=None, trade_objs_chan=None, account: Optional[Union[TqAccount, TqKq, TqSim]] = None):
        """
        创建追价下单task实例

        Args:
            api (TqApi): TqApi实例，该task依托于指定api下单/撤单

            symbol (str): 拟下单的合约symbol, 格式为 交易所代码.合约代码,  例如 "SHFE.cu1801"

            direction (str): "BUY" 或 "SELL"

            offset (str): "OPEN", "CLOSE" 或 "CLOSETODAY"

            volume (int): 需要下单的手数

            min_volume (int): [可选] **大单拆分模式下** 每笔最小下单的手数，默认不启用 **大单拆分模式**

            max_volume (int): [可选] **大单拆分模式下** 每笔最大下单的手数，默认不启用 **大单拆分模式**

            price (str / Callable): [可选]下单方式, 默认为 "ACTIVE"。
                * "ACTIVE"：对价下单，在持仓调整过程中，若下单方向为买，对价为卖一价；若下单方向为卖，对价为买一价。
                * "PASSIVE"：对价下单，在持仓调整过程中，若下单方向为买，对价为买一价；若下单方向为卖，对价为卖一价。
                * Callable[[str], Union[float, int]]: 函数参数为下单方向，函数返回值是下单价格。如果返回 nan，程序会抛错。

            trade_chan (TqChan): [可选]成交通知channel, 当有成交发生时会将成交手数(多头为正数，空头为负数)发到该channel上

            trade_objs_chan (TqChan): [可选]成交对象通知channel, 当有成交发生时会将成交对象发送到该channel上

            account (TqAccount/TqKq/TqSim): [可选]指定发送下单指令的账户实例, 多账户模式下，该参数必须指定
        """
        self._api = api
        self._account = account
        self._symbol = symbol
        self._direction = _check_direction(direction)
        self._offset = _check_offset(offset)
        self._volume = _check_volume(volume)
        self._min_volume, self._max_volume = _check_volume_limit(min_volume, max_volume)
        self._price = _check_price(price)
        self._trade_chan = trade_chan
        self._trade_objs_chan = trade_objs_chan
        self._task = self._api.create_task(self._wrap_run())
        self._log = structlog.get_logger(symbol = symbol, id = id(self),
                                         task = id(self._task), clazz = "InsertOrderUntilAllTradedTask")
        self._log.debug("create", children = [id(x) for x in [self._trade_chan, self._trade_objs_chan, self._task]])

    async def _wrap_run(self):
        async_log = self._log.bind(current_task = id(asyncio.current_task(self._api._loop)))
        async_log.debug("_run", my_event = "await", depends = [],
                        check_rev = self._api._check_rev,
                        event_rev = self._api._event_rev,
                        wait_update_counter = self._api._wait_update_counter)
        try:
            await self._run()
        finally:
            async_log.debug("_run", my_event = "resume",
                            check_rev = self._api._check_rev,
                            event_rev = self._api._event_rev,
                            wait_update_counter = self._api._wait_update_counter)
    async def _run(self):
        """负责追价下单的task"""
        async_log = self._log.bind(current_task = id(asyncio.current_task(self._api._loop)))

        quote = self._api.get_quote(self._symbol)
        async_log.debug("self._quote = await self._api.get_quote(self._symbol)",
                        my_event="await",
                        depends = [id(quote)],
                        check_rev = self._api._check_rev,
                        event_rev = self._api._event_rev,
                        wait_update_counter = self._api._wait_update_counter)
        self._quote = await quote
        async_log.debug("self._quote = await self._api.get_quote(self._symbol)",
                        my_event="resume",
                        check_rev = self._api._check_rev,
                        event_rev = self._api._event_rev,
                        wait_update_counter = self._api._wait_update_counter)
        while self._volume != 0:
            limit_price = self._get_price(self._direction)
            if limit_price != limit_price:
                raise Exception("设置价格函数返回 nan，无法处理。请检查后重试。")
            # 当前下单手数
            if self._min_volume and self._max_volume and self._volume >= self._max_volume:
                this_volume = utils.RD.randint(self._min_volume, self._max_volume)
            else:
                this_volume = self._volume
            insert_order_task = InsertOrderTask(self._api, self._symbol, self._direction, self._offset,
                                                this_volume, limit_price=limit_price, trade_chan=self._trade_chan,
                                                trade_objs_chan=self._trade_objs_chan, account=self._account)

            async_log.debug("order = await insert_order_task._order_chan.recv()",
                            my_event="await",
                            depends = [id(insert_order_task._order_chan)],
                            check_rev = self._api._check_rev,
                            event_rev = self._api._event_rev,
                            wait_update_counter = self._api._wait_update_counter)
            order = await insert_order_task._order_chan.recv()

            async_log.debug("order = await insert_order_task._order_chan.recv()",
                            my_event="resume",
                            check_rev = self._api._check_rev,
                            event_rev = self._api._event_rev,
                            wait_update_counter = self._api._wait_update_counter)

            check_chan = TqChan(self._api, last_only=True)
            check_task = self._api.create_task(self._check_price(check_chan, limit_price, order['order_id']))
            try:
                # 当父 task 被 cancel，子 task 如果正在执行，也会捕获 CancelError
                # 添加 asyncio.shield 后，如果父 task 被 cancel，asyncio.shield 也会被 cancel，但是子 task 不会收到 CancelError
                # 这里需要 asyncio.shield，是因为 insert_order_task._task 预期不会被 cancel， 应该等待到 order 状态是 FINISHED 才返回
                async_log.debug("await asyncio.shield(insert_order_task._task)",
                                my_event="await",
                                depends = [id(insert_order_task._task)],
                                check_rev = self._api._check_rev,
                                event_rev = self._api._event_rev,
                                wait_update_counter = self._api._wait_update_counter)
                try:
                    await asyncio.shield(insert_order_task._task)
                finally:
                    async_log.debug("await asyncio.shield(insert_order_task._task)",
                                    my_event="resume",
                                    check_rev = self._api._check_rev,
                                    event_rev = self._api._event_rev,
                                    wait_update_counter = self._api._wait_update_counter)

                order = insert_order_task._order_chan.recv_latest(order)
                self._volume -= (this_volume - order['volume_left'])
                if order['volume_left'] != 0 and not check_task.done():
                    raise Exception("遇到错单: %s %s %s %d手 %f %s" % (
                        self._symbol, self._direction, self._offset, this_volume, limit_price, order['last_msg']))
            finally:
                if self._api.get_order(order['order_id'], account=self._account).status == "ALIVE":
                    # 当 task 被 cancel 时，主动撤掉未成交的挂单
                    self._api.cancel_order(order['order_id'], account=self._account)

                async_log.debug("await check_chan.close()",
                                my_event="await",
                                depends = [id(check_chan)],
                                check_rev = self._api._check_rev,
                                event_rev = self._api._event_rev,
                                wait_update_counter = self._api._wait_update_counter)
                await check_chan.close()
                async_log.debug("await check_chan.close()",
                                my_event="resume",
                                check_rev = self._api._check_rev,
                                event_rev = self._api._event_rev,
                                wait_update_counter = self._api._wait_update_counter)

                async_log.debug("await check_task",
                                my_event="await",
                                depends = [id(check_task)],
                                check_rev = self._api._check_rev,
                                event_rev = self._api._event_rev,
                                wait_update_counter = self._api._wait_update_counter)
                await check_task

                async_log.debug("await check_task",
                                my_event="resume",
                                check_rev = self._api._check_rev,
                                event_rev = self._api._event_rev,
                                wait_update_counter = self._api._wait_update_counter)
                # 在每次退出时，都等到 insert_order_task 执行完，此时 order 状态一定是 FINISHED；self._trade_chan 也一定会收到全部的成交手数
                try:
                    # 当用户调用 api.close(), 会主动 cancel 所有由 api 创建的 task，包括 TargetPosTask._target_pos_task，
                    # 此时，insert_order_task._task 如果有未完成委托单，会永远等待下去（因为网络连接已经断开），所以这里增加超时机制。
                    async_log.debug("await asyncio.wait_for(insert_order_task._task, timeout=30)",
                                    my_event="await",
                                    depends= [id(insert_order_task._task)],
                                    check_rev = self._api._check_rev,
                                    event_rev = self._api._event_rev,
                                    wait_update_counter = self._api._wait_update_counter)
                    try:
                        await asyncio.wait_for(insert_order_task._task, timeout=30)
                    finally:
                        async_log.debug("await asyncio.wait_for(insert_order_task._task, timeout=30)",
                                        my_event="resume",
                                        check_rev = self._api._check_rev,
                                        event_rev = self._api._event_rev,
                                        wait_update_counter = self._api._wait_update_counter)
                except asyncio.TimeoutError:
                    raise Exception(f"InsertOrderTask 执行超时，30s 内报单未执行完。此错误产生可能的原因："
                                    f"可能是用户调用了 api.close() 之后，已经创建的 InsertOrderTask 无法正常结束。")


    def _get_price(self, direction):
        """根据最新行情和下单方式计算出最优的下单价格"""
        if self._price not in ('ACTIVE', 'PASSIVE'):
            assert isfunction(self._price)
            return self._price(direction)
        # 主动买的价格序列(优先判断卖价，如果没有则用买价)
        price_list = [self._quote.ask_price1, self._quote.bid_price1]
        if direction == "SELL":
            price_list.reverse()
        if self._price == "PASSIVE":
            price_list.reverse()
        limit_price = price_list[0]
        if limit_price != limit_price:
            limit_price = price_list[1]
        if limit_price != limit_price:
            limit_price = self._quote.last_price
        if limit_price != limit_price:
            limit_price = self._quote.pre_close
        return limit_price

    async def _check_price(self, update_chan, order_price, order_id):
        """判断价格是否变化的task"""
        async_log = self._log.bind(current_task = id(asyncio.current_task(self._api._loop)))
        async_log.debug("_check_price", my_event = "await", depends= [],
                        check_rev = self._api._check_rev,
                        event_rev = self._api._event_rev,
                        wait_update_counter = self._api._wait_update_counter)
        temp_ch = self._api.register_update_notify(chan=update_chan)
        async_log.debug("async with self._api.register_update_notify(chan=update_chan) __enter__",
                        my_event = "await",
                        depends = [id(temp_ch)],
                        check_rev = self._api._check_rev,
                        event_rev = self._api._event_rev,
                        wait_update_counter = self._api._wait_update_counter)

        async with temp_ch:
            async_log.debug("async with self._api.register_update_notify(chan=update_chan) __enter__",
                            my_event = "resume",
                            check_rev = self._api._check_rev,
                            event_rev = self._api._event_rev,
                            wait_update_counter = self._api._wait_update_counter)

            async_log.debug("async for _ in update_chan",
                            my_event = "await",
                            depends = [id(update_chan)],
                            check_rev = self._api._check_rev,
                            event_rev = self._api._event_rev,
                            wait_update_counter = self._api._wait_update_counter)
            async for _ in update_chan:
                async_log.debug("async for _ in update_chan",
                                my_event = "resume",
                                depends = [id(update_chan)],
                                check_rev = self._api._check_rev,
                                event_rev = self._api._event_rev,
                                wait_update_counter = self._api._wait_update_counter)

                new_price = self._get_price(self._direction)
                if (self._direction == "BUY" and new_price > order_price) or (
                        self._direction == "SELL" and new_price < order_price):
                    self._api.cancel_order(order_id, account=self._account)
                    async_log.debug("async for _ in update_chan",
                                    my_event = "await",
                                    depends = [id(update_chan)],
                                    check_rev = self._api._check_rev,
                                    event_rev = self._api._event_rev,
                                    wait_update_counter = self._api._wait_update_counter)
                    break

                async_log.debug("async for _ in update_chan",
                                my_event = "await",
                                depends = [id(update_chan)],
                                check_rev = self._api._check_rev,
                                event_rev = self._api._event_rev,
                                wait_update_counter = self._api._wait_update_counter)
            async_log.debug("async for _ in update_chan",
                            my_event = "resume",
                            check_rev = self._api._check_rev,
                            event_rev = self._api._event_rev,
                            wait_update_counter = self._api._wait_update_counter)
            async_log.debug("async with self._api.register_update_notify(chan=update_chan) __exit__",
                            my_event = "await",
                            depends = [id(temp_ch)],
                            check_rev = self._api._check_rev,
                            event_rev = self._api._event_rev,
                            wait_update_counter = self._api._wait_update_counter)

        async_log.debug("async with self._api.register_update_notify(chan=update_chan) __exit__",
                        my_event = "resume",
                        check_rev = self._api._check_rev,
                        event_rev = self._api._event_rev,
                        wait_update_counter = self._api._wait_update_counter)
        async_log.debug("_check_price", my_event = "resume", 
                        check_rev = self._api._check_rev,
                        event_rev = self._api._event_rev,
                        wait_update_counter = self._api._wait_update_counter)

class InsertOrderTask(object):
    """下单task （注：此类主要在tqsdk内部使用，并非简单用法，不建议用户使用）"""

    def __init__(self, api, symbol, direction, offset, volume, limit_price=None, order_chan=None, trade_chan=None,
                 trade_objs_chan=None, account: Optional[Union[TqAccount, TqKq, TqSim]] = None):
        """
        创建下单task实例

        Args:
            api (TqApi): TqApi实例，该task依托于指定api下单/撤单

            symbol (str): 拟下单的合约symbol, 格式为 交易所代码.合约代码,  例如 "SHFE.cu1801"

            direction (str): "BUY" 或 "SELL"

            offset (str): "OPEN", "CLOSE" 或 "CLOSETODAY"

            volume (int): 需要下单的手数

            limit_price (float): [可选]下单价格, 默认市价单

            order_chan (TqChan): [可选]委托单通知channel, 当委托单状态发生时会将委托单信息发到该channel上

            trade_chan (TqChan): [可选]成交通知channel, 当有成交发生时会将成交手数(多头为正数，空头为负数)发到该channel上

            trade_objs_chan (TqChan): [可选]成交对象通知channel, 当有成交发生时会将成交对象发送到该channel上

            account (TqAccount/TqKq/TqSim): [可选]指定发送下单指令的账户实例, 多账户模式下，该参数必须指定
        """
        self._api = api
        self._account = account
        self._symbol = symbol
        self._direction = _check_direction(direction)
        self._offset = _check_offset(offset)
        self._volume = _check_volume(volume)
        self._offset = offset
        self._volume = int(volume)
        self._limit_price = float(limit_price) if limit_price is not None else None
        self._order_chan = order_chan if order_chan is not None else TqChan(self._api)
        self._trade_chan = trade_chan
        self._trade_objs_chan = trade_objs_chan
        self._task = self._api.create_task(self._wrap_run())
        self._log = structlog.get_logger(symbol = symbol, id = id(self),
                                         task = id(self._task), clazz = "InsertOrderTask")
        self._log.debug("create", children = [id(x) for x in [self._order_chan, self._trade_chan, self._trade_objs_chan, self._task]])

    async def _wrap_run(self):
        async_log = self._log.bind(current_task = id(asyncio.current_task(self._api._loop)))
        async_log.debug("_run", my_event = "await", depends = [], 
                        check_rev = self._api._check_rev,
                        event_rev = self._api._event_rev,
                        wait_update_counter = self._api._wait_update_counter)
        try:
            await self._run()
        finally:
            async_log.debug("_run", my_event = "resume",
                            check_rev = self._api._check_rev,
                            event_rev = self._api._event_rev,
                            wait_update_counter = self._api._wait_update_counter)

    async def _run(self):
        """负责下单的task"""
        async_log = self._log.bind(current_task = id(asyncio.current_task(self._api._loop)))
        
        order_id = utils._generate_uuid("PYSDK_target")
        order = self._api.insert_order(self._symbol, self._direction, self._offset, self._volume, self._limit_price,
                                       order_id=order_id, account=self._account)
        last_order = order.copy()  # 保存当前 order 的状态
        last_left = self._volume
        all_trades_id = set()  # 记录所有的 trade_id
        update_chan = self._api.register_update_notify()
        async_log.debug("with self._api.register_update_notify() __enter__",
                        my_event="await",
                        depends = [id(update_chan)],
                        check_rev = self._api._check_rev,
                        event_rev = self._api._event_rev,
                        wait_update_counter = self._api._wait_update_counter)
        async with update_chan:
            async_log.debug("with self._api.register_update_notify() __enter__",
                            my_event="resume",
                            check_rev = self._api._check_rev,
                            event_rev = self._api._event_rev,
                            wait_update_counter = self._api._wait_update_counter)

            async_log.debug('await self._order_chan.send({k: v for k, v in last_order.items() if not k.startswith("_")})',
                            my_event="await",
                            depends = [id(self._order_chan)],
                            check_rev = self._api._check_rev,
                            event_rev = self._api._event_rev,
                            wait_update_counter = self._api._wait_update_counter)
            await self._order_chan.send({k: v for k, v in last_order.items() if not k.startswith("_")})  # 将副本的数据及所有权转移
            async_log.debug('await self._order_chan.send({k: v for k, v in last_order.items() if not k.startswith("_")})',
                            my_event="resume",
                            depends = [id(self._order_chan)],
                            check_rev = self._api._check_rev,
                            event_rev = self._api._event_rev,
                            wait_update_counter = self._api._wait_update_counter)

            while order.status != "FINISHED" or (order.volume_orign - order.volume_left) != sum(
                    [trade.volume for trade in order.trade_records.values()]):

                async_log.debug("await update_chan.recv()",
                                my_event="await",
                                depends = [id(update_chan)],
                                check_rev = self._api._check_rev,
                                event_rev = self._api._event_rev,
                                wait_update_counter = self._api._wait_update_counter)
                try:
                    await update_chan.recv()
                finally:
                    async_log.debug("await update_chan.recv()",
                                    my_event="resume",
                                    check_rev = self._api._check_rev,
                                    event_rev = self._api._event_rev,
                                    wait_update_counter = self._api._wait_update_counter)
                if order.volume_left != last_left:
                    vol = last_left - order.volume_left
                    last_left = order.volume_left
                    if self._trade_chan:
                        async_log.debug('await self._trade_chan.send(vol if order.direction == "BUY" else -vol)',
                                        my_event="await",
                                        depends = [id(self._trade_chan)],
                                        check_rev = self._api._check_rev,
                                        event_rev = self._api._event_rev,
                                        wait_update_counter = self._api._wait_update_counter)
                        await self._trade_chan.send(vol if order.direction == "BUY" else -vol)
                        async_log.debug('await self._trade_chan.send(vol if order.direction == "BUY" else -vol)',
                                        my_event="resume",
                                        check_rev = self._api._check_rev,
                                        event_rev = self._api._event_rev,
                                        wait_update_counter = self._api._wait_update_counter)
                if self._trade_objs_chan:
                    # 当前用户需要接受 trade_obj，才会运行以下代码
                    rest_trades_id = set(order.trade_records) - all_trades_id
                    for trade_id in rest_trades_id:
                        # 新收到的 trade 发送到 self._trade_objs_chan
                        async_log.debug('await self._trade_objs_chan.send({k: v for k, v in order.trade_records[trade_id].items() if not k.startswith("_")})',
                                        my_event="await",
                                        depends = [id(self._trade_objs_chan)],
                                        check_rev = self._api._check_rev,
                                        event_rev = self._api._event_rev,
                                        wait_update_counter = self._api._wait_update_counter)
                        await self._trade_objs_chan.send({k: v for k, v in order.trade_records[trade_id].items() if not k.startswith("_")})
                        async_log.debug('await self._trade_objs_chan.send({k: v for k, v in order.trade_records[trade_id].items() if not k.startswith("_")})',
                                        my_event="resume",
                                        check_rev = self._api._check_rev,
                                        event_rev = self._api._event_rev,
                                        wait_update_counter = self._api._wait_update_counter)

                        all_trades_id.add(trade_id)
                if order != last_order:
                    last_order = order.copy()

                    async_log.debug('await self._order_chan.send({k: v for k, v in last_order.items() if not k.startswith("_")})',
                                    my_event="await",
                                    depends= [id(self._order_chan)],
                                    check_rev = self._api._check_rev,
                                    event_rev = self._api._event_rev,
                                    wait_update_counter = self._api._wait_update_counter)
                    await self._order_chan.send({k: v for k, v in last_order.items() if not k.startswith("_")})
                    async_log.debug('await self._order_chan.send({k: v for k, v in last_order.items() if not k.startswith("_")})',
                                    my_event="resume",
                                    check_rev = self._api._check_rev,
                                    event_rev = self._api._event_rev,
                                    wait_update_counter = self._api._wait_update_counter)
            async_log.debug("with self._api.register_update_notify() __exit__",
                            my_event="await",
                            depends = [id(update_chan)],
                            check_rev = self._api._check_rev,
                            event_rev = self._api._event_rev,
                            wait_update_counter = self._api._wait_update_counter)

        async_log.debug("with self._api.register_update_notify() __exit__",
                        my_event="resume",
                        check_rev = self._api._check_rev,
                        event_rev = self._api._event_rev,
                        wait_update_counter = self._api._wait_update_counter)


